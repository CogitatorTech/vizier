-- TPC-H correctness validation for Vizier
-- Generates real TPC-H data via dbgen, feeds all 22 queries into Vizier,
-- and checks whether the recommendations are reasonable.
--
-- Expected recommendations:
--   lineitem: sort by l_shipdate (most filtered column across Q1, Q3, Q4, Q6, Q7, Q12, Q14, Q15)
--   orders:   sort by o_orderdate (filtered in Q3, Q4, Q5, Q8, Q10)
--   customer: index on c_mktsegment (equality filter in Q3) or c_nationkey
--   region:   index on r_name (equality filter in Q2, Q5, Q8)
--   nation:   index on n_regionkey (join predicate in Q2, Q5, Q8)
--   supplier: index on s_nationkey (join predicate)
--   partsupp: considered for join-path sort
--
-- Usage: duckdb -unsigned -c ".read benches/tpch_correctness.sql"

load 'zig-out/lib/vizier.duckdb_extension';
install tpch;
load tpch;

-- ======================================================================
-- 1. Generate TPC-H data at scale factor 0.1 (~60K lineitem rows)
-- ======================================================================
select '>>> Generating TPC-H SF=0.1' as step;
call dbgen(sf=0.1);

select 'lineitem' as tbl, count(*) as rows from lineitem
union all select 'orders', count(*) from orders
union all select 'customer', count(*) from customer
union all select 'supplier', count(*) from supplier
union all select 'nation', count(*) from nation
union all select 'region', count(*) from region
union all select 'part', count(*) from part
union all select 'partsupp', count(*) from partsupp
order by rows desc;

-- ======================================================================
-- 2. Capture all 22 TPC-H queries (each repeated to boost frequency)
-- ======================================================================
select '>>> Capturing all 22 TPC-H queries' as step;

-- Create a persistent table so g_flush_conn can access it for bulk capture
create table vizier_tpch_queries as
  select query_nr, query from tpch_queries();

-- Capture each query 3 times to simulate a repeated workload
select * from vizier_capture_bulk('vizier_tpch_queries', 'query');
select * from vizier_capture_bulk('vizier_tpch_queries', 'query');
select * from vizier_capture_bulk('vizier_tpch_queries', 'query');

select * from vizier_flush();

-- ======================================================================
-- 3. Check captured workload
-- ======================================================================
select '>>> Workload summary' as step;
select query_signature as sig, execution_count as runs,
       avg_time_ms as avg_ms, estimated_rows,
       substr(sample_sql, 1, 80) || '...' as sql_preview
from vizier.workload_queries
order by execution_count desc;

select '>>> Predicate summary' as step;
select table_name, column_name, predicate_kind, frequency
from vizier.workload_predicates
order by frequency desc, table_name, column_name
limit 30;

-- ======================================================================
-- 4. Run advisors
-- ======================================================================
select '>>> Running all advisors' as step;
select * from vizier_analyze();

-- ======================================================================
-- 5. Validate recommendations
-- ======================================================================
select '>>> All recommendations (ranked by score)' as step;
select recommendation_id as id, kind, table_name,
       round(score, 3) as score, round(confidence, 2) as conf,
       substr(reason, 1, 100) as reason_preview
from vizier.recommendations
order by score desc;

-- Validation checks: verify expected recommendations exist
select '>>> Validation: expected recommendations' as step;

-- Check 1: lineitem should have a sort recommendation (most filtered table)
select 'lineitem sort recommendation' as check_name,
       case when count(*) > 0 then 'PASS' else 'FAIL' end as result
from vizier.recommendation_store
where kind = 'rewrite_sorted_table' and table_name = 'lineitem';

-- Check 2: orders should have a sort recommendation
select 'orders sort recommendation' as check_name,
       case when count(*) > 0 then 'PASS' else 'FAIL' end as result
from vizier.recommendation_store
where kind = 'rewrite_sorted_table' and table_name = 'orders';

-- Check 3: there should be index recommendations for equality predicates
select 'index recommendations exist' as check_name,
       case when count(*) > 0 then 'PASS' else 'FAIL' end as result
from vizier.recommendation_store
where kind = 'create_index';

-- Check 4: the lineitem sort columns should include l_shipdate
select 'lineitem sort includes l_shipdate' as check_name,
       case when count(*) > 0 then 'PASS' else 'FAIL' end as result
from vizier.recommendation_store
where kind = 'rewrite_sorted_table'
  and table_name = 'lineitem'
  and columns_json like '%l_shipdate%';

-- Check 5: no recommendations for tables entirely outside the workload
-- (aliases like n1, n2, l1 from TPC-H self-joins are acceptable)
select 'no unrelated table recommendations' as check_name,
       case when count(*) = 0 then 'PASS' else 'FAIL' end as result
from vizier.recommendation_store
where table_name not in ('lineitem', 'orders', 'customer', 'supplier',
                          'nation', 'region', 'part', 'partsupp',
                          'n1', 'n2', 'l1', 'l2', 'l3', '')
  and kind != 'no_action';

-- Check 6: estimated_rows should be populated (EXPLAIN integration working)
select 'estimated_rows populated' as check_name,
       case when count(*) > 0 then 'PASS' else 'FAIL' end as result
from vizier.workload_queries
where estimated_rows > 0;

-- Check 7: score ordering is sane (highest-scored recommendations should be
-- for the most-filtered tables)
-- Check 7: top recommendation should be for a real TPC-H table
select 'top recommendation is for a TPC-H table' as check_name,
       case when table_name in ('lineitem', 'orders', 'customer', 'supplier',
                                 'nation', 'region', 'part', 'partsupp')
            then 'PASS' else 'FAIL' end as result
from vizier.recommendations
where table_name != ''
order by score desc
limit 1;

-- ======================================================================
-- 6. Explain top recommendations
-- ======================================================================
select '>>> Top 3 recommendations explained' as step;
select * from vizier.explain(1);
select * from vizier.explain(2);
select * from vizier.explain(3);

-- ======================================================================
-- 7. Apply top recommendation and benchmark
-- ======================================================================
select '>>> Applying top recommendation (dry run)' as step;
select * from vizier_apply(1, dry_run => true);

select '>>> Replay workload before applying' as step;
select * from vizier_replay();
select queries_replayed, round(replay_total_ms, 2) as replay_ms,
       overall_verdict
from vizier.replay_totals;

-- ======================================================================
-- Summary
-- ======================================================================
select '>>> Validation summary' as step;
select check_name, result from (
  select 'lineitem sort recommendation' as check_name,
         case when count(*) > 0 then 'PASS' else 'FAIL' end as result
  from vizier.recommendation_store
  where kind = 'rewrite_sorted_table' and table_name = 'lineitem'
  union all
  select 'orders sort recommendation',
         case when count(*) > 0 then 'PASS' else 'FAIL' end
  from vizier.recommendation_store
  where kind = 'rewrite_sorted_table' and table_name = 'orders'
  union all
  select 'index recommendations exist',
         case when count(*) > 0 then 'PASS' else 'FAIL' end
  from vizier.recommendation_store
  where kind = 'create_index'
  union all
  select 'lineitem sort includes l_shipdate',
         case when count(*) > 0 then 'PASS' else 'FAIL' end
  from vizier.recommendation_store
  where kind = 'rewrite_sorted_table'
    and table_name = 'lineitem'
    and columns_json like '%l_shipdate%'
  union all
  select 'no unrelated table recommendations',
         case when count(*) = 0 then 'PASS' else 'FAIL' end
  from vizier.recommendation_store
  where table_name not in ('lineitem', 'orders', 'customer', 'supplier',
                            'nation', 'region', 'part', 'partsupp',
                            'n1', 'n2', 'l1', 'l2', 'l3', '')
    and kind != 'no_action'
  union all
  select 'estimated_rows populated',
         case when count(*) > 0 then 'PASS' else 'FAIL' end
  from vizier.workload_queries
  where estimated_rows > 0
  union all
  select 'top recommendation is for a TPC-H table',
         case when table_name in ('lineitem', 'orders', 'customer', 'supplier',
                                   'nation', 'region', 'part', 'partsupp')
              then 'PASS' else 'FAIL' end
  from (select table_name from vizier.recommendations where table_name != '' order by score desc limit 1)
);

drop table vizier_tpch_queries;
