#include "duckdb_extension.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// This macro declares the duckdb_ext_api global and sets up the extension API
DUCKDB_EXTENSION_EXTERN

// ============================================================================
// Forward declarations for Zig functions
// ============================================================================
extern bool zig_init_schema(duckdb_database db);
extern const char *zig_get_version(void);
extern bool zig_extract_and_store(duckdb_connection conn,
                                  const char *query_signature,
                                  const char *sql_text);
extern void zig_normalize_sql(const char *sql, const char **out_ptr,
                              size_t *out_len);
extern const char *zig_get_clear_pending_sql(void);
extern const char *zig_get_index_advisor_sql(void);
extern const char *zig_get_sort_advisor_sql(void);
extern const char *zig_get_count_recommendations_sql(void);

// ============================================================================
// Global state
// ============================================================================

#define MAX_PENDING 4096
#define MAX_SQL_LEN 4096

#define SIG_LEN 13 // 12 hex chars + null

typedef struct {
  char query_signature[SIG_LEN]; // 12-char hex fingerprint
  char sql_text[MAX_SQL_LEN];       // original SQL (sample)
  char normalized_sql[MAX_SQL_LEN]; // normalized SQL (literals replaced)
} PendingCapture;

// Format a 64-bit hash as a 12-character hex signature (48-bit truncation)
static void format_signature(uint64_t hash, char out[SIG_LEN]) {
  uint64_t trunc = hash & 0xFFFFFFFFFFFFULL; // keep lower 48 bits
  snprintf(out, SIG_LEN, "%012llx", (unsigned long long)trunc);
}

static PendingCapture g_pending[MAX_PENDING];
static int g_pending_count = 0;
static duckdb_connection g_flush_conn = NULL;

// Escape single quotes for SQL embedding
static char *escape_sql_str(const char *src) {
  size_t len = strlen(src);
  char *escaped = (char *)malloc(len * 2 + 1);
  char *dst = escaped;
  for (const char *p = src; *p; p++) {
    if (*p == '\'') {
      *dst++ = '\'';
      *dst++ = '\'';
    } else {
      *dst++ = *p;
    }
  }
  *dst = '\0';
  return escaped;
}

// FNV-1a hash
static int64_t fnv1a_hash(const char *str) {
  uint64_t h = 14695981039346656037ULL;
  for (const char *p = str; *p; p++) {
    h ^= (uint64_t)(unsigned char)(*p);
    h *= 1099511628211ULL;
  }
  return (int64_t)h;
}

// Flush all pending captures to the database using the persistent connection.
static bool flush_pending(void) {
  if (g_pending_count == 0 || !g_flush_conn)
    return true;

  bool all_ok = true;
  for (int i = 0; i < g_pending_count; i++) {
    char *escaped_sample = escape_sql_str(g_pending[i].sql_text);
    char *escaped_norm = escape_sql_str(g_pending[i].normalized_sql);
    char buf[16384];
    snprintf(buf, sizeof(buf),
             "insert into vizier.workload_queries (query_signature, "
             "normalized_sql, sample_sql) "
             "values ('%s', '%s', '%s') "
             "on conflict (query_signature) do update set "
             "last_seen = now(), execution_count = "
             "workload_queries.execution_count + 1",
             g_pending[i].query_signature, escaped_norm, escaped_sample);
    free(escaped_sample);
    free(escaped_norm);

    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, buf, &result) != DuckDBSuccess) {
      all_ok = false;
    } else {
      // Extract predicates and update tables_json/columns_json
      zig_extract_and_store(g_flush_conn, g_pending[i].query_signature,
                            g_pending[i].sql_text);
    }
    duckdb_destroy_result(&result);
  }

  g_pending_count = 0;
  return all_ok;
}

// ============================================================================
// vizier_version() scalar function
// ============================================================================

static void vizier_version_func(duckdb_function_info info,
                                duckdb_data_chunk input, duckdb_vector output) {
  (void)info;
  idx_t size = duckdb_data_chunk_get_size(input);
  const char *ver = zig_get_version();
  idx_t len = strlen(ver);
  for (idx_t i = 0; i < size; i++) {
    duckdb_vector_assign_string_element_len(output, i, ver, len);
  }
}

// ============================================================================
// vizier_capture(sql VARCHAR) table function
//
// Captures query metadata to an in-memory buffer.
// Data is persisted when vizier_flush() is called.
// ============================================================================

typedef struct {
  char query_signature[SIG_LEN];
  bool success;
} CaptureBindData;

typedef struct {
  bool done;
} CaptureInitData;

static void capture_query_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "query_signature", varchar_type);
  duckdb_bind_add_result_column(info, "message", varchar_type);
  duckdb_destroy_logical_type(&varchar_type);

  duckdb_value param = duckdb_bind_get_parameter(info, 0);
  char *sql_text = duckdb_get_varchar(param);
  duckdb_destroy_value(&param);

  // Normalize SQL for deduplication
  const char *norm_ptr = NULL;
  size_t norm_len = 0;
  zig_normalize_sql(sql_text, &norm_ptr, &norm_len);

  char norm_buf[MAX_SQL_LEN];
  size_t copy_len = norm_len < MAX_SQL_LEN - 1 ? norm_len : MAX_SQL_LEN - 1;
  memcpy(norm_buf, norm_ptr, copy_len);
  norm_buf[copy_len] = '\0';

  // Hash normalized SQL and format as 12-char hex signature
  char sig[SIG_LEN];
  format_signature((uint64_t)fnv1a_hash(norm_buf), sig);

  bool success = false;

  if (g_pending_count < MAX_PENDING) {
    memcpy(g_pending[g_pending_count].query_signature, sig, SIG_LEN);
    strncpy(g_pending[g_pending_count].sql_text, sql_text, MAX_SQL_LEN - 1);
    g_pending[g_pending_count].sql_text[MAX_SQL_LEN - 1] = '\0';
    strncpy(g_pending[g_pending_count].normalized_sql, norm_buf,
            MAX_SQL_LEN - 1);
    g_pending[g_pending_count].normalized_sql[MAX_SQL_LEN - 1] = '\0';
    g_pending_count++;
    success = true;
  }

  duckdb_free(sql_text);

  CaptureBindData *bind_data =
      (CaptureBindData *)malloc(sizeof(CaptureBindData));
  memcpy(bind_data->query_signature, sig, SIG_LEN);
  bind_data->success = success;
  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void capture_query_init(duckdb_init_info info) {
  CaptureInitData *init_data =
      (CaptureInitData *)malloc(sizeof(CaptureInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void capture_query_execute(duckdb_function_info info,
                                  duckdb_data_chunk output) {
  CaptureInitData *init_data =
      (CaptureInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  CaptureBindData *bind_data =
      (CaptureBindData *)duckdb_function_get_bind_data(info);

  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  duckdb_vector sig_vec = duckdb_data_chunk_get_vector(output, 1);
  duckdb_vector msg_vec = duckdb_data_chunk_get_vector(output, 2);

  const char *status = bind_data->success ? "ok" : "error";
  const char *message = bind_data->success ? "Query captured (pending flush)"
                                           : "Capture buffer full";

  duckdb_vector_assign_string_element_len(status_vec, 0, status,
                                          strlen(status));
  duckdb_vector_assign_string_element_len(sig_vec, 0,
                                          bind_data->query_signature, 12);
  duckdb_vector_assign_string_element_len(msg_vec, 0, message, strlen(message));

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// vizier_flush() table function - persists pending captures to DB tables
// ============================================================================

typedef struct {
  int flushed_count;
  bool success;
} FlushBindData;

typedef struct {
  bool done;
} FlushInitData;

static void flush_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigint_type =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "flushed_count", bigint_type);
  duckdb_destroy_logical_type(&varchar_type);
  duckdb_destroy_logical_type(&bigint_type);

  int count = g_pending_count;
  bool ok = flush_pending();

  FlushBindData *bind_data = (FlushBindData *)malloc(sizeof(FlushBindData));
  bind_data->flushed_count = count;
  bind_data->success = ok;
  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void flush_init(duckdb_init_info info) {
  FlushInitData *init_data = (FlushInitData *)malloc(sizeof(FlushInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void flush_execute(duckdb_function_info info, duckdb_data_chunk output) {
  FlushInitData *init_data =
      (FlushInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  FlushBindData *bind_data =
      (FlushBindData *)duckdb_function_get_bind_data(info);

  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  duckdb_vector count_vec = duckdb_data_chunk_get_vector(output, 1);

  const char *status = bind_data->success ? "ok" : "error";
  duckdb_vector_assign_string_element_len(status_vec, 0, status,
                                          strlen(status));
  ((int64_t *)duckdb_vector_get_data(count_vec))[0] = bind_data->flushed_count;

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// vizier_apply(recommendation_id) table function
// ============================================================================

typedef struct {
  char status[64];
  char sql_text[4096];
  bool success;
} ApplyBindData;

typedef struct {
  bool done;
} ApplyInitData;

static void apply_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "sql_executed", varchar_type);
  duckdb_destroy_logical_type(&varchar_type);

  duckdb_value param = duckdb_bind_get_parameter(info, 0);
  int64_t rec_id = duckdb_get_int64(param);
  duckdb_destroy_value(&param);

  ApplyBindData *bind_data = (ApplyBindData *)malloc(sizeof(ApplyBindData));
  strncpy(bind_data->status, "error", sizeof(bind_data->status));
  bind_data->sql_text[0] = '\0';
  bind_data->success = false;

  if (g_flush_conn) {
    // Fetch the recommendation
    char fetch_buf[512];
    snprintf(fetch_buf, sizeof(fetch_buf),
             "select sql_text::varchar from vizier.recommendation_store "
             "where recommendation_id = %lld and status = 'pending'",
             (long long)rec_id);

    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, fetch_buf, &result) == DuckDBSuccess) {
      duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
      if (chunk && duckdb_data_chunk_get_size(chunk) > 0) {
        duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, 0);
        duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
        uint32_t len = duckdb_string_t_length(data[0]);
        const char *ptr = duckdb_string_t_data(&data[0]);
        if (len < sizeof(bind_data->sql_text)) {
          memcpy(bind_data->sql_text, ptr, len);
          bind_data->sql_text[len] = '\0';
        }
        duckdb_destroy_data_chunk(&chunk);

        // Execute the recommendation SQL
        duckdb_result apply_result;
        memset(&apply_result, 0, sizeof(apply_result));
        bool applied = duckdb_query(g_flush_conn, bind_data->sql_text,
                                    &apply_result) == DuckDBSuccess;
        duckdb_destroy_result(&apply_result);

        // Mark as applied and log
        char mark_buf[256];
        snprintf(mark_buf, sizeof(mark_buf),
                 "update vizier.recommendation_store set status = '%s' "
                 "where recommendation_id = %lld",
                 applied ? "applied" : "failed", (long long)rec_id);
        duckdb_result mark_result;
        memset(&mark_result, 0, sizeof(mark_result));
        duckdb_query(g_flush_conn, mark_buf, &mark_result);
        duckdb_destroy_result(&mark_result);

        // Log the action
        char *escaped = escape_sql_str(bind_data->sql_text);
        char log_buf[8192];
        snprintf(
            log_buf, sizeof(log_buf),
            "insert into vizier.applied_actions (recommendation_id, sql_text, "
            "success, notes) values (%lld, '%s', %s, '%s')",
            (long long)rec_id, escaped, applied ? "true" : "false",
            applied ? "Applied successfully" : "Apply failed");
        free(escaped);
        duckdb_result log_result;
        memset(&log_result, 0, sizeof(log_result));
        duckdb_query(g_flush_conn, log_buf, &log_result);
        duckdb_destroy_result(&log_result);

        bind_data->success = applied;
        strncpy(bind_data->status, applied ? "applied" : "failed",
                sizeof(bind_data->status));
      } else {
        if (chunk)
          duckdb_destroy_data_chunk(&chunk);
        strncpy(bind_data->status, "not found", sizeof(bind_data->status));
      }
    }
    duckdb_destroy_result(&result);
  }

  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void apply_init(duckdb_init_info info) {
  ApplyInitData *init_data = (ApplyInitData *)malloc(sizeof(ApplyInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void apply_execute(duckdb_function_info info, duckdb_data_chunk output) {
  ApplyInitData *init_data =
      (ApplyInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  ApplyBindData *bind_data =
      (ApplyBindData *)duckdb_function_get_bind_data(info);

  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  duckdb_vector sql_vec = duckdb_data_chunk_get_vector(output, 1);

  duckdb_vector_assign_string_element_len(status_vec, 0, bind_data->status,
                                          strlen(bind_data->status));
  duckdb_vector_assign_string_element_len(sql_vec, 0, bind_data->sql_text,
                                          strlen(bind_data->sql_text));

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// vizier_analyze() table function — runs all advisors
// ============================================================================

typedef struct {
  int64_t recommendation_count;
  bool success;
} AnalyzeBindData;

typedef struct {
  bool done;
} AnalyzeInitData;

static void analyze_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigint_type =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "recommendations", bigint_type);
  duckdb_destroy_logical_type(&varchar_type);
  duckdb_destroy_logical_type(&bigint_type);

  bool ok = true;
  int64_t count = 0;

  if (g_flush_conn) {
    duckdb_result result;

    // Clear old pending recommendations
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, zig_get_clear_pending_sql(), &result) !=
        DuckDBSuccess)
      ok = false;
    duckdb_destroy_result(&result);

    // Run index advisor
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, zig_get_index_advisor_sql(), &result) !=
        DuckDBSuccess)
      ok = false;
    duckdb_destroy_result(&result);

    // Run sort/clustering advisor
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, zig_get_sort_advisor_sql(), &result) !=
        DuckDBSuccess)
      ok = false;
    duckdb_destroy_result(&result);

    // Count results
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, zig_get_count_recommendations_sql(),
                     &result) == DuckDBSuccess) {
      duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
      if (chunk) {
        if (duckdb_data_chunk_get_size(chunk) > 0) {
          duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, 0);
          count = ((int64_t *)duckdb_vector_get_data(vec))[0];
        }
        duckdb_destroy_data_chunk(&chunk);
      }
    }
    duckdb_destroy_result(&result);
  } else {
    ok = false;
  }

  AnalyzeBindData *bind_data =
      (AnalyzeBindData *)malloc(sizeof(AnalyzeBindData));
  bind_data->recommendation_count = count;
  bind_data->success = ok;
  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void analyze_init(duckdb_init_info info) {
  AnalyzeInitData *init_data =
      (AnalyzeInitData *)malloc(sizeof(AnalyzeInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void analyze_execute(duckdb_function_info info,
                            duckdb_data_chunk output) {
  AnalyzeInitData *init_data =
      (AnalyzeInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  AnalyzeBindData *bind_data =
      (AnalyzeBindData *)duckdb_function_get_bind_data(info);

  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  duckdb_vector count_vec = duckdb_data_chunk_get_vector(output, 1);

  const char *status = bind_data->success ? "ok" : "error";
  duckdb_vector_assign_string_element_len(status_vec, 0, status,
                                          strlen(status));
  ((int64_t *)duckdb_vector_get_data(count_vec))[0] =
      bind_data->recommendation_count;

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// Extension entry point
// ============================================================================

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection conn, duckdb_extension_info info,
                            struct duckdb_extension_access *access) {
  // Open a persistent connection for flush operations.
  // This connection stays alive for the lifetime of the extension.
  duckdb_database *db = access->get_database(info);
  if (duckdb_connect(*db, &g_flush_conn) != DuckDBSuccess) {
    access->set_error(info, "Vizier: failed to open persistent connection");
    return false;
  }

  // Initialize vizier schema and metadata tables
  if (!zig_init_schema(*db)) {
    access->set_error(
        info, "Vizier: failed to initialize schema and metadata tables");
    duckdb_disconnect(&g_flush_conn);
    g_flush_conn = NULL;
    return false;
  }

  // ---- Register vizier_version() scalar function ----
  {
    duckdb_scalar_function func = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(func, "vizier_version");
    duckdb_logical_type varchar_type =
        duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_scalar_function_set_return_type(func, varchar_type);
    duckdb_destroy_logical_type(&varchar_type);
    duckdb_scalar_function_set_function(func, vizier_version_func);
    if (duckdb_register_scalar_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_version");
      duckdb_destroy_scalar_function(&func);
      return false;
    }
    duckdb_destroy_scalar_function(&func);
  }

  // ---- Register vizier_capture(sql) table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_capture");
    duckdb_logical_type varchar_type =
        duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(func, varchar_type);
    duckdb_destroy_logical_type(&varchar_type);
    duckdb_table_function_set_bind(func, capture_query_bind);
    duckdb_table_function_set_init(func, capture_query_init);
    duckdb_table_function_set_function(func, capture_query_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info,
                        "Vizier: failed to register vizier_capture");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  // ---- Register vizier_flush() table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_flush");
    duckdb_table_function_set_bind(func, flush_bind);
    duckdb_table_function_set_init(func, flush_init);
    duckdb_table_function_set_function(func, flush_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_flush");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  // ---- Register vizier_apply(recommendation_id) table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_apply");
    duckdb_logical_type bigint_type =
        duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_table_function_add_parameter(func, bigint_type);
    duckdb_destroy_logical_type(&bigint_type);
    duckdb_table_function_set_bind(func, apply_bind);
    duckdb_table_function_set_init(func, apply_init);
    duckdb_table_function_set_function(func, apply_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_apply");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  // ---- Register vizier_analyze() table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_analyze");
    duckdb_table_function_set_bind(func, analyze_bind);
    duckdb_table_function_set_init(func, analyze_init);
    duckdb_table_function_set_function(func, analyze_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_analyze");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  return true;
}
