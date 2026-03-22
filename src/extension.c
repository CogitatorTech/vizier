#include "duckdb_extension.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

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
extern int64_t zig_run_all_advisors(duckdb_connection conn);
extern const char *zig_get_count_recommendations_sql(void);

// ============================================================================
// Global state
// ============================================================================

#define MAX_PENDING 4096
#define MAX_SQL_LEN 4096

#define SIG_LEN 13 // 12 hex chars + null

typedef struct {
  char query_signature[SIG_LEN];    // 12-char hex fingerprint
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

// Normalize, hash, and add a SQL query to the pending capture buffer.
// Returns true on success, false if the buffer is full.
// sig_out (if non-NULL) receives the 12-char hex signature.
static bool add_pending_capture(const char *sql, char sig_out[SIG_LEN]) {
  if (g_pending_count >= MAX_PENDING)
    return false;

  // Normalize SQL for deduplication
  const char *norm_ptr = NULL;
  size_t norm_len = 0;
  zig_normalize_sql(sql, &norm_ptr, &norm_len);

  char norm_buf[MAX_SQL_LEN];
  size_t nc = norm_len < MAX_SQL_LEN - 1 ? norm_len : MAX_SQL_LEN - 1;
  memcpy(norm_buf, norm_ptr, nc);
  norm_buf[nc] = '\0';

  char sig[SIG_LEN];
  format_signature((uint64_t)fnv1a_hash(norm_buf), sig);

  memcpy(g_pending[g_pending_count].query_signature, sig, SIG_LEN);
  strncpy(g_pending[g_pending_count].sql_text, sql, MAX_SQL_LEN - 1);
  g_pending[g_pending_count].sql_text[MAX_SQL_LEN - 1] = '\0';
  strncpy(g_pending[g_pending_count].normalized_sql, norm_buf, MAX_SQL_LEN - 1);
  g_pending[g_pending_count].normalized_sql[MAX_SQL_LEN - 1] = '\0';
  g_pending_count++;

  if (sig_out)
    memcpy(sig_out, sig, SIG_LEN);
  return true;
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
// vizier_configure(key, value) scalar function
// ============================================================================

static void vizier_configure_func(duckdb_function_info info,
                                  duckdb_data_chunk input,
                                  duckdb_vector output) {
  (void)info;
  idx_t size = duckdb_data_chunk_get_size(input);
  duckdb_vector key_vec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_vector val_vec = duckdb_data_chunk_get_vector(input, 1);
  duckdb_string_t *keys = (duckdb_string_t *)duckdb_vector_get_data(key_vec);
  duckdb_string_t *vals = (duckdb_string_t *)duckdb_vector_get_data(val_vec);

  for (idx_t i = 0; i < size; i++) {
    const char *result_str = "error";
    if (g_flush_conn) {
      uint32_t klen = duckdb_string_t_length(keys[i]);
      const char *kptr = duckdb_string_t_data(&keys[i]);
      uint32_t vlen = duckdb_string_t_length(vals[i]);
      const char *vptr = duckdb_string_t_data(&vals[i]);

      char buf[1024];
      snprintf(buf, sizeof(buf),
               "update vizier.settings set value = '%.*s' where key = '%.*s'",
               (int)vlen, vptr, (int)klen, kptr);

      duckdb_result res;
      memset(&res, 0, sizeof(res));
      if (duckdb_query(g_flush_conn, buf, &res) == DuckDBSuccess)
        result_str = "ok";
      duckdb_destroy_result(&res);
    }
    duckdb_vector_assign_string_element_len(output, i, result_str,
                                            strlen(result_str));
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

  CaptureBindData *bind_data =
      (CaptureBindData *)malloc(sizeof(CaptureBindData));
  bind_data->success =
      add_pending_capture(sql_text, bind_data->query_signature);

  duckdb_free(sql_text);
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
// vizier_apply_all(min_score, max_actions, dry_run) table function
// Applies all pending recommendations above min_score, up to max_actions.
// ============================================================================

typedef struct {
  int64_t applied_count;
  int64_t skipped_count;
  bool dry_run;
  bool success;
} ApplyAllBindData;

typedef struct {
  bool done;
} ApplyAllInitData;

static void apply_all_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigint_type =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "applied_count", bigint_type);
  duckdb_bind_add_result_column(info, "skipped_count", bigint_type);
  duckdb_destroy_logical_type(&varchar_type);
  duckdb_destroy_logical_type(&bigint_type);

  // Named parameters
  double min_score = 0.5;
  int64_t max_actions = 10;
  bool dry_run = false;

  duckdb_value p_min = duckdb_bind_get_named_parameter(info, "min_score");
  if (p_min) {
    min_score = duckdb_get_double(p_min);
    duckdb_destroy_value(&p_min);
  }
  duckdb_value p_max = duckdb_bind_get_named_parameter(info, "max_actions");
  if (p_max) {
    max_actions = duckdb_get_int64(p_max);
    duckdb_destroy_value(&p_max);
  }
  duckdb_value p_dry = duckdb_bind_get_named_parameter(info, "dry_run");
  if (p_dry) {
    dry_run = duckdb_get_bool(p_dry);
    duckdb_destroy_value(&p_dry);
  }

  ApplyAllBindData *bind_data =
      (ApplyAllBindData *)malloc(sizeof(ApplyAllBindData));
  bind_data->applied_count = 0;
  bind_data->skipped_count = 0;
  bind_data->dry_run = dry_run;
  bind_data->success = false;

  if (!g_flush_conn)
    goto apply_all_done;

  {
    // Fetch all pending recommendations above min_score
    char fetch_buf[512];
    snprintf(fetch_buf, sizeof(fetch_buf),
             "select recommendation_id::bigint, sql_text::varchar "
             "from vizier.recommendation_store "
             "where status = 'pending' and score >= %f "
             "order by score desc limit %lld",
             min_score, (long long)max_actions);

    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, fetch_buf, &result) != DuckDBSuccess)
      goto apply_all_done;

    duckdb_data_chunk chunk;
    while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
      idx_t chunk_size = duckdb_data_chunk_get_size(chunk);
      if (chunk_size == 0) {
        duckdb_destroy_data_chunk(&chunk);
        break;
      }
      duckdb_vector id_vec = duckdb_data_chunk_get_vector(chunk, 0);
      duckdb_vector sql_vec = duckdb_data_chunk_get_vector(chunk, 1);
      int64_t *ids = (int64_t *)duckdb_vector_get_data(id_vec);
      duckdb_string_t *sqls =
          (duckdb_string_t *)duckdb_vector_get_data(sql_vec);

      for (idx_t row = 0; row < chunk_size; row++) {
        int64_t rec_id = ids[row];
        uint32_t sql_len = duckdb_string_t_length(sqls[row]);
        const char *sql_ptr = duckdb_string_t_data(&sqls[row]);

        char sql_text[4096];
        size_t copy_len =
            sql_len < sizeof(sql_text) - 1 ? sql_len : sizeof(sql_text) - 1;
        memcpy(sql_text, sql_ptr, copy_len);
        sql_text[copy_len] = '\0';

        if (dry_run) {
          bind_data->applied_count++;
          continue;
        }

        // Execute the recommendation
        duckdb_result apply_res;
        memset(&apply_res, 0, sizeof(apply_res));
        bool applied =
            duckdb_query(g_flush_conn, sql_text, &apply_res) == DuckDBSuccess;
        duckdb_destroy_result(&apply_res);

        // Mark status
        char mark_buf[256];
        snprintf(mark_buf, sizeof(mark_buf),
                 "update vizier.recommendation_store set status = '%s' "
                 "where recommendation_id = %lld",
                 applied ? "applied" : "failed", (long long)rec_id);
        duckdb_result mark_res;
        memset(&mark_res, 0, sizeof(mark_res));
        duckdb_query(g_flush_conn, mark_buf, &mark_res);
        duckdb_destroy_result(&mark_res);

        // Log action
        char *escaped = escape_sql_str(sql_text);
        char log_buf[8192];
        snprintf(
            log_buf, sizeof(log_buf),
            "insert into vizier.applied_actions (recommendation_id, sql_text, "
            "success, notes) values (%lld, '%s', %s, '%s')",
            (long long)rec_id, escaped, applied ? "true" : "false",
            applied ? "Applied via apply_all" : "Failed via apply_all");
        free(escaped);
        duckdb_result log_res;
        memset(&log_res, 0, sizeof(log_res));
        duckdb_query(g_flush_conn, log_buf, &log_res);
        duckdb_destroy_result(&log_res);

        if (applied)
          bind_data->applied_count++;
        else
          bind_data->skipped_count++;
      }
      duckdb_destroy_data_chunk(&chunk);
    }
    duckdb_destroy_result(&result);
    bind_data->success = true;
  }

apply_all_done:
  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void apply_all_init(duckdb_init_info info) {
  ApplyAllInitData *init_data =
      (ApplyAllInitData *)malloc(sizeof(ApplyAllInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void apply_all_execute(duckdb_function_info info,
                              duckdb_data_chunk output) {
  ApplyAllInitData *init_data =
      (ApplyAllInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  ApplyAllBindData *bd =
      (ApplyAllBindData *)duckdb_function_get_bind_data(info);

  const char *status = bd->success ? (bd->dry_run ? "dry_run" : "ok") : "error";
  duckdb_vector_assign_string_element_len(
      duckdb_data_chunk_get_vector(output, 0), 0, status, strlen(status));
  ((int64_t *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 1)))[0] = bd->applied_count;
  ((int64_t *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 2)))[0] = bd->skipped_count;

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// vizier_start_capture() / vizier_stop_capture() — session capture
//
// start_capture enables query logging to vizier.session_log table.
// stop_capture reads from session_log, captures each query, and flushes.
// ============================================================================

static bool g_session_capture_active = false;

typedef struct {
  bool success;
  char status[64];
} SessionBindData;

typedef struct {
  bool done;
} SessionInitData;

static void start_capture_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_destroy_logical_type(&varchar_type);

  SessionBindData *bind_data =
      (SessionBindData *)malloc(sizeof(SessionBindData));

  if (g_session_capture_active) {
    strncpy(bind_data->status, "already active", sizeof(bind_data->status));
    bind_data->success = false;
  } else if (g_flush_conn) {
    // Clear previous session (table created by schema init)
    duckdb_result result;
    memset(&result, 0, sizeof(result));
    duckdb_query(g_flush_conn, "delete from vizier.session_log", &result);
    duckdb_destroy_result(&result);

    g_session_capture_active = true;
    strncpy(bind_data->status, "capture started", sizeof(bind_data->status));
    bind_data->success = true;
  } else {
    strncpy(bind_data->status, "error", sizeof(bind_data->status));
    bind_data->success = false;
  }

  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void session_init(duckdb_init_info info) {
  SessionInitData *init_data =
      (SessionInitData *)malloc(sizeof(SessionInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void session_execute(duckdb_function_info info,
                            duckdb_data_chunk output) {
  SessionInitData *init_data =
      (SessionInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  SessionBindData *bind_data =
      (SessionBindData *)duckdb_function_get_bind_data(info);
  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  duckdb_vector_assign_string_element_len(status_vec, 0, bind_data->status,
                                          strlen(bind_data->status));
  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

typedef struct {
  int64_t captured_count;
  bool success;
  char status[64];
} StopCaptureBindData;

static void stop_capture_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigint_type =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "captured_count", bigint_type);
  duckdb_destroy_logical_type(&varchar_type);
  duckdb_destroy_logical_type(&bigint_type);

  StopCaptureBindData *bind_data =
      (StopCaptureBindData *)malloc(sizeof(StopCaptureBindData));
  bind_data->captured_count = 0;
  bind_data->success = false;

  if (!g_session_capture_active) {
    strncpy(bind_data->status, "not active", sizeof(bind_data->status));
  } else if (g_flush_conn) {
    g_session_capture_active = false;

    // Read all queries from session_log and capture them
    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn,
                     "select sql_text::varchar from vizier.session_log",
                     &result) == DuckDBSuccess) {
      int64_t count = 0;
      duckdb_data_chunk chunk;
      while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
        idx_t chunk_size = duckdb_data_chunk_get_size(chunk);
        if (chunk_size == 0) {
          duckdb_destroy_data_chunk(&chunk);
          break;
        }
        duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, 0);
        uint64_t *validity = duckdb_vector_get_validity(vec);
        duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);

        for (idx_t row = 0; row < chunk_size; row++) {
          if (validity && !duckdb_validity_row_is_valid(validity, row))
            continue;
          uint32_t len = duckdb_string_t_length(data[row]);
          const char *ptr = duckdb_string_t_data(&data[row]);
          if (len == 0)
            continue;

          char sql_buf[MAX_SQL_LEN];
          size_t copy_len = len < MAX_SQL_LEN - 1 ? len : MAX_SQL_LEN - 1;
          memcpy(sql_buf, ptr, copy_len);
          sql_buf[copy_len] = '\0';

          if (add_pending_capture(sql_buf, NULL))
            count++;
        }
        duckdb_destroy_data_chunk(&chunk);
      }
      bind_data->captured_count = count;
      bind_data->success = true;
      strncpy(bind_data->status, "ok", sizeof(bind_data->status));
    }
    duckdb_destroy_result(&result);

    // Flush the captured queries
    flush_pending();
  } else {
    strncpy(bind_data->status, "error", sizeof(bind_data->status));
  }

  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void stop_capture_execute(duckdb_function_info info,
                                 duckdb_data_chunk output) {
  SessionInitData *init_data =
      (SessionInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  StopCaptureBindData *bind_data =
      (StopCaptureBindData *)duckdb_function_get_bind_data(info);

  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  duckdb_vector count_vec = duckdb_data_chunk_get_vector(output, 1);

  duckdb_vector_assign_string_element_len(status_vec, 0, bind_data->status,
                                          strlen(bind_data->status));
  ((int64_t *)duckdb_vector_get_data(count_vec))[0] = bind_data->captured_count;

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// Helper: log SQL to session_log if capture is active.
// Called from vizier_session_log() scalar function.
static void session_log_func(duckdb_function_info info, duckdb_data_chunk input,
                             duckdb_vector output) {
  (void)info;
  idx_t size = duckdb_data_chunk_get_size(input);
  duckdb_vector in_vec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_string_t *in_data = (duckdb_string_t *)duckdb_vector_get_data(in_vec);

  for (idx_t i = 0; i < size; i++) {
    bool logged = false;
    if (g_session_capture_active && g_flush_conn) {
      uint32_t len = duckdb_string_t_length(in_data[i]);
      const char *ptr = duckdb_string_t_data(&in_data[i]);

      char *escaped = (char *)malloc(len * 2 + 1);
      char *dst = escaped;
      for (uint32_t j = 0; j < len; j++) {
        if (ptr[j] == '\'') {
          *dst++ = '\'';
          *dst++ = '\'';
        } else {
          *dst++ = ptr[j];
        }
      }
      *dst = '\0';

      char buf[8192];
      snprintf(buf, sizeof(buf),
               "insert into vizier.session_log (sql_text) values ('%s')",
               escaped);
      free(escaped);

      duckdb_result result;
      memset(&result, 0, sizeof(result));
      duckdb_query(g_flush_conn, buf, &result);
      duckdb_destroy_result(&result);
      logged = true;
    }
    const char *val = logged ? "true" : "false";
    duckdb_vector_assign_string_element_len(output, i, val, strlen(val));
  }
}

// ============================================================================
// vizier_import_profile(path) table function
// Reads a DuckDB JSON profiling output file and captures the queries.
// ============================================================================

typedef struct {
  int64_t imported_count;
  bool success;
  char status[64];
} ImportProfileBindData;

typedef struct {
  bool done;
} ImportProfileInitData;

static void import_profile_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigint_type =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "imported_count", bigint_type);
  duckdb_destroy_logical_type(&varchar_type);
  duckdb_destroy_logical_type(&bigint_type);

  duckdb_value param = duckdb_bind_get_parameter(info, 0);
  char *file_path = duckdb_get_varchar(param);
  duckdb_destroy_value(&param);

  ImportProfileBindData *bind_data =
      (ImportProfileBindData *)malloc(sizeof(ImportProfileBindData));
  bind_data->imported_count = 0;
  bind_data->success = false;
  strncpy(bind_data->status, "error", sizeof(bind_data->status));

  if (g_flush_conn && file_path) {
    // Read queries from JSON profiling file
    // DuckDB profiling JSON has "query" field at top level
    char query_buf[1024];
    char *escaped_path = escape_sql_str(file_path);
    snprintf(query_buf, sizeof(query_buf),
             "select query::varchar from read_json_auto('%s') "
             "where query is not null",
             escaped_path);
    free(escaped_path);

    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, query_buf, &result) == DuckDBSuccess) {
      int64_t count = 0;
      duckdb_data_chunk chunk;
      while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
        idx_t chunk_size = duckdb_data_chunk_get_size(chunk);
        if (chunk_size == 0) {
          duckdb_destroy_data_chunk(&chunk);
          break;
        }
        duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, 0);
        uint64_t *validity = duckdb_vector_get_validity(vec);
        duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);

        for (idx_t row = 0; row < chunk_size; row++) {
          if (validity && !duckdb_validity_row_is_valid(validity, row))
            continue;
          uint32_t len = duckdb_string_t_length(data[row]);
          const char *ptr = duckdb_string_t_data(&data[row]);
          if (len == 0)
            continue;

          char sql_buf[MAX_SQL_LEN];
          size_t copy_len = len < MAX_SQL_LEN - 1 ? len : MAX_SQL_LEN - 1;
          memcpy(sql_buf, ptr, copy_len);
          sql_buf[copy_len] = '\0';

          if (add_pending_capture(sql_buf, NULL))
            count++;
        }
        duckdb_destroy_data_chunk(&chunk);
      }
      bind_data->imported_count = count;
      bind_data->success = true;
      strncpy(bind_data->status, "ok", sizeof(bind_data->status));
    }
    duckdb_destroy_result(&result);
  }

  duckdb_free(file_path);
  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void import_profile_init(duckdb_init_info info) {
  ImportProfileInitData *init_data =
      (ImportProfileInitData *)malloc(sizeof(ImportProfileInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void import_profile_execute(duckdb_function_info info,
                                   duckdb_data_chunk output) {
  ImportProfileInitData *init_data =
      (ImportProfileInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  ImportProfileBindData *bind_data =
      (ImportProfileBindData *)duckdb_function_get_bind_data(info);

  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  duckdb_vector count_vec = duckdb_data_chunk_get_vector(output, 1);

  duckdb_vector_assign_string_element_len(status_vec, 0, bind_data->status,
                                          strlen(bind_data->status));
  ((int64_t *)duckdb_vector_get_data(count_vec))[0] = bind_data->imported_count;

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// vizier_capture_bulk(table_name, column_name) table function
// Reads SQL queries from a table column and captures them in bulk.
// ============================================================================

typedef struct {
  int64_t captured_count;
  bool success;
} BulkCaptureBindData;

typedef struct {
  bool done;
} BulkCaptureInitData;

static void bulk_capture_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigint_type =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "captured_count", bigint_type);
  duckdb_destroy_logical_type(&varchar_type);
  duckdb_destroy_logical_type(&bigint_type);

  duckdb_value param0 = duckdb_bind_get_parameter(info, 0);
  duckdb_value param1 = duckdb_bind_get_parameter(info, 1);
  char *table_name = duckdb_get_varchar(param0);
  char *column_name = duckdb_get_varchar(param1);
  duckdb_destroy_value(&param0);
  duckdb_destroy_value(&param1);

  BulkCaptureBindData *bind_data =
      (BulkCaptureBindData *)malloc(sizeof(BulkCaptureBindData));
  bind_data->captured_count = 0;
  bind_data->success = false;

  if (g_flush_conn && table_name && column_name) {
    // Query all SQL from the source table
    char query_buf[512];
    snprintf(query_buf, sizeof(query_buf), "select \"%s\"::varchar from \"%s\"",
             column_name, table_name);

    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, query_buf, &result) == DuckDBSuccess) {
      int64_t count = 0;
      duckdb_data_chunk chunk;
      while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
        idx_t chunk_size = duckdb_data_chunk_get_size(chunk);
        if (chunk_size == 0) {
          duckdb_destroy_data_chunk(&chunk);
          break;
        }
        duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, 0);
        uint64_t *validity = duckdb_vector_get_validity(vec);
        duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);

        for (idx_t row = 0; row < chunk_size; row++) {
          if (validity && !duckdb_validity_row_is_valid(validity, row))
            continue;
          uint32_t len = duckdb_string_t_length(data[row]);
          const char *ptr = duckdb_string_t_data(&data[row]);
          if (len == 0)
            continue;

          char sql_buf[MAX_SQL_LEN];
          size_t copy_len = len < MAX_SQL_LEN - 1 ? len : MAX_SQL_LEN - 1;
          memcpy(sql_buf, ptr, copy_len);
          sql_buf[copy_len] = '\0';

          if (add_pending_capture(sql_buf, NULL))
            count++;
        }
        duckdb_destroy_data_chunk(&chunk);
      }
      bind_data->captured_count = count;
      bind_data->success = true;
    }
    duckdb_destroy_result(&result);
  }

  duckdb_free(table_name);
  duckdb_free(column_name);
  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void bulk_capture_init(duckdb_init_info info) {
  BulkCaptureInitData *init_data =
      (BulkCaptureInitData *)malloc(sizeof(BulkCaptureInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void bulk_capture_execute(duckdb_function_info info,
                                 duckdb_data_chunk output) {
  BulkCaptureInitData *init_data =
      (BulkCaptureInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  BulkCaptureBindData *bind_data =
      (BulkCaptureBindData *)duckdb_function_get_bind_data(info);

  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  duckdb_vector count_vec = duckdb_data_chunk_get_vector(output, 1);

  const char *status = bind_data->success ? "ok" : "error";
  duckdb_vector_assign_string_element_len(status_vec, 0, status,
                                          strlen(status));
  ((int64_t *)duckdb_vector_get_data(count_vec))[0] = bind_data->captured_count;

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// vizier_apply(recommendation_id, dry_run => true/false) table function
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

  // Check dry_run named parameter (defaults to false)
  bool dry_run = false;
  duckdb_value dry_run_param = duckdb_bind_get_named_parameter(info, "dry_run");
  if (dry_run_param) {
    dry_run = duckdb_get_bool(dry_run_param);
    duckdb_destroy_value(&dry_run_param);
  }

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

        if (dry_run) {
          // Dry run: just return the SQL without executing
          bind_data->success = true;
          strncpy(bind_data->status, "dry_run", sizeof(bind_data->status));
        } else {
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
          snprintf(log_buf, sizeof(log_buf),
                   "insert into vizier.applied_actions (recommendation_id, "
                   "sql_text, "
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
        }
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
// vizier_benchmark(sql, runs) table function
// ============================================================================

static double get_time_ms(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

static int compare_double(const void *a, const void *b) {
  double da = *(const double *)a, db = *(const double *)b;
  return (da > db) - (da < db);
}

#define MAX_BENCH_RUNS 1000

typedef struct {
  int64_t runs;
  double min_ms;
  double avg_ms;
  double max_ms;
  double p95_ms;
  bool success;
} BenchmarkBindData;

typedef struct {
  bool done;
} BenchmarkInitData;

static void benchmark_bind(duckdb_bind_info info) {
  duckdb_logical_type bigint_type =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type double_type =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_bind_add_result_column(info, "runs", bigint_type);
  duckdb_bind_add_result_column(info, "min_ms", double_type);
  duckdb_bind_add_result_column(info, "avg_ms", double_type);
  duckdb_bind_add_result_column(info, "p95_ms", double_type);
  duckdb_bind_add_result_column(info, "max_ms", double_type);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_destroy_logical_type(&bigint_type);
  duckdb_destroy_logical_type(&double_type);
  duckdb_destroy_logical_type(&varchar_type);

  duckdb_value param0 = duckdb_bind_get_parameter(info, 0);
  char *sql_text = duckdb_get_varchar(param0);
  duckdb_destroy_value(&param0);

  duckdb_value param1 = duckdb_bind_get_parameter(info, 1);
  int64_t num_runs = duckdb_get_int64(param1);
  duckdb_destroy_value(&param1);

  if (num_runs < 1)
    num_runs = 1;
  if (num_runs > MAX_BENCH_RUNS)
    num_runs = MAX_BENCH_RUNS;

  BenchmarkBindData *bind_data =
      (BenchmarkBindData *)malloc(sizeof(BenchmarkBindData));
  memset(bind_data, 0, sizeof(*bind_data));
  bind_data->runs = num_runs;

  if (g_flush_conn && sql_text) {
    double *timings = (double *)malloc(sizeof(double) * (size_t)num_runs);
    bool all_ok = true;

    for (int64_t i = 0; i < num_runs; i++) {
      double start = get_time_ms();
      duckdb_result result;
      memset(&result, 0, sizeof(result));
      if (duckdb_query(g_flush_conn, sql_text, &result) != DuckDBSuccess) {
        all_ok = false;
        duckdb_destroy_result(&result);
        break;
      }
      duckdb_destroy_result(&result);
      timings[i] = get_time_ms() - start;
    }

    if (all_ok) {
      qsort(timings, (size_t)num_runs, sizeof(double), compare_double);

      double total = 0;
      for (int64_t i = 0; i < num_runs; i++)
        total += timings[i];

      bind_data->min_ms = timings[0];
      bind_data->max_ms = timings[num_runs - 1];
      bind_data->avg_ms = total / (double)num_runs;
      int64_t p95_idx = (int64_t)((double)num_runs * 0.95);
      if (p95_idx >= num_runs)
        p95_idx = num_runs - 1;
      bind_data->p95_ms = timings[p95_idx];
      bind_data->success = true;
    }

    free(timings);
  }

  duckdb_free(sql_text);
  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void benchmark_init(duckdb_init_info info) {
  BenchmarkInitData *init_data =
      (BenchmarkInitData *)malloc(sizeof(BenchmarkInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void benchmark_execute(duckdb_function_info info,
                              duckdb_data_chunk output) {
  BenchmarkInitData *init_data =
      (BenchmarkInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  BenchmarkBindData *bd =
      (BenchmarkBindData *)duckdb_function_get_bind_data(info);

  ((int64_t *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 0)))[0] = bd->runs;
  ((double *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 1)))[0] = bd->min_ms;
  ((double *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 2)))[0] = bd->avg_ms;
  ((double *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 3)))[0] = bd->p95_ms;
  ((double *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 4)))[0] = bd->max_ms;

  const char *status = bd->success ? "ok" : "error";
  duckdb_vector_assign_string_element_len(
      duckdb_data_chunk_get_vector(output, 5), 0, status, strlen(status));

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// vizier_compare(recommendation_id) table function
// Benchmarks queries before/after applying a recommendation, stores results.
// ============================================================================

typedef struct {
  char status[64];
  double baseline_ms;
  double candidate_ms;
  double speedup;
  bool success;
} CompareBindData;

typedef struct {
  bool done;
} CompareInitData;

#define COMPARE_BENCH_RUNS 5

static void compare_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type double_type =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "baseline_ms", double_type);
  duckdb_bind_add_result_column(info, "candidate_ms", double_type);
  duckdb_bind_add_result_column(info, "speedup", double_type);
  duckdb_destroy_logical_type(&varchar_type);
  duckdb_destroy_logical_type(&double_type);

  duckdb_value param = duckdb_bind_get_parameter(info, 0);
  int64_t rec_id = duckdb_get_int64(param);
  duckdb_destroy_value(&param);

  CompareBindData *bind_data =
      (CompareBindData *)malloc(sizeof(CompareBindData));
  strncpy(bind_data->status, "error", sizeof(bind_data->status));
  bind_data->baseline_ms = 0;
  bind_data->candidate_ms = 0;
  bind_data->speedup = 0;
  bind_data->success = false;

  if (!g_flush_conn)
    goto compare_done;

  // Fetch recommendation SQL and a sample query for that table
  char fetch_buf[512];
  snprintf(fetch_buf, sizeof(fetch_buf),
           "select r.sql_text::varchar, "
           "(select w.sample_sql from vizier.workload_queries w "
           " join vizier.workload_predicates p on w.query_signature = "
           "p.query_signature "
           " where p.table_name = r.table_name limit 1)::varchar "
           "from vizier.recommendation_store r "
           "where r.recommendation_id = %lld and r.status = 'pending'",
           (long long)rec_id);

  duckdb_result result;
  memset(&result, 0, sizeof(result));
  if (duckdb_query(g_flush_conn, fetch_buf, &result) != DuckDBSuccess)
    goto compare_done;

  duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
  if (!chunk || duckdb_data_chunk_get_size(chunk) == 0) {
    if (chunk)
      duckdb_destroy_data_chunk(&chunk);
    duckdb_destroy_result(&result);
    strncpy(bind_data->status, "not found", sizeof(bind_data->status));
    goto compare_done;
  }

  // Get rec SQL and sample query
  char rec_sql[4096] = {0};
  char sample_sql[4096] = {0};
  {
    duckdb_vector v0 = duckdb_data_chunk_get_vector(chunk, 0);
    duckdb_string_t *d0 = (duckdb_string_t *)duckdb_vector_get_data(v0);
    uint32_t l0 = duckdb_string_t_length(d0[0]);
    const char *p0 = duckdb_string_t_data(&d0[0]);
    if (l0 < sizeof(rec_sql)) {
      memcpy(rec_sql, p0, l0);
      rec_sql[l0] = '\0';
    }

    duckdb_vector v1 = duckdb_data_chunk_get_vector(chunk, 1);
    uint64_t *val1 = duckdb_vector_get_validity(v1);
    if (val1 == NULL || duckdb_validity_row_is_valid(val1, 0)) {
      duckdb_string_t *d1 = (duckdb_string_t *)duckdb_vector_get_data(v1);
      uint32_t l1 = duckdb_string_t_length(d1[0]);
      const char *p1 = duckdb_string_t_data(&d1[0]);
      if (l1 < sizeof(sample_sql)) {
        memcpy(sample_sql, p1, l1);
        sample_sql[l1] = '\0';
      }
    }
  }
  duckdb_destroy_data_chunk(&chunk);
  duckdb_destroy_result(&result);

  if (sample_sql[0] == '\0') {
    strncpy(bind_data->status, "no sample query", sizeof(bind_data->status));
    goto compare_done;
  }

  // Baseline benchmark
  double baseline_total = 0;
  {
    for (int i = 0; i < COMPARE_BENCH_RUNS; i++) {
      double start = get_time_ms();
      duckdb_result br;
      memset(&br, 0, sizeof(br));
      duckdb_query(g_flush_conn, sample_sql, &br);
      duckdb_destroy_result(&br);
      baseline_total += get_time_ms() - start;
    }
  }
  bind_data->baseline_ms = baseline_total / COMPARE_BENCH_RUNS;

  // Apply the recommendation
  {
    duckdb_result ar;
    memset(&ar, 0, sizeof(ar));
    bool applied = duckdb_query(g_flush_conn, rec_sql, &ar) == DuckDBSuccess;
    duckdb_destroy_result(&ar);
    if (!applied) {
      strncpy(bind_data->status, "apply failed", sizeof(bind_data->status));
      goto compare_done;
    }
    // Mark as applied
    char mark_buf[256];
    snprintf(mark_buf, sizeof(mark_buf),
             "update vizier.recommendation_store set status = 'applied' "
             "where recommendation_id = %lld",
             (long long)rec_id);
    duckdb_result mr;
    memset(&mr, 0, sizeof(mr));
    duckdb_query(g_flush_conn, mark_buf, &mr);
    duckdb_destroy_result(&mr);
  }

  // Candidate benchmark (after applying)
  double candidate_total = 0;
  {
    for (int i = 0; i < COMPARE_BENCH_RUNS; i++) {
      double start = get_time_ms();
      duckdb_result br;
      memset(&br, 0, sizeof(br));
      duckdb_query(g_flush_conn, sample_sql, &br);
      duckdb_destroy_result(&br);
      candidate_total += get_time_ms() - start;
    }
  }
  bind_data->candidate_ms = candidate_total / COMPARE_BENCH_RUNS;
  bind_data->speedup = bind_data->candidate_ms > 0
                           ? bind_data->baseline_ms / bind_data->candidate_ms
                           : 0;

  // Store in benchmark_results
  {
    char ins_buf[1024];
    snprintf(ins_buf, sizeof(ins_buf),
             "insert into vizier.benchmark_results "
             "(recommendation_id, baseline_ms, candidate_ms, speedup) "
             "values (%lld, %f, %f, %f)",
             (long long)rec_id, bind_data->baseline_ms, bind_data->candidate_ms,
             bind_data->speedup);
    duckdb_result ir;
    memset(&ir, 0, sizeof(ir));
    duckdb_query(g_flush_conn, ins_buf, &ir);
    duckdb_destroy_result(&ir);
  }

  bind_data->success = true;
  strncpy(bind_data->status, "ok", sizeof(bind_data->status));

compare_done:
  duckdb_bind_set_bind_data(info, bind_data, free);
  duckdb_bind_set_cardinality(info, 1, true);
}

static void compare_init(duckdb_init_info info) {
  CompareInitData *init_data =
      (CompareInitData *)malloc(sizeof(CompareInitData));
  init_data->done = false;
  duckdb_init_set_init_data(info, init_data, free);
}

static void compare_execute(duckdb_function_info info,
                            duckdb_data_chunk output) {
  CompareInitData *init_data =
      (CompareInitData *)duckdb_function_get_init_data(info);
  if (init_data->done) {
    duckdb_data_chunk_set_size(output, 0);
    return;
  }

  CompareBindData *bd = (CompareBindData *)duckdb_function_get_bind_data(info);

  duckdb_vector status_vec = duckdb_data_chunk_get_vector(output, 0);
  const char *status = bd->status;
  duckdb_vector_assign_string_element_len(status_vec, 0, status,
                                          strlen(status));

  ((double *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 1)))[0] = bd->baseline_ms;
  ((double *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 2)))[0] = bd->candidate_ms;
  ((double *)duckdb_vector_get_data(
      duckdb_data_chunk_get_vector(output, 3)))[0] = bd->speedup;

  duckdb_data_chunk_set_size(output, 1);
  init_data->done = true;
}

// ============================================================================
// vizier_analyze() table function — runs all advisors
// ============================================================================

typedef struct {
  int64_t recommendation_count;
  int64_t advisor_failures;
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

  bool ok = false;
  int64_t count = 0;
  int64_t advisor_result = -1;

  if (g_flush_conn) {
    // Run all advisors (clear pending, run each advisor, no_action last)
    // Returns: -1 = clear failed, 0 = all ok, >0 = number of advisor failures
    advisor_result = zig_run_all_advisors(g_flush_conn);
    ok = (advisor_result >= 0);

    // Count results
    duckdb_result result;
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
  }

  AnalyzeBindData *bind_data =
      (AnalyzeBindData *)malloc(sizeof(AnalyzeBindData));
  bind_data->recommendation_count = count;
  bind_data->advisor_failures = (ok && advisor_result > 0) ? advisor_result : 0;
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

// Helper macro to register a table function with no parameters.
#define REGISTER_TABLE_FUNC_0(conn, access, info, name, bind_fn, init_fn,      \
                              exec_fn)                                         \
  do {                                                                         \
    duckdb_table_function _f = duckdb_create_table_function();                 \
    duckdb_table_function_set_name(_f, name);                                  \
    duckdb_table_function_set_bind(_f, bind_fn);                               \
    duckdb_table_function_set_init(_f, init_fn);                               \
    duckdb_table_function_set_function(_f, exec_fn);                           \
    if (duckdb_register_table_function(conn, _f) == DuckDBError) {             \
      access->set_error(info, "Vizier: failed to register " name);             \
      duckdb_destroy_table_function(&_f);                                      \
      return false;                                                            \
    }                                                                          \
    duckdb_destroy_table_function(&_f);                                        \
  } while (0)

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

  // ---- Register vizier_configure(key, value) scalar function ----
  {
    duckdb_scalar_function func = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(func, "vizier_configure");
    duckdb_logical_type vc_type =
        duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_scalar_function_add_parameter(func, vc_type);
    duckdb_scalar_function_add_parameter(func, vc_type);
    duckdb_scalar_function_set_return_type(func, vc_type);
    duckdb_destroy_logical_type(&vc_type);
    duckdb_scalar_function_set_function(func, vizier_configure_func);
    if (duckdb_register_scalar_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_configure");
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
      access->set_error(info, "Vizier: failed to register vizier_capture");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  REGISTER_TABLE_FUNC_0(conn, access, info, "vizier_flush", flush_bind,
                        flush_init, flush_execute);

  // ---- Register vizier_apply_all(min_score, max_actions, dry_run) ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_apply_all");
    duckdb_logical_type dbl_type =
        duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
    duckdb_logical_type bi_type =
        duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_logical_type bl_type =
        duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_table_function_add_named_parameter(func, "min_score", dbl_type);
    duckdb_table_function_add_named_parameter(func, "max_actions", bi_type);
    duckdb_table_function_add_named_parameter(func, "dry_run", bl_type);
    duckdb_destroy_logical_type(&dbl_type);
    duckdb_destroy_logical_type(&bi_type);
    duckdb_destroy_logical_type(&bl_type);
    duckdb_table_function_set_bind(func, apply_all_bind);
    duckdb_table_function_set_init(func, apply_all_init);
    duckdb_table_function_set_function(func, apply_all_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_apply_all");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  REGISTER_TABLE_FUNC_0(conn, access, info, "vizier_start_capture",
                        start_capture_bind, session_init, session_execute);
  REGISTER_TABLE_FUNC_0(conn, access, info, "vizier_stop_capture",
                        stop_capture_bind, session_init, stop_capture_execute);

  // ---- Register vizier_session_log(sql) scalar function ----
  {
    duckdb_scalar_function func = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(func, "vizier_session_log");
    duckdb_logical_type varchar_type3 =
        duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_scalar_function_add_parameter(func, varchar_type3);
    duckdb_scalar_function_set_return_type(func, varchar_type3);
    duckdb_destroy_logical_type(&varchar_type3);
    duckdb_scalar_function_set_function(func, session_log_func);
    if (duckdb_register_scalar_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_session_log");
      duckdb_destroy_scalar_function(&func);
      return false;
    }
    duckdb_destroy_scalar_function(&func);
  }

  // ---- Register vizier_import_profile(path) table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_import_profile");
    duckdb_logical_type varchar_type4 =
        duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(func, varchar_type4);
    duckdb_destroy_logical_type(&varchar_type4);
    duckdb_table_function_set_bind(func, import_profile_bind);
    duckdb_table_function_set_init(func, import_profile_init);
    duckdb_table_function_set_function(func, import_profile_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info,
                        "Vizier: failed to register vizier_import_profile");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  // ---- Register vizier_capture_bulk(table, column) table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_capture_bulk");
    duckdb_logical_type varchar_type2 =
        duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(func, varchar_type2);
    duckdb_table_function_add_parameter(func, varchar_type2);
    duckdb_destroy_logical_type(&varchar_type2);
    duckdb_table_function_set_bind(func, bulk_capture_bind);
    duckdb_table_function_set_init(func, bulk_capture_init);
    duckdb_table_function_set_function(func, bulk_capture_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_capture_bulk");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  // ---- Register vizier_apply(recommendation_id, dry_run => bool) ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_apply");
    duckdb_logical_type bigint_type =
        duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_logical_type bool_type =
        duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_table_function_add_parameter(func, bigint_type);
    duckdb_table_function_add_named_parameter(func, "dry_run", bool_type);
    duckdb_destroy_logical_type(&bigint_type);
    duckdb_destroy_logical_type(&bool_type);
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

  // ---- Register vizier_compare(recommendation_id) table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_compare");
    duckdb_logical_type bigint_type2 =
        duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_table_function_add_parameter(func, bigint_type2);
    duckdb_destroy_logical_type(&bigint_type2);
    duckdb_table_function_set_bind(func, compare_bind);
    duckdb_table_function_set_init(func, compare_init);
    duckdb_table_function_set_function(func, compare_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_compare");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  // ---- Register vizier_benchmark(sql, runs) table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_benchmark");
    duckdb_logical_type varchar_type =
        duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_logical_type bigint_type =
        duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_table_function_add_parameter(func, varchar_type);
    duckdb_table_function_add_parameter(func, bigint_type);
    duckdb_destroy_logical_type(&varchar_type);
    duckdb_destroy_logical_type(&bigint_type);
    duckdb_table_function_set_bind(func, benchmark_bind);
    duckdb_table_function_set_init(func, benchmark_init);
    duckdb_table_function_set_function(func, benchmark_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info, "Vizier: failed to register vizier_benchmark");
      duckdb_destroy_table_function(&func);
      return false;
    }
    duckdb_destroy_table_function(&func);
  }

  REGISTER_TABLE_FUNC_0(conn, access, info, "vizier_analyze", analyze_bind,
                        analyze_init, analyze_execute);

  return true;
}
