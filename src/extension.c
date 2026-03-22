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
extern bool zig_extract_and_store(duckdb_connection conn, int64_t query_hash,
                                  const char *sql_text);

// ============================================================================
// Global state
// ============================================================================

#define MAX_PENDING 4096
#define MAX_SQL_LEN 4096

typedef struct {
  int64_t query_hash;
  char sql_text[MAX_SQL_LEN];
} PendingCapture;

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
    char *escaped = escape_sql_str(g_pending[i].sql_text);
    char buf[16384];
    snprintf(buf, sizeof(buf),
             "INSERT INTO vizier.workload_queries (query_hash, normalized_sql, "
             "sample_sql) "
             "VALUES (%lld, '%s', '%s') "
             "ON CONFLICT (query_hash) DO UPDATE SET "
             "last_seen = now(), execution_count = "
             "workload_queries.execution_count + 1",
             (long long)g_pending[i].query_hash, escaped, escaped);
    free(escaped);

    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(g_flush_conn, buf, &result) != DuckDBSuccess) {
      all_ok = false;
    } else {
      // Extract predicates and update tables_json/columns_json
      zig_extract_and_store(g_flush_conn, g_pending[i].query_hash,
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
// vizier_capture_query(sql VARCHAR) table function
//
// Captures query metadata to an in-memory buffer.
// Data is persisted when vizier_flush() is called.
// ============================================================================

typedef struct {
  int64_t query_hash;
  bool success;
} CaptureBindData;

typedef struct {
  bool done;
} CaptureInitData;

static void capture_query_bind(duckdb_bind_info info) {
  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigint_type =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_bind_add_result_column(info, "status", varchar_type);
  duckdb_bind_add_result_column(info, "query_hash", bigint_type);
  duckdb_bind_add_result_column(info, "message", varchar_type);
  duckdb_destroy_logical_type(&varchar_type);
  duckdb_destroy_logical_type(&bigint_type);

  duckdb_value param = duckdb_bind_get_parameter(info, 0);
  char *sql_text = duckdb_get_varchar(param);
  duckdb_destroy_value(&param);

  int64_t hash = fnv1a_hash(sql_text);
  bool success = false;

  if (g_pending_count < MAX_PENDING) {
    g_pending[g_pending_count].query_hash = hash;
    strncpy(g_pending[g_pending_count].sql_text, sql_text, MAX_SQL_LEN - 1);
    g_pending[g_pending_count].sql_text[MAX_SQL_LEN - 1] = '\0';
    g_pending_count++;
    success = true;
  }

  duckdb_free(sql_text);

  CaptureBindData *bind_data =
      (CaptureBindData *)malloc(sizeof(CaptureBindData));
  bind_data->query_hash = hash;
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
  duckdb_vector hash_vec = duckdb_data_chunk_get_vector(output, 1);
  duckdb_vector msg_vec = duckdb_data_chunk_get_vector(output, 2);

  const char *status = bind_data->success ? "ok" : "error";
  const char *message = bind_data->success ? "Query captured (pending flush)"
                                           : "Capture buffer full";

  duckdb_vector_assign_string_element_len(status_vec, 0, status,
                                          strlen(status));
  ((int64_t *)duckdb_vector_get_data(hash_vec))[0] = bind_data->query_hash;
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

  // ---- Register vizier_capture_query(sql) table function ----
  {
    duckdb_table_function func = duckdb_create_table_function();
    duckdb_table_function_set_name(func, "vizier_capture_query");
    duckdb_logical_type varchar_type =
        duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(func, varchar_type);
    duckdb_destroy_logical_type(&varchar_type);
    duckdb_table_function_set_bind(func, capture_query_bind);
    duckdb_table_function_set_init(func, capture_query_init);
    duckdb_table_function_set_function(func, capture_query_execute);
    if (duckdb_register_table_function(conn, func) == DuckDBError) {
      access->set_error(info,
                        "Vizier: failed to register vizier_capture_query");
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

  return true;
}
