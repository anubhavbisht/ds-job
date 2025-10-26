export const ETL_METADATA_TABLE = 'etl_metadata';

export const BATCH_MS = 1000 * 60 * 60 * 24 * 5;
export const OVERLAP_MS = 45 * 60 * 1000;
export const CONCURRENCY = 1;

export const CLICKHOUSE_SETTINGS_SQL = `
    SET
  /* THREADING + PARALLELISM */
  max_threads = 8,
  max_insert_threads = 8,
  max_parallel_replicas = 3,
  
  /* MEMORY + SPILLING */
  max_memory_usage = 60000000000,  -- 60 GiB soft cap per query
  max_memory_usage_for_user = 0,   -- no user-wide cap
  max_bytes_before_external_group_by = 8000000000,
  max_bytes_before_external_sort = 8000000000,
  temporary_files_codec = 'LZ4',

  /* EXECUTION SAFETY */
  max_execution_time = 3600,
  timeout_before_checking_execution_speed = 60,
  max_concurrent_queries_for_user = 0,

  /* JOIN OPTIMIZATION */
  join_algorithm = 'parallel_hash, grace_hash, partial_merge, hash',
  join_use_nulls = 1,
  distributed_aggregation_memory_efficient = 1,

  /* INSERT + IO BEHAVIOR */
  max_insert_block_size = 500000,
  async_insert = 1,
  wait_for_async_insert = 0,
  insert_distributed_sync = 1,

  /* NETWORK TIMEOUTS */
  send_timeout = 600,
  receive_timeout = 600,
  connect_timeout = 60,

  /* LOGGING + DIAGNOSTICS */
  log_queries = 1,
  log_profile_events = 1;

`;
