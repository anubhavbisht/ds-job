export const ETL_METADATA_TABLE = 'etl_metadata';

export const BATCH_MS = 1000 * 60 * 60 * 24 * 10;
export const OVERLAP_MS = 45 * 60 * 1000;
export const CONCURRENCY = 2;

export const CLICKHOUSE_SETTINGS_SQL = `
  SET
    max_threads = 8,
    max_memory_usage = 42000000000,
    max_bytes_before_external_group_by = 4000000000,
    max_bytes_before_external_sort = 4000000000,
    distributed_aggregation_memory_efficient = 1,
    join_algorithm = 'parallel_hash, grace_hash, partial_merge, hash',
    join_use_nulls = 1,
    max_execution_time = 3600,
    max_insert_block_size = 1000000,
    send_timeout = 600,
    receive_timeout = 600,
    connect_timeout = 60,
    async_insert = 1,
    wait_for_async_insert = 0;
`;
