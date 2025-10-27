import { Logger } from '@nestjs/common';
import { ClickhouseService } from '@jobber/clickhouse';
import { runWithRetry } from '@jobber/clickhouse';

export function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export function formatISO(dt: Date) {
  return dt.toISOString().slice(0, 19).replace('T', ' ');
}

export function qid(id: string) {
  return `"${id.replace(/"/g, '""')}"`;
}

export async function doesTableExist(
  ch: ClickhouseService,
  name: string,
  logger: Logger
): Promise<boolean> {
  const sql = `SELECT count() AS c FROM system.tables WHERE database=currentDatabase() AND name='${name}'`;

  const res = await runWithRetry(
    () => ch.query<{ c: number }>(sql, 'JSON'),
    'doesTableExist',
    logger
  );

  if (typeof res === 'string') {
    logger.error(`[ClickHouse] Unexpected CSV result for doesTableExist`);
    throw new Error('Expected JSON result but got CSV');
  }

  return res[0]?.c > 0;
}

export async function getLastRunTime(
  ch: ClickhouseService,
  metadataTable: string,
  analyticsTable: string,
  logger: Logger
): Promise<string | null> {
  const sql = `
    SELECT last_run
    FROM ${metadataTable}
    WHERE precalculateTableName='${analyticsTable}'
    ORDER BY last_run DESC
    LIMIT 1
  `;

  const res = await runWithRetry(
    () => ch.query<{ last_run: string }>(sql, 'JSON'),
    'getLastRunTime',
    logger
  );

  if (typeof res === 'string') {
    logger.error(`[ClickHouse] Unexpected CSV result for getLastRunTime`);
    throw new Error('Expected JSON result but got CSV');
  }

  return res[0]?.last_run ?? null;
}

export async function insertLastRunTime(
  ch: ClickhouseService,
  metadataTable: string,
  analyticsTable: string,
  ts: string,
  logger: Logger
) {
  const sql = `
    INSERT INTO ${metadataTable}(precalculateTableName,last_run)
    VALUES('${analyticsTable}', parseDateTimeBestEffort('${ts}'))
  `;
  await runWithRetry(() => ch.command(sql), 'insertLastRunTime', logger);
}

export async function getDynamicKeys(
  ch: ClickhouseService,
  campaignId: string,
  logger: Logger
) {
  const qSql = `
    SELECT questionId, questionType AS type, isMultiSelect
    FROM CampaignQuestionnairetable
    WHERE campaignId='${campaignId}'
  `;

  const fSql = `
    SELECT fieldName
    FROM ParticipantsFields_temp
    WHERE organizationId IN (
      SELECT organizationId FROM Campaigns WHERE id='${campaignId}')
  `;

  const [questions, fields] = await Promise.all([
    runWithRetry(
      () =>
        ch.query<{ questionId: string; type: string; isMultiSelect: string }>(
          qSql,
          'JSON'
        ),
      'fetch questions',
      logger
    ),
    runWithRetry(
      () => ch.query<{ fieldName: string }>(fSql, 'JSON'),
      'fetch fields',
      logger
    ),
  ]);

  const questionKeys = questions.map((q) => ({
    id: q.questionId.toString(),
    type: q.type,
    isMultiSelect: ['true', 't', '1', 'yes'].includes(
      (q.isMultiSelect ?? '').toString().toLowerCase()
    ),
  }));

  const participantKeys = fields.map((f) => f.fieldName).filter(Boolean);

  logger.log(
    `[ETL] Loaded ${questionKeys.length} questions, ${participantKeys.length} fields`
  );

  return { questionKeys, participantKeys };
}

export async function ensureColumnsAdded(
  ch: ClickhouseService,
  table: string,
  qks: any[],
  pks: string[],
  logger: Logger
) {
  const alters: string[] = [];
  for (const q of qks) {
    const t = q.isMultiSelect ? 'Array(String)' : 'String';
    alters.push(
      `ADD COLUMN IF NOT EXISTS ${qid(q.id)} ${t}`,
      `MODIFY COLUMN IF EXISTS ${qid(q.id)} ${t}`
    );
  }
  for (const k of pks) alters.push(`ADD COLUMN IF NOT EXISTS ${qid(k)} String`);
  if (!alters.length) return;
  const sql = `ALTER TABLE ${table} ${alters.join(',\n')}`;
  logger.log(`[ETL] Ensuring ${alters.length} dynamic columns…`);
  await runWithRetry(() => ch.command(sql), 'ensureColumnsAdded', logger);
}

export async function getTableColumnsOrdered(
  ch: ClickhouseService,
  db: string,
  table: string,
  logger: Logger
): Promise<string[]> {
  const sql = `
    SELECT name
    FROM system.columns
    WHERE database='${db}' AND table='${table}'
    ORDER BY position
  `;

  const res = await runWithRetry(
    () => ch.query<{ name: string }>(sql, 'JSON'),
    'getTableColumnsOrdered',
    logger
  );

  if (typeof res === 'string') {
    logger.error(`[ETL] Unexpected CSV format for getTableColumnsOrdered`);
    throw new Error('Expected JSON result but got CSV');
  }

  return res.map((r) => r.name);
}

export function createResponseAnalyticsTableSQL(
  tableName: string,
  options?: {
    engine?: string;
    partitionBy?: string;
    orderBy?: string;
  }
): string {
  const engine = options?.engine ?? 'ReplacingMergeTree(_peerdb_synced_at)';
  const partitionBy = options?.partitionBy ?? 'toYYYYMM(_peerdb_synced_at)';
  const orderBy = options?.orderBy ?? '(participantListMemberId, responseId)';

  return `
CREATE TABLE IF NOT EXISTS ${tableName}
(
  participantListId UUID,
  participantListMemberId UUID,
  participantId UUID,
  responseId String,
  surveyCreationDate DateTime,     
  responseFilledDate DateTime,     
  _peerdb_synced_at DateTime
)
ENGINE = ${engine}
PARTITION BY ${partitionBy}
ORDER BY ${orderBy};
`.trim();
}

export function createScheduleAnalyticsTableSQL(
  tableName: string,
  options?: {
    engine?: string;
    partitionBy?: string;
    orderBy?: string;
  }
): string {
  const engine = options?.engine ?? 'ReplacingMergeTree(_peerdb_synced_at)';
  const partitionBy = options?.partitionBy ?? 'toYYYYMM(_peerdb_synced_at)';
  const orderBy = options?.orderBy ?? '(participantListMemberId, scheduleId)';

  return `
    CREATE TABLE IF NOT EXISTS ${tableName}
  (
    participantListMemberId UUID,
    participantListId UUID,
    participantId UUID,
    createdAt DateTime,
    token String,
    visited UInt8,
    scheduleId UUID,
    scheduleDateAndTime DateTime,
    sentStatus LowCardinality(String),
    sendTo LowCardinality(String),
    provider LowCardinality(String),
    isAutoReminder UInt8,
    distributionTemplateId String,
    listName LowCardinality(String),
    distributionId UUID,
    success UInt8,
    triggeredAt DateTime,
    providerId LowCardinality(String),
    templateName LowCardinality(String),
    distributionChannel LowCardinality(String),
    _peerdb_synced_at DateTime
  )
  ENGINE = ${engine}
  PARTITION BY ${partitionBy}
  ORDER BY ${orderBy};
  `.trim();
}

export async function getTableName(
  ch: ClickhouseService,
  campaignId: string,
  logger: Logger
): Promise<string> {
  const sql = `
    SELECT 
      Campaigns.name AS campaignName, 
      Organizations.name AS orgName
    FROM Campaigns
    INNER JOIN Organizations 
      ON Organizations.id = Campaigns.organizationId
    WHERE Campaigns.id = '${campaignId}'
    LIMIT 1
  `;

  const res = await runWithRetry(
    () => ch.query<{ campaignName: string; orgName: string }>(sql, 'JSON'),
    'getTableName',
    logger
  );

  if (!res || res.length === 0) {
    throw new Error(`[ETL] No campaign found for ID ${campaignId}`);
  }

  const cName = sanitizeTableIdentifier(res[0].campaignName);
  const oName = sanitizeTableIdentifier(res[0].orgName);

  logger.log(`[ETL] Using campaign name "${cName}" for analytics table`);
  return `${oName}_${cName}`;
}

export function sanitizeTableIdentifier(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '');
}
