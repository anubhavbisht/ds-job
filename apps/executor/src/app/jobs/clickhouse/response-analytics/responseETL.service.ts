import { Injectable, Logger } from '@nestjs/common';
import { ClickhouseService, runWithRetry } from '@jobber/clickhouse';
import {
  BATCH_MS,
  CLICKHOUSE_SETTINGS_SQL,
  CONCURRENCY,
  ETL_METADATA_TABLE,
  OVERLAP_MS,
} from './etl.constants';
import {
  createAnalyticsTableSQL,
  doesTableExist,
  ensureColumnsAdded,
  formatISO,
  getCampaignName,
  getDynamicKeys,
  getLastRunTime,
  getTableColumnsOrdered,
  insertLastRunTime,
  qid,
  sleep,
} from './etl.utils';

@Injectable()
export class ResponseAnalyticsEtlService {
  private readonly logger = new Logger(ResponseAnalyticsEtlService.name);

  constructor(private readonly clickhouse: ClickhouseService) {}

  async run(campaignId: string): Promise<void> {
    const campaignName = await getCampaignName(
      this.clickhouse,
      campaignId,
      this.logger
    );
    const ANALYTICS_TABLE = `responseAnalytics_${campaignName}`;
    const startTime = Date.now();

    this.logger.log(
      `[ETL] 🚀 Starting response_analytics ETL for campaign ${campaignId}`
    );

    // ✅ Preflight health check
    await this.safeHealthCheck();

    // ✅ Apply ClickHouse session settings (with retry)
    await runWithRetry(
      () => this.clickhouse.command(CLICKHOUSE_SETTINGS_SQL),
      'apply-settings',
      this.logger
    );

    // ✅ Ensure analytics table exists
    const tableExists = await doesTableExist(
      this.clickhouse,
      ANALYTICS_TABLE,
      this.logger
    );
    if (!tableExists) {
      this.logger.log(`[ETL] Creating missing table ${ANALYTICS_TABLE}`);
      const sql = createAnalyticsTableSQL(ANALYTICS_TABLE);
      await runWithRetry(
        () => this.clickhouse.command(sql),
        'create-table',
        this.logger
      );
    }

    // ✅ Load dynamic schema keys
    const { questionKeys, participantKeys } = await getDynamicKeys(
      this.clickhouse,
      campaignId,
      this.logger
    );
    await ensureColumnsAdded(
      this.clickhouse,
      ANALYTICS_TABLE,
      questionKeys,
      participantKeys,
      this.logger
    );

    // ✅ Retrieve existing columns + build expressions
    const tableCols = await getTableColumnsOrdered(
      this.clickhouse,
      'zykrr_production',
      ANALYTICS_TABLE,
      this.logger
    );
    const exprMap = this.buildSelectExprMap(questionKeys, participantKeys);
    const { ORDERED_COLS, SELECTS } = this.buildOrderedColumnsAndSelects(
      tableCols,
      exprMap
    );

    // ✅ Determine mode (first load vs incremental)
    const lastRun = await getLastRunTime(
      this.clickhouse,
      ETL_METADATA_TABLE,
      ANALYTICS_TABLE,
      this.logger
    );

    if (!lastRun)
      await this.runFirstLoad(
        campaignId,
        ANALYTICS_TABLE,
        ORDERED_COLS,
        SELECTS
      );
    else
      await this.runIncremental(
        campaignId,
        ANALYTICS_TABLE,
        ORDERED_COLS,
        SELECTS,
        lastRun
      );

    this.logger.log(
      `[ETL] ✅ Completed ETL for campaign ${campaignId} in ${(
        (Date.now() - startTime) /
        1000
      ).toFixed(2)}s`
    );
  }

  // ---------------------------------------------------------------------------
  // 🔹 Health Check Wrapper
  private async safeHealthCheck() {
    try {
      await this.clickhouse.healthCheck();
      this.logger.log('[ETL] ✅ ClickHouse health check passed');
    } catch (err) {
      this.logger.error('[ETL] ❌ ClickHouse health check failed', err);
      throw err;
    }
  }

  // ---------------------------------------------------------------------------
  // 🔹 First Load ETL
  private async runFirstLoad(
    campaignId: string,
    ANALYTICS_TABLE: string,
    ORDERED_COLS: string[],
    SELECTS: string[]
  ) {
    this.logger.log('[ETL] Performing first load...');
    await this.safeHealthCheck();

    const rangeRes = await runWithRetry(
      () =>
        this.clickhouse.query<{ min_ts: string; max_ts: string }>(
          `
          SELECT min(p.createdAt) AS min_ts, max(p.createdAt) AS max_ts
          FROM Participants p
          INNER JOIN Campaigns c ON c.organizationId = p.organizationId
          WHERE c.id='${campaignId}'
          `,
          'JSON'
        ),
      'get-first-load-range',
      this.logger
    );

    const minTs = rangeRes[0]?.min_ts;
    const maxTs = rangeRes[0]?.max_ts;

    if (!minTs || !maxTs) {
      this.logger.warn('[ETL] No data found for first load.');
      return;
    }

    let start = new Date(minTs).getTime();
    const end = new Date(maxTs).getTime();
    const batches: { sql: string; batchEndISO: string }[] = [];

    while (start < end) {
      const next = Math.min(start + BATCH_MS, end);
      const sISO = formatISO(new Date(start));
      const eISO = formatISO(new Date(next));
      const selectBody = this.buildFirstLoadSelectBody(campaignId, sISO, eISO);
      const sql = this.wrapInsertWithColumns(
        ANALYTICS_TABLE,
        ORDERED_COLS,
        SELECTS,
        selectBody
      );
      batches.push({ sql, batchEndISO: eISO });
      start = next;
    }

    for (let i = 0; i < batches.length; i += CONCURRENCY) {
      const chunk = batches.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (b) => {
          const t0 = Date.now();
          try {
            await this.safeHealthCheck();
            this.logger.log(
              `[ETL] 🟡 Running first-load batch up to ${b.batchEndISO}`
            );
            await runWithRetry(
              () => this.clickhouse.command(b.sql),
              'batch-insert',
              this.logger
            );
            await insertLastRunTime(
              this.clickhouse,
              ETL_METADATA_TABLE,
              ANALYTICS_TABLE,
              b.batchEndISO,
              this.logger
            );
            this.logger.log(
              `[ETL] ✅ Batch ${b.batchEndISO} done in ${
                (Date.now() - t0) / 1000
              }s`
            );
          } catch (err) {
            this.logger.error(
              `[ETL] ❌ Batch ${b.batchEndISO} failed: ${err.message}`
            );
          } finally {
            await sleep(200); // smooth out heavy loads
          }
        })
      );
    }
  }

  // ---------------------------------------------------------------------------
  // 🔹 Incremental ETL
  private async runIncremental(
    campaignId: string,
    ANALYTICS_TABLE: string,
    ORDERED_COLS: string[],
    SELECTS: string[],
    lastRun: string
  ) {
    this.logger.log('[ETL] Performing incremental load...');
    await this.safeHealthCheck();

    const from = formatISO(new Date(new Date(lastRun).getTime() - OVERLAP_MS));
    const to = formatISO(new Date());
    const incBody = this.buildIncrementalSelectBody(campaignId, from, to);
    const incSql = this.wrapInsertWithColumns(
      ANALYTICS_TABLE,
      ORDERED_COLS,
      SELECTS,
      incBody
    );

    const start = Date.now();
    await runWithRetry(
      () => this.clickhouse.command(incSql),
      'incremental-load',
      this.logger
    );
    await insertLastRunTime(
      this.clickhouse,
      ETL_METADATA_TABLE,
      ANALYTICS_TABLE,
      to,
      this.logger
    );
    this.logger.log(
      `[ETL] ✅ Incremental load completed in ${(Date.now() - start) / 1000}s`
    );
  }

  // ---------------------------------------------------------------------------
  // 🔹 Expression Builders (unchanged)
  private buildSelectExprMap(qks: any[], pks: string[]) {
    const map: Record<string, string> = {
      participantListId: 'plm.participantListId',
      participantListMemberId: 'plm.participantListMemberId',
      participantId: 'plm.participantId',
      responseId: 'toString(rj.responseId) AS responseId',
      _peerdb_synced_at: `greatest(
        ifNull(plm.plm_synced_at, toDateTime('1970-01-01 00:00:00')),
        ifNull(pl.list_synced_at, toDateTime('1970-01-01 00:00:00')),
        ifNull(p.participant_synced_at, toDateTime('1970-01-01 00:00:00')),
        ifNull(rj.response_synced_at, toDateTime('1970-01-01 00:00:00'))
      ) AS _peerdb_synced_at`,
    };

    for (const q of qks) {
      if (q.isMultiSelect) {
        map[q.id] = `
coalesce(
  JSONExtract(rj.answers, '${q.id}', 'Array(String)'),
  JSONExtract(plm.prefilledData, '${q.id}', 'Array(String)')
) AS ${qid(q.id)}`.trim();
      } else {
        map[q.id] = `
coalesce(
  arrayElement(JSONExtract(rj.answers, '${q.id}', 'Array(String)'), 1),
  arrayElement(JSONExtract(plm.prefilledData, '${q.id}', 'Array(String)'), 1),
  JSONExtractString(rj.answers, '${q.id}'),
  JSONExtractString(plm.prefilledData, '${q.id}')
) AS ${qid(q.id)}`.trim();
      }
    }

    for (const k of pks)
      map[k] = `JSONExtractString(p.fields, '${k}') AS ${qid(k)}`;
    return map;
  }

  private buildOrderedColumnsAndSelects(
    allCols: string[],
    exprMap: Record<string, string>
  ) {
    const BASE = [
      'participantListId',
      'participantListMemberId',
      'participantId',
      'responseId',
    ];
    const DYN = allCols.filter(
      (c) => !BASE.includes(c) && c !== '_peerdb_synced_at'
    );
    const ORDERED_COLS = [...BASE, ...DYN, '_peerdb_synced_at'];
    const SELECTS = ORDERED_COLS.map((c) => exprMap[c] || `NULL AS ${qid(c)}`);
    return { ORDERED_COLS, SELECTS };
  }

  private buildFirstLoadSelectBody(
    campaignId: string,
    startISO: string,
    endISO: string
  ) {
    return `
WITH campaign_participants AS (
  SELECT p.id AS participantId, p.fields, p.createdAt, p._peerdb_synced_at AS participant_synced_at
  FROM Participants p
  INNER JOIN Campaigns c ON c.organizationId = p.organizationId
  WHERE c.id='${campaignId}'
    AND p.createdAt >= parseDateTimeBestEffort('${startISO}')
    AND p.createdAt <  parseDateTimeBestEffort('${endISO}')
),
participant_members AS (
  SELECT id AS participantListMemberId, participantListId, participantId, prefilledData, _peerdb_synced_at AS plm_synced_at
  FROM ParticipantListMembers
  WHERE participantId IN (SELECT participantId FROM campaign_participants)
),
participant_lists AS (
  SELECT id AS participantListId, listName, _peerdb_synced_at AS list_synced_at
  FROM ParticipantLists
  WHERE campaignId='${campaignId}'
),
response_join AS (
  SELECT id AS responseId, participantListMemberId, answers, participantId, _peerdb_synced_at AS response_synced_at
  FROM Responses
)
SELECT
  /* SELECT_LIST_PLACEHOLDER */
FROM participant_members plm
INNER JOIN participant_lists pl ON pl.participantListId = plm.participantListId
INNER JOIN campaign_participants p ON p.participantId = plm.participantId
LEFT JOIN response_join rj ON rj.participantListMemberId = plm.participantListMemberId
`.trim();
  }

  private buildIncrementalSelectBody(
    campaignId: string,
    startISO: string,
    endISO: string
  ) {
    return `
WITH changed_participants AS (
  SELECT DISTINCT p.id AS participantId
  FROM Participants p
  INNER JOIN Campaigns c ON c.organizationId = p.organizationId
  WHERE c.id='${campaignId}'
    AND p._peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
),
changed_plms AS (
  SELECT DISTINCT plm.participantId, plm.participantListId
  FROM ParticipantListMembers plm
  INNER JOIN ParticipantLists pl ON pl.id = plm.participantListId
  WHERE plm._peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
    AND pl.campaignId='${campaignId}'
),
changed_lists AS (
  SELECT DISTINCT id AS participantListId
  FROM ParticipantLists
  WHERE campaignId='${campaignId}'
    AND _peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
),
changed_responses AS (
  SELECT DISTINCT r.participantId
  FROM Responses r
  INNER JOIN Campaigns c ON c.id = r.campaignId
  WHERE c.id='${campaignId}'
    AND r._peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
),
changed_scope AS (
  SELECT participantId FROM changed_participants
  UNION ALL SELECT participantId FROM changed_plms
  UNION ALL SELECT participantId FROM changed_responses
  UNION ALL
  SELECT participantId
  FROM ParticipantListMembers
  WHERE participantListId IN (SELECT participantListId FROM changed_lists)
),
campaign_participants AS (
  SELECT p.id AS participantId, p.fields, p._peerdb_synced_at AS participant_synced_at
  FROM Participants p
  WHERE p.id IN (SELECT participantId FROM changed_scope)
),
participant_members AS (
  SELECT id AS participantListMemberId, participantListId, participantId, prefilledData, _peerdb_synced_at AS plm_synced_at
  FROM ParticipantListMembers
  WHERE participantId IN (SELECT participantId FROM changed_scope)
),
participant_lists AS (
  SELECT id AS participantListId, listName, _peerdb_synced_at AS list_synced_at
  FROM ParticipantLists
  WHERE participantListId IN (SELECT participantListId FROM participant_members)
),
response_join AS (
  SELECT id AS responseId, participantListMemberId, answers, participantId, _peerdb_synced_at AS response_synced_at
  FROM Responses
  WHERE participantListMemberId IN (SELECT participantListMemberId FROM participant_members)
)
SELECT
  /* SELECT_LIST_PLACEHOLDER */
FROM participant_members plm
INNER JOIN participant_lists pl ON pl.participantListId = plm.participantListId
INNER JOIN campaign_participants p ON p.participantId = plm.participantId
LEFT JOIN response_join rj ON rj.participantListMemberId = plm.participantListMemberId
`.trim();
  }

  private wrapInsertWithColumns(
    ANALYTICS_TABLE,
    cols: string[],
    selects: string[],
    selectBody: string
  ) {
    const colList = cols.map(qid).join(', ');
    const body = selectBody.replace(
      '/* SELECT_LIST_PLACEHOLDER */',
      selects.join(',\n    ')
    );
    return `
    INSERT INTO ${ANALYTICS_TABLE} (${colList})
    ${body}
    `.trim();
  }
}
