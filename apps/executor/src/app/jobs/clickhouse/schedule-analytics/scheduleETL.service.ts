import { Injectable, Logger } from '@nestjs/common';
import { ClickhouseService, runWithRetry } from '@jobber/clickhouse';
import {
  BATCH_MS,
  CLICKHOUSE_SETTINGS_SQL,
  CONCURRENCY,
  ETL_METADATA_TABLE,
  OVERLAP_MS,
} from '../etl.constants';
import {
  createScheduleAnalyticsTableSQL,
  doesTableExist,
  formatISO,
  getLastRunTime,
  getTableName,
  insertLastRunTime,
  sleep,
} from '../etl.utils';

@Injectable()
export class ScheduleAnalyticsEtlService {
  private readonly logger = new Logger(ScheduleAnalyticsEtlService.name);

  constructor(private readonly clickhouse: ClickhouseService) {}

  async run(campaignId: string): Promise<void> {
    const analyticsTableName = await getTableName(
      this.clickhouse,
      campaignId,
      this.logger
    );
    const ANALYTICS_TABLE = `scheduleAnalytics_${analyticsTableName}`;
    const startTime = Date.now();

    this.logger.log(
      `[ETL] 🚀 Starting schedule_analytics ETL for campaign ${campaignId}`
    );

    await this.safeHealthCheck();

    await runWithRetry(
      () => this.clickhouse.command(CLICKHOUSE_SETTINGS_SQL),
      'apply-settings',
      this.logger
    );

    const tableExists = await doesTableExist(
      this.clickhouse,
      ANALYTICS_TABLE,
      this.logger
    );
    if (!tableExists) {
      this.logger.log(`[ETL] Creating missing table ${ANALYTICS_TABLE}`);

      const sql = createScheduleAnalyticsTableSQL(ANALYTICS_TABLE);

      await runWithRetry(
        () => this.clickhouse.command(sql),
        'create-table',
        this.logger
      );
    }

    const lastRun = await getLastRunTime(
      this.clickhouse,
      ETL_METADATA_TABLE,
      ANALYTICS_TABLE,
      this.logger
    );

    if (!lastRun) await this.runFirstLoad(campaignId, ANALYTICS_TABLE);
    else await this.runIncremental(campaignId, ANALYTICS_TABLE, lastRun);

    this.logger.log(
      `[ETL] ✅ Completed Schedule ETL for campaign ${campaignId} in ${(
        (Date.now() - startTime) /
        1000
      ).toFixed(2)}s`
    );
  }

  private async safeHealthCheck() {
    try {
      await this.clickhouse.healthCheck();
      this.logger.log('[ETL] ✅ ClickHouse health check passed');
    } catch (err) {
      this.logger.error('[ETL] ❌ ClickHouse health check failed', err);
      throw err;
    }
  }

  private async runFirstLoad(campaignId: string, ANALYTICS_TABLE: string) {
    this.logger.log('[ETL] Performing first load...');
    await this.safeHealthCheck();

    const rangeRes = await runWithRetry(
      () =>
        this.clickhouse.query<{ min_ts: string; max_ts: string }>(
          `SELECT min(createdAt) AS min_ts, max(createdAt) AS max_ts FROM ParticipantLists WHERE campaignId='${campaignId}'`,
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
      const sql = this.wrapInsertWithColumns(ANALYTICS_TABLE, selectBody);
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
            await sleep(200);
          }
        })
      );
    }
  }

  private async runIncremental(
    campaignId: string,
    ANALYTICS_TABLE: string,
    lastRun: string
  ) {
    this.logger.log('[ETL] Performing incremental load...');
    await this.safeHealthCheck();

    const from = formatISO(new Date(new Date(lastRun).getTime() - OVERLAP_MS));
    const to = formatISO(new Date());
    const incBody = this.buildIncrementalSelectBody(campaignId, from, to);
    const incSql = this.wrapInsertWithColumns(ANALYTICS_TABLE, incBody);

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

  private buildFirstLoadSelectBody(
    campaignId: string,
    startISO: string,
    endISO: string
  ) {
    return `
        WITH
        campaign_lists AS (
          SELECT id, listName, createdAt, _peerdb_synced_at AS list_synced_at
          FROM ParticipantLists
          WHERE campaignId = '${campaignId}'
            AND createdAt >= parseDateTimeBestEffort('${startISO}')
            AND createdAt <  parseDateTimeBestEffort('${endISO}')
        ),
        campaign_members AS (
          SELECT id AS participantListMemberId, participantId, participantListId,
                 token, visited, createdAt, _peerdb_synced_at AS member_synced_at
          FROM ParticipantListMembers
          WHERE participantListId IN (SELECT id FROM campaign_lists)
        ),
        campaign_schedules AS (
          SELECT id AS scheduleId, participantListId, scheduleDateAndTime, sentStatus,
                 sendTo, provider, isAutoReminder,
                 arrayJoin(arrayConcat([toString(templateId)],
                 splitByString(',', assumeNotNull(additionalTemplates)))) AS distributionTemplateId,
                 _peerdb_synced_at AS schedule_synced_at
          FROM Schedules
          WHERE campaignId = '${campaignId}'
            AND participantListId IN (SELECT id FROM campaign_lists)
            AND distributionTemplateId != ''
        ),
        schedule_plm AS (
          SELECT sd.scheduleId, sd.participantListId, plm.participantListMemberId, sd.distributionTemplateId
          FROM campaign_schedules AS sd
          INNER JOIN campaign_members AS plm ON sd.participantListId = plm.participantListId
        ),
        filtered_logs AS (
          SELECT
              dl.scheduleId,
              dl.participantListMemberId,
              ifNull(toString(dl.templateId), '') AS templateId,
              argMax(dl.id, dl.triggeredAt) AS distributionId,
              argMax(dl.success, dl.triggeredAt) AS success,
              argMax(dl.triggeredAt, dl._peerdb_synced_at) AS triggeredAt,
              argMax(dl._peerdb_synced_at, dl.triggeredAt) AS log_synced_at
          FROM DistributionLogs AS dl
          INNER JOIN schedule_plm AS sp
            ON dl.scheduleId = sp.scheduleId
           AND dl.participantListMemberId = sp.participantListMemberId
           AND ifNull(toString(dl.templateId), '') = ifNull(sp.distributionTemplateId, '')
          GROUP BY dl.scheduleId, dl.participantListMemberId, templateId
        ),
        campaign_templates AS (
          SELECT toString(id) AS id_str, provider, providerId, templateName,
                 distributionChannel, _peerdb_synced_at AS template_synced_at
          FROM DistributionTemplates
          WHERE campaignId = '${campaignId}'
            AND toString(id) IN (SELECT DISTINCT distributionTemplateId FROM campaign_schedules)
        )
      SELECT
        plm.participantListMemberId,
        plm.participantListId,
        plm.participantId,
        plm.createdAt,
        plm.token,
        plm.visited,
        sd.scheduleId,
        sd.scheduleDateAndTime,
        sd.sentStatus,
        sd.sendTo,
        dt.provider,
        sd.isAutoReminder,
        sd.distributionTemplateId,
        pl.listName,
        dl.distributionId,
        dl.success,
        ifNull(dl.triggeredAt, sd.scheduleDateAndTime) AS triggeredAt,
        dt.providerId,
        dt.templateName,
        dt.distributionChannel,
        greatest(
          ifNull(plm.member_synced_at,  toDateTime('1970-01-01 00:00:00')),
          ifNull(pl.list_synced_at,     toDateTime('1970-01-01 00:00:00')),
          ifNull(sd.schedule_synced_at, toDateTime('1970-01-01 00:00:00')),
          ifNull(dl.log_synced_at,      toDateTime('1970-01-01 00:00:00')),
          ifNull(dt.template_synced_at, toDateTime('1970-01-01 00:00:00'))
        ) AS _peerdb_synced_at
      FROM campaign_members AS plm
      INNER JOIN campaign_lists AS pl ON pl.id = plm.participantListId
      LEFT JOIN campaign_schedules AS sd ON pl.id = sd.participantListId
      LEFT JOIN filtered_logs AS dl ON (sd.scheduleId, plm.participantListMemberId, sd.distributionTemplateId)
                                    = (dl.scheduleId, dl.participantListMemberId, dl.templateId)
      LEFT JOIN campaign_templates AS dt ON sd.distributionTemplateId = dt.id_str;
`.trim();
  }

  private buildIncrementalSelectBody(
    campaignId: string,
    startISO: string,
    endISO: string
  ) {
    return `
    WITH changed_participantLists AS (
        SELECT DISTINCT id AS participantListId FROM ParticipantLists
        WHERE campaignId = '${campaignId}'
          AND _peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
      ),
      changed_participantListMembers AS (
        SELECT DISTINCT participantListId FROM ParticipantListMembers
        WHERE _peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
      ),
      changed_schedules AS (
        SELECT DISTINCT participantListId, id AS scheduleId FROM Schedules
        WHERE campaignId = '${campaignId}'
          AND _peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
      ),
      changed_logs AS (
        SELECT DISTINCT scheduleId, participantListMemberId FROM DistributionLogs
        INNER JOIN Schedules ON Schedules.id = DistributionLogs.scheduleId
        WHERE campaignId = '${campaignId}'
          AND _peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
      ),
      changed_scope AS (
        SELECT DISTINCT participantListId FROM changed_participantLists
        UNION ALL SELECT DISTINCT participantListId FROM changed_participantListMembers
        UNION ALL SELECT DISTINCT participantListId FROM changed_schedules
        UNION ALL SELECT DISTINCT s.participantListId FROM Schedules AS s
          INNER JOIN changed_logs AS cl ON s.id = cl.scheduleId
      ),
      campaign_lists AS (
        SELECT id, listName, _peerdb_synced_at FROM ParticipantLists
        WHERE campaignId = '${campaignId}' AND id IN (SELECT participantListId FROM changed_scope)
      ),
      campaign_members AS (
        SELECT id, participantId, participantListId, token, visited, createdAt, _peerdb_synced_at
        FROM ParticipantListMembers WHERE participantListId IN (SELECT id FROM campaign_lists)
      ),
      campaign_schedules AS (
        SELECT id, participantListId, scheduleDateAndTime, sentStatus, sendTo, provider, isAutoReminder,
               arrayJoin(arrayConcat([toString(templateId)], splitByString(',', assumeNotNull(additionalTemplates))))
               AS distributionTemplateId, _peerdb_synced_at
        FROM Schedules
        WHERE campaignId = '${campaignId}' AND participantListId IN (SELECT id FROM campaign_lists)
          AND distributionTemplateId != ''
      ),
      filtered_logs AS (
        SELECT
            dl.scheduleId,
            dl.participantListMemberId,
            ifNull(toString(dl.templateId), '') AS templateId,
            argMax(dl.id, dl.triggeredAt) AS distributionId,
            argMax(dl.success, dl.triggeredAt) AS success,
            argMax(dl.triggeredAt, dl._peerdb_synced_at) AS triggeredAt,
            argMax(dl._peerdb_synced_at, dl.triggeredAt) AS log_synced_at
        FROM DistributionLogs AS dl
        WHERE scheduleId IN (SELECT id FROM campaign_schedules)
           OR participantListMemberId IN (SELECT id FROM campaign_members)
        GROUP BY dl.scheduleId, dl.participantListMemberId, templateId
      ),
      last_ts AS (
        SELECT scheduleId, participantListMemberId, max(triggeredAt) AS triggeredAt
        FROM filtered_logs GROUP BY scheduleId, participantListMemberId
      ),
      last_logs AS (
        SELECT fl.scheduleId, fl.participantListMemberId, fl.success, fl.triggeredAt,
               fl.log_synced_at AS _peerdb_synced_at, fl.distributionId
        FROM filtered_logs fl
        INNER JOIN last_ts t
          ON fl.scheduleId = t.scheduleId AND fl.participantListMemberId = t.participantListMemberId
          AND fl.triggeredAt = t.triggeredAt
      ),
      campaign_templates AS (
        SELECT toString(id) AS id_str, provider, providerId, templateName, distributionChannel, _peerdb_synced_at
        FROM DistributionTemplates
        WHERE campaignId = '${campaignId}'
          AND (_peerdb_synced_at BETWEEN parseDateTimeBestEffort('${startISO}') AND parseDateTimeBestEffort('${endISO}')
            OR toString(id) IN (SELECT DISTINCT distributionTemplateId FROM campaign_schedules))
      )
      SELECT plm.id AS participantListMemberId, plm.participantListId, plm.participantId, plm.createdAt, plm.token, plm.visited,
             sd.id AS scheduleId, sd.scheduleDateAndTime, sd.sentStatus, sd.sendTo, dt.provider, sd.isAutoReminder,
             sd.distributionTemplateId, pl.listName, dl.distributionId, dl.success,
             ifNull(dl.triggeredAt, sd.scheduleDateAndTime) AS triggeredAt,
             dt.providerId, dt.templateName, dt.distributionChannel,
             greatest(ifNull(plm._peerdb_synced_at,toDateTime('1970-01-01 00:00:00')),
                      ifNull(pl._peerdb_synced_at,toDateTime('1970-01-01 00:00:00')),
                      ifNull(sd._peerdb_synced_at,toDateTime('1970-01-01 00:00:00')),
                      ifNull(dl._peerdb_synced_at,toDateTime('1970-01-01 00:00:00')),
                      ifNull(dt._peerdb_synced_at,toDateTime('1970-01-01 00:00:00'))) AS _peerdb_synced_at
      FROM campaign_members AS plm
      INNER JOIN campaign_lists AS pl ON pl.id = plm.participantListId
      LEFT JOIN campaign_schedules AS sd ON pl.id = sd.participantListId
      LEFT JOIN last_logs AS dl ON (sd.id, plm.id) = (dl.scheduleId, dl.participantListMemberId)
      LEFT JOIN campaign_templates AS dt ON sd.distributionTemplateId = dt.id_str;
    `.trim();
  }

  private wrapInsertWithColumns(ANALYTICS_TABLE: string, selectBody: string) {
    const clean = selectBody.trim().replace(/;+\s*$/, '');
    return `
    INSERT INTO ${ANALYTICS_TABLE}
    ${clean}
    `.trim();
  }
}
