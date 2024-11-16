import { JobsOptions, QueueBase, RedisClient } from 'bullmq';
import { RouterOptions } from './router-options';
import { v4 } from 'uuid';
import * as _debug from 'debug';

const debug = _debug('bullmq:router:consumer');

type XReadGroupResult = [string, [string, string[]][]];

export class Consumer<DataType = any> extends QueueBase {
  protected consumerOpts: RouterOptions;
  private lastTrim = 0;

  constructor(streamName: string, opts?: RouterOptions) {
    super(streamName, {
      blockingConnection: false,
      ...opts,
    });

    this.consumerOpts = opts || { connection: {} };

    this.waitUntilReady()
      .then((client) => {
        debug(`ready on ${client.options['port']}`);
      })
      .catch(() => {
        // We ignore this error to avoid warnings. The error can still
        // be received by listening to event 'error'
      });
  }

  consume(
    consumerGroup: string,
    cb: (data: DataType, opts: JobsOptions) => Promise<void>,
  ): void {
    this.waitUntilReady()
      .then(async () => {
        const streamName = this.name;
        const consumerName = v4();

        const client = await this.client;

        try {
          // Create the consumer group if it doesn't already exist
          await client.xgroup(
            'CREATE',
            streamName,
            consumerGroup,
            '0',
            'MKSTREAM',
          );
        } catch (error) {
          if (!(error as Error).message.includes('BUSYGROUP')) {
            // If the group already exists, ignore the error
            throw error;
          }
        }

        while (!this.closing) {
          try {
            // First, read pending messages (PEL)
            while (!this.closing) {
              debug('pending.read');
              const pendingResult = (await client.xreadgroup(
                'GROUP',
                consumerGroup,
                consumerName,
                'COUNT',
                this.consumerOpts.batchSize || 1,
                'STREAMS',
                streamName,
                '0', // Read all pending messages
              )) as XReadGroupResult[] | null;

              if (
                pendingResult &&
                pendingResult.length > 1 &&
                pendingResult[1].length > 0
              ) {
                const now = Date.now();
                const maxRetentionMs =
                  this.consumerOpts.maxRetentionMs || 1000 * 60 * 60 * 24;
                const [, entries] = pendingResult[0];
                for (const [id] of entries) {
                  const [timestamp] = id.split('-').map(Number);
                  if (now - timestamp <= maxRetentionMs) {
                    await this.processMessages(
                      client,
                      streamName,
                      consumerGroup,
                      pendingResult,
                      cb,
                    );
                  } else {
                    await client.xack(streamName, consumerGroup, id);
                  }
                }
                debug('pending.results');
              } else {
                debug('pending.none');
                break;
              }
            }

            await this.trimStream();

            // Then read new messages if no pending messages are left
            while (!this.closing) {
              debug('new.read');
              const newResult = (await client.xreadgroup(
                'GROUP',
                consumerGroup,
                consumerName,
                'COUNT',
                this.consumerOpts.batchSize || 1,
                'STREAMS',
                streamName,
                '>', // Read new messages
              )) as XReadGroupResult[] | null;

              if (
                newResult &&
                newResult.length > 0 &&
                newResult[0].length > 0
              ) {
                debug('new.results');
                await this.processMessages(
                  client,
                  streamName,
                  consumerGroup,
                  newResult,
                  cb,
                );
              } else {
                debug('new.none');
                break;
              }
            }

            debug('wait');
            await new Promise((resolve) =>
              setTimeout(resolve, this.consumerOpts.blockTimeMs || 1000),
            );
          } catch (e) {
            this.emit('error', e);
          }
        }
      })
      .catch((error) => this.emit('error', error));
  }

  private async processMessages(
    client: RedisClient,
    streamName: string,
    consumerGroup: string,
    messages: XReadGroupResult[],
    cb: (data: DataType, opts: JobsOptions) => Promise<void>,
  ): Promise<void> {
    const [, entries] = messages[0];
    for (const [id, fields] of entries) {
      const jobData: Record<string, string> = {};
      for (let i = 0; i < fields.length; i += 2) {
        const key = fields[i];
        jobData[key] = fields[i + 1];
      }
      const data = JSON.parse(jobData['data']);
      const opts = JSON.parse(jobData['opts']);
      try {
        await cb(data, opts);
        await client.xack(streamName, consumerGroup, id);
        debug('processed', data);
      } catch (e) {
        this.emit('error', e);
      }
    }
  }

  async trimStream(): Promise<void> {
    if (
      this.lastTrim + (this.consumerOpts.trimIntervalMs || 60000) >
      Date.now()
    ) {
      return;
    }
    debug('trim');
    this.lastTrim = Date.now();
    const streamName = this.name;
    const client = await this.client;
    const now = Date.now();
    const cutoffTime =
      now - this.consumerOpts.maxRetentionMs || 1000 * 60 * 60 * 24;
    const oldestMessages = await client.xrange(
      streamName,
      '-',
      '+',
      'COUNT',
      1,
    );

    if (oldestMessages.length > 0) {
      const oldestMessageId = oldestMessages[0][0];
      const [oldestTimestamp] = oldestMessageId.split('-').map(Number);

      if (oldestTimestamp < cutoffTime) {
        await client.xtrim(streamName, 'MINID', `${cutoffTime}-0`);
      }
    }
  }

  async getLength(): Promise<number> {
    const client = await this.client;
    return client.xlen(this.name);
  }
}
