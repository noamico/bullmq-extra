import { ConnectionOptions, Queue, Worker } from 'bullmq';
import BottleNeck from 'bottleneck';
import * as IORedis from 'ioredis';
import * as _debug from 'debug';
import { GetRedisInstance } from '../common';

const debug = _debug('bullmq:join');

export type JoinSource<DataType = any> = {
  queue: string;
  getJoinKey: (data: DataType) => Promise<string>;
};

export class Join<ResultType = any> {
  private timeoutQueue: Queue;
  private joinName: string;
  private timeout?: number;
  private onComplete: (
    data: { queue: string; val: any }[],
  ) => Promise<ResultType>;
  private sources: JoinSource[];
  private target?: Queue;
  private limiter: BottleNeck.Group;
  private redis: IORedis.Redis | IORedis.Cluster;
  private timeoutWorker: Worker;
  private workers = new Map<string, Worker>();

  constructor(opts: {
    opts: { connection: ConnectionOptions };
    joinName: string;
    timeout: number;
    onComplete: (data: { queue: string; val: any }[]) => Promise<ResultType>;
    sources: JoinSource[];
    target?: Queue;
  }) {
    this.joinName = opts.joinName;
    this.timeout = opts.timeout;
    this.onComplete = opts.onComplete;
    this.sources = opts.sources;
    this.target = opts.target;
    this.redis = GetRedisInstance.getIORedisInstance(opts.opts.connection);
    this.timeoutQueue = new Queue(`bullmq__join__timeout__${this.joinName}`, {
      connection: this.redis,
      prefix: `{${this.joinName}}`,
      defaultJobOptions: {
        removeOnFail: { age: 60 * 60 * 24 }, // 1 day
        removeOnComplete: { count: 1000 },
      },
    });
    this.limiter = new BottleNeck.Group({
      maxConcurrent: 1,
      Redis: this.redis,
    });
  }

  public async close() {
    for (const worker of this.workers.values()) {
      await worker.close();
    }
    await this.timeoutWorker.close();
  }

  public run() {
    for (const source of this.sources) {
      this.workers.set(
        source.queue,
        new Worker(
          source.queue,
          async (job) => {
            const data = job.data;
            const joinKey = await source.getJoinKey(data);
            if (!joinKey) {
              debug('joinKey is undefined! skipping', data);
              return;
            }
            const limiterKey = `bullmq__join:limiter:${this.joinName}:${joinKey}`;
            await this.storeData(source, data);
            await this.limiter.key(limiterKey).schedule(async () => {
              const result = await this.evaluate(joinKey);
              if (result) {
                debug('completed', result);
                await this.target?.add('completed', result);
              }
            });
          },
          {
            connection: this.redis,
            prefix: `{${this.joinName}}`,
          },
        ),
      );
    }
    this.timeoutWorker = new Worker(
      this.timeoutQueue.name,
      async (job) => {
        const data = job.data;
        const { joinKey } = data;
        if (!joinKey) {
          debug('joinKey is undefined! skipping', data);
          return;
        }
        const limiterKey = `bullmq__join:limiter:${this.joinName}:${joinKey}`;
        await this.limiter.key(limiterKey).schedule(async () => {
          const result = await this.evaluate(joinKey, true);
          if (result) {
            debug('timeout', result);
            await this.target?.add('completed', result);
          }
        });
      },
      {
        connection: this.redis,
        prefix: `{${this.joinName}}`,
      },
    );
  }

  private async storeData(source: JoinSource, data: any) {
    const joinKey = await source.getJoinKey(data);
    if (!joinKey) {
      debug('joinKey is undefined! ignoring data', data);
      return;
    }
    const storeKey = `bullmq__join:value:${this.joinName}:${joinKey}:${source.queue}`;
    await this.redis.set(storeKey, JSON.stringify(data));
    await this.redis.pexpire(storeKey, this.timeout * 2);
  }

  private async evaluate(
    joinKey: string,
    terminate?: boolean,
  ): Promise<ResultType | void> {
    const completionKey = `bullmq__join:isComplete:${this.joinName}:${joinKey}`;
    const isComplete = await this.redis.exists(completionKey);
    if (isComplete) {
      debug('joinKey is already completed', completionKey);
      return;
    }
    const allStored = (
      await Promise.all(
        this.sources.map(async (source) => {
          const val = await this.redis.get(
            `bullmq__join:value:${this.joinName}:${joinKey}:${source.queue}`,
          );
          return { queue: source.queue, val };
        }),
      )
    ).filter((stored) => stored.val);

    if (allStored.length === this.sources.length || terminate) {
      const data = allStored.map((stored) => ({
        queue: stored.queue,
        val: JSON.parse(stored.val),
      }));
      const result = await this.onComplete(data);
      await this.redis.set(completionKey, '1');
      await this.redis.pexpire(completionKey, this.timeout * 2);
      return result;
    }

    if (allStored.length === 1) {
      await this.timeoutQueue.add(
        'timeout',
        { joinKey },
        { delay: this.timeout },
      );
    }
  }
}
