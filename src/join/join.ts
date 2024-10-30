import { ConnectionOptions, Queue, Worker } from 'bullmq';
import BottleNeck from 'bottleneck';
import * as IORedis from 'ioredis';
import * as _debug from 'debug';

const debug = _debug('bullmq:join');

export type Source<DataType = any> = {
  queue: string;
  getJoinKey: (data: DataType) => string;
};

export class Join<ResultType = any> {
  private timeoutQueue: Queue;
  private joinName: string;
  private timeout?: number;
  private onComplete: (data: { queue: string; val: any }[]) => ResultType;
  private sources: Source[];
  private target: Queue<ResultType>;
  private limiter: BottleNeck.Group;
  private redis: IORedis.Redis | IORedis.Cluster;

  constructor(opts: {
    opts: { connection: ConnectionOptions };
    joinName: string;
    timeout?: number;
    onComplete: (data: { queue: string; val: any }[]) => ResultType;
    sources: Source[];
    target: Queue<ResultType>;
  }) {
    this.joinName = opts.joinName;
    this.timeout = opts.timeout;
    this.onComplete = opts.onComplete;
    this.sources = opts.sources;
    this.target = opts.target;
    this.timeoutQueue = new Queue(
      `bullmq__join__timeout__${this.joinName}`,
      opts.opts,
    );
    this.redis = this.getIORedisInstance(opts.opts.connection);
    this.limiter = new BottleNeck.Group({
      maxConcurrent: 1,
      Redis: this.redis,
    });
  }

  public run() {
    for (const source of this.sources) {
      new Worker(
        source.queue,
        async (job) => {
          const data = job.data;
          const limiterKey = `bullmq__join:limiter:${this.joinName}:${source.getJoinKey(data)}`;
          await this.storeData(source, data);
          await this.limiter.key(limiterKey).schedule(async () => {
            const result = await this.evaluateJoin(source.getJoinKey(data));
            if (result) {
              debug('completed', result);
              await this.target.add('completed', result);
            }
          });
        },
        {
          connection: this.redis,
        },
      );
    }
    new Worker(
      this.timeoutQueue.name,
      async (job) => {
        const data = job.data;
        const { joinKey } = data;
        const limiterKey = `bullmq__join:limiter:${this.joinName}:${joinKey}`;
        await this.limiter.key(limiterKey).schedule(async () => {
          const result = await this.evaluateJoin(joinKey, true);
          if (result) {
            debug('timeout', result);
            await this.target.add('completed', result);
          }
        });
      },
      {
        connection: this.redis,
      },
    );
  }

  private async storeData(source: Source, data: any) {
    const joinKey = source.getJoinKey(data);
    const storeKey = `bullmq__join:value:${this.joinName}:${joinKey}:${source.queue}`;
    await this.redis.set(storeKey, JSON.stringify(data));
    await this.redis.expire(storeKey, (this.timeout || 1000 * 60 * 60) * 2);
  }

  private async evaluateJoin(
    joinKey: string,
    terminate?: boolean,
  ): Promise<ResultType | void> {
    const completionKey = `bullmq__join:isComplete:${this.joinName}:${joinKey}`;
    const isComplete = await this.redis.exists(completionKey);
    if (isComplete) {
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
      const result = this.onComplete(data);
      await this.redis.set(completionKey, '1');
      await this.redis.expire(
        completionKey,
        (this.timeout || 1000 * 60 * 60) * 2,
      );
      return result;
    }

    if (allStored.length === 1) {
      await this.timeoutQueue.add(
        'timeout',
        { joinKey },
        { delay: this.timeout || 1000 * 60 * 60 },
      );
    }
  }

  getIORedisInstance(
    connection: ConnectionOptions,
  ): IORedis.Redis | IORedis.Cluster {
    if (
      connection instanceof IORedis.Redis ||
      connection instanceof IORedis.Cluster
    ) {
      return connection;
    } else if (Array.isArray(connection)) {
      return new IORedis.Cluster(connection);
    } else {
      return new IORedis.Redis(connection);
    }
  }
}
