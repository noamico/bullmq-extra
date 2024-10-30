import { Queue, Worker } from 'bullmq';
import { Redis } from 'ioredis';
import BottleNeck from 'bottleneck';
import * as _debug from 'debug';

const debug = _debug('bullmq:accumulation');

export type Source<DataType = any> = {
  queue: string;
  getGroupKey: (data: DataType) => string;
};

export class Accumulation<ResultType = any> {
  private timeoutQueue: Queue;
  private redis: Redis;
  private groupName: string;
  private timeout?: number;
  private onComplete: (data: { queue: string; val: any }[]) => ResultType;
  private sources: Source[];
  private target: Queue<ResultType>;
  private limiter: BottleNeck.Group;

  constructor(opts: {
    redis: Redis;
    groupName: string;
    timeout?: number;
    onComplete: (data: { queue: string; val: any }[]) => ResultType;
    sources: Source[];
    target: Queue<ResultType>;
  }) {
    this.redis = opts.redis;
    this.groupName = opts.groupName;
    this.timeout = opts.timeout;
    this.onComplete = opts.onComplete;
    this.sources = opts.sources;
    this.target = opts.target;
    this.timeoutQueue = new Queue(`bullmq__group__timeout__${this.groupName}`, {
      connection: this.redis,
    });
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
          const limiterKey = `bullmq__group:limiter:${this.groupName}:${source.getGroupKey(data)}`;
          await this.storeData(source, data);
          await this.limiter.key(limiterKey).schedule(async () => {
            const result = await this.evaluateJoin(source.getGroupKey(data));
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
        const { groupKey } = data;
        const limiterKey = `bullmq__group:limiter:${this.groupName}:${groupKey}`;
        await this.limiter.key(limiterKey).schedule(async () => {
          const result = await this.evaluateJoin(groupKey, true);
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
    const groupKey = source.getGroupKey(data);
    const storeKey = `bullmq__group:value:${this.groupName}:${groupKey}:${source.queue}`;
    await this.redis.set(storeKey, JSON.stringify(data));
    await this.redis.expire(storeKey, (this.timeout || 1000 * 60 * 60) * 2);
  }

  private async evaluateJoin(
    groupKey: string,
    terminate?: boolean,
  ): Promise<ResultType | void> {
    const completionKey = `bullmq__group:isComplete:${this.groupName}:${groupKey}`;
    const isComplete = await this.redis.exists(completionKey);
    if (isComplete) {
      return;
    }
    const allStored = (
      await Promise.all(
        this.sources.map(async (source) => {
          const val = await this.redis.get(
            `bullmq__group:value:${this.groupName}:${groupKey}:${source.queue}`,
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
        { groupKey },
        { delay: this.timeout || 1000 * 60 * 60 },
      );
    }
  }
}
