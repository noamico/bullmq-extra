import { ConnectionOptions, Queue, Worker } from 'bullmq';
import BottleNeck from 'bottleneck';
import * as IORedis from 'ioredis';
import * as _debug from 'debug';
import { GetRedisInstance } from '../common';

const debug = _debug('bullmq:accumulation');

export type AccumulationSource<DataType = any> = {
  queue: string;
  getGroupKey: (data: DataType) => string;
};

export class Accumulation<DataType = any, ResultType = any> {
  private timeoutQueue: Queue;
  private accumulationName: string;
  private timeout: number;
  private onComplete: (data: DataType[]) => ResultType;
  private isComplete?: (data: DataType[]) => Promise<boolean>;
  private source: AccumulationSource;
  private target: Queue<ResultType>;
  private limiter: BottleNeck.Group;
  private redis: IORedis.Redis | IORedis.Cluster;

  constructor(
    private opts: {
      opts: { connection: ConnectionOptions };
      accumulationName: string;
      timeout: number;
      isComplete?: (data: DataType[]) => Promise<boolean>;
      onComplete: (data: DataType[]) => ResultType;
      source: AccumulationSource;
      target: Queue<ResultType>;
    },
  ) {
    this.accumulationName = opts.accumulationName;
    this.timeout = opts.timeout;
    this.onComplete = opts.onComplete;
    this.isComplete = opts.isComplete;
    this.source = opts.source;
    this.target = opts.target;
    this.redis = GetRedisInstance.getIORedisInstance(opts.opts.connection);
    this.timeoutQueue = new Queue(
      `bullmq__accumulation__timeout__${this.accumulationName}`,
      { connection: this.redis },
    );
    this.limiter = new BottleNeck.Group({
      maxConcurrent: 1,
      Redis: this.redis,
    });
  }

  public run() {
    new Worker(
      this.source.queue,
      async (job) => {
        const data = job.data;
        const limiterKey = `bullmq__accumulation:limiter:${this.accumulationName}:${this.source.getGroupKey(data)}`;
        await this.storeData(data);
        await this.limiter.key(limiterKey).schedule(async () => {
          const result = await this.evaluate(this.source.getGroupKey(data));
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
    new Worker(
      this.timeoutQueue.name,
      async (job) => {
        const data = job.data;
        const { groupKey } = data;
        const limiterKey = `bullmq__accumulation:limiter:${this.accumulationName}:${groupKey}`;
        await this.limiter.key(limiterKey).schedule(async () => {
          const result = await this.evaluate(groupKey, true);
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

  private async storeData(data: any) {
    const groupKey = this.source.getGroupKey(data);
    const storeKey = `bullmq__accumulation:value:${this.accumulationName}:${groupKey}`;
    await this.redis.lpush(storeKey, JSON.stringify(data));
    await this.redis.pexpire(storeKey, this.timeout * 2);
  }

  private async evaluate(
    groupKey: string,
    terminate?: boolean,
  ): Promise<ResultType | void> {
    const completionKey = `bullmq__accumulation:isComplete:${this.accumulationName}:${groupKey}`;
    const keyCompleted = await this.redis.exists(completionKey);
    if (keyCompleted) {
      return;
    }
    const storedLen = await this.redis.llen(
      `bullmq__accumulation:value:${this.accumulationName}:${groupKey}`,
    );

    const data = (
      await this.redis.lrange(
        `bullmq__accumulation:value:${this.accumulationName}:${groupKey}`,
        0,
        -1,
      )
    ).map((stored) => JSON.parse(stored));

    if (terminate || (this.isComplete && (await this.isComplete(data)))) {
      const result = this.onComplete(data);
      await this.redis.set(completionKey, '1');
      await this.redis.pexpire(completionKey, this.timeout * 2);
      return result;
    }

    if (storedLen === 1) {
      await this.timeoutQueue.add(
        'timeout',
        { groupKey },
        { delay: this.timeout },
      );
    }
  }
}
