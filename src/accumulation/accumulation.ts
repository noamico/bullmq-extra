import { ConnectionOptions, Queue, Worker } from 'bullmq';
import BottleNeck from 'bottleneck';
import * as IORedis from 'ioredis';
import * as _debug from 'debug';
import { GetRedisInstance } from '../common';

const debug = _debug('bullmq:accumulation');

export type AccumulationSource<DataType = any> = {
  queue: string;
  getGroupKey: (data: DataType) => Promise<string>;
  prefix: string;
};

export class Accumulation<DataType = any, ResultType = any> {
  private timeoutQueue: Queue;
  private accumulationName: string;
  private timeout: number;
  private onComplete: (data: DataType[]) => Promise<ResultType>;
  private isComplete?: (data: DataType[]) => Promise<boolean>;
  private source: AccumulationSource;
  private target?: Queue<ResultType>;
  private limiter: BottleNeck.Group;
  private redis: IORedis.Redis | IORedis.Cluster;
  private worker: Worker;
  private timeoutWorker: Worker;

  constructor(
    private opts: {
      opts: { connection: ConnectionOptions };
      accumulationName: string;
      timeout: number;
      isComplete?: (data: DataType[]) => Promise<boolean>;
      onComplete: (data: DataType[]) => Promise<ResultType>;
      source: AccumulationSource;
      target?: Queue<ResultType>;
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
      `bullmq__accumulation__timeout_${this.accumulationName}`,
      {
        connection: this.redis,
        prefix: this.source.prefix,
        defaultJobOptions: {
          removeOnFail: { age: 60 * 60 * 24 }, // 1 day
          removeOnComplete: { count: 1000 },
        },
      },
    );
    this.limiter = new BottleNeck.Group({
      maxConcurrent: 1,
      Redis: this.redis,
    });
  }

  public run() {
    this.worker = new Worker(
      this.source.queue,
      async (job) => {
        const data = job.data;
        const groupKey = await this.source.getGroupKey(data);
        if (!groupKey) {
          debug('groupKey is undefined! skipping', data);
          return;
        }
        const limiterKey = `bullmq__accumulation_limiter_${this.accumulationName}_${groupKey}`;
        await this.storeData(data);
        await this.limiter.key(limiterKey).schedule(async () => {
          const result = await this.evaluate(groupKey);
          if (result) {
            debug('completed', result);
            await this.target?.add('completed', result);
          }
        });
      },
      {
        connection: this.redis,
        prefix: this.source.prefix,
      },
    );
    this.timeoutWorker = new Worker(
      this.timeoutQueue.name,
      async (job) => {
        const data = job.data;
        const { groupKey } = data;
        if (!groupKey) {
          debug('groupKey is undefined! skipping', data);
          return;
        }
        const limiterKey = `bullmq__accumulation_limiter_${this.accumulationName}_${groupKey}`;
        await this.limiter.key(limiterKey).schedule(async () => {
          const result = await this.evaluate(groupKey, true);
          if (result) {
            debug('timeout', result);
            await this.target?.add('completed', result);
          }
        });
      },
      {
        connection: this.redis,
        prefix: this.source.prefix,
      },
    );
  }

  public async close() {
    await this.worker.close();
    await this.timeoutWorker.close();
  }

  private async storeData(data: any) {
    const groupKey = await this.source.getGroupKey(data);
    if (!groupKey) {
      debug('groupKey is undefined! ignoring data', data);
      return;
    }
    const storeKey = `bullmq__accumulation_value_${this.accumulationName}_${groupKey}`;
    await this.redis.lpush(storeKey, JSON.stringify(data));
    await this.redis.pexpire(storeKey, this.timeout * 2);
  }

  private async evaluate(
    groupKey: string,
    terminate?: boolean,
  ): Promise<ResultType | void> {
    const completionKey = `bullmq__accumulation_isComplete_${this.accumulationName}_${groupKey}`;
    const keyCompleted = await this.redis.exists(completionKey);
    if (keyCompleted) {
      return;
    }
    const storedLen = await this.redis.llen(
      `bullmq__accumulation_value_${this.accumulationName}_${groupKey}`,
    );

    const data = (
      await this.redis.lrange(
        `bullmq__accumulation_value_${this.accumulationName}_${groupKey}`,
        0,
        -1,
      )
    ).map((stored) => JSON.parse(stored));

    if (terminate || (this.isComplete && (await this.isComplete(data)))) {
      const result = await this.onComplete(data);
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
