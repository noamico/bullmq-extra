import { Queue, Worker } from 'bullmq';
import { Redis } from 'ioredis';

export type Source<DataType = any> = {
  queue: string;
  getJoinkey: (data: DataType) => string;
};

export class Join<ResultType = any> {
  private timeoutQueue: Queue;
  private redis: Redis;
  private group: string;
  private timeout?: number;
  private onComplete: (data: { queue: string; val: any }[]) => ResultType;
  private sources: Source[];
  private target: Queue<ResultType>;

  constructor(opts: {
    redis: Redis;
    group: string;
    timeout?: number;
    onComplete: (data: { queue: string; val: any }[]) => ResultType;
    sources: Source[];
    target: Queue<ResultType>;
  }) {
    this.redis = opts.redis;
    this.group = opts.group;
    this.timeout = opts.timeout;
    this.onComplete = opts.onComplete;
    this.sources = opts.sources;
    this.target = opts.target;
    this.timeoutQueue = new Queue(`bullmq__join:timeout:${this.group}`, {
      connection: this.redis,
    });
  }

  public run() {
    for (const source of this.sources) {
      new Worker(
        source.queue,
        async (job) => {
          const data = job.data;
          await this.storeData(source, data);
          const result = await this.evaluateJoin(source.getJoinkey(data));
          if (result) {
            await this.target.add('completed', result);
          }
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
        const result = await this.evaluateJoin(joinKey, true);
        if (result) {
          await this.target.add('completed', result);
        }
      },
      {
        connection: this.redis,
      },
    );
  }

  private async storeData(source: Source, data: any) {
    const joinKey = source.getJoinkey(data);
    const storeKey = `bullmq__join:${this.group}:${joinKey}:${source.queue}`;
    await this.redis.set(storeKey, JSON.stringify(data));
    await this.redis.expire(storeKey, (this.timeout || 1000 * 60 * 60) * 2);
  }

  private async evaluateJoin(
    joinKey: string,
    terminate?: boolean,
  ): Promise<ResultType | void> {
    const allStored = await Promise.all(
      this.sources.map(async (source) => {
        const val = await this.redis.get(
          `bullmq__join:${this.group}:${joinKey}:${source.queue}`,
        );
        return { queue: source.queue, val };
      }),
    );

    if (allStored.length === this.sources.length || terminate) {
      const data = allStored.map((stored) => ({
        queue: stored.queue,
        val: JSON.parse(stored.val),
      }));
      return this.onComplete(data);
    }

    if (allStored.length === 1) {
      await this.timeoutQueue.add(
        'timeout',
        { joinKey },
        { delay: this.timeout || 1000 * 60 * 60 },
      );
    }
  }
}
