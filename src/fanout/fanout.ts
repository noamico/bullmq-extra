import { JobsOptions, Queue } from 'bullmq';
import { FanoutOptions } from './fanout-options';
import { Consumer } from './consumer';
import { QueueToStreamWorker } from './queue-to-stream-worker';
import * as _debug from 'debug';

const debug = _debug('bullmq:fanout:fanout');

export class Fanout<DataType = any> {
  private consumer: Consumer;
  private worker: QueueToStreamWorker;
  private closed: Promise<void>;
  private sourceQueue: string;
  private targetQueues: Queue<DataType>[] = [];
  private opts?: FanoutOptions = { connection: null };

  setSource(queueName: string): Fanout {
    this.sourceQueue = queueName;
    return this;
  }

  addTargets(...queues: Queue<DataType>[]): Fanout {
    this.targetQueues.push(...queues);
    return this;
  }

  setOptions(opts: FanoutOptions): Fanout {
    this.opts = opts;
    return this;
  }

  async run(): Promise<void> {
    const streamName = `bullmq__fanout__${this.sourceQueue}`;
    this.consumer = new Consumer(streamName, {
      blockingConnection: false,
      ...this.opts,
    });

    this.consumer
      .waitUntilReady()
      .then(() => {
        // Nothing to do here atm
      })
      .catch(() => {
        // We ignore this error to avoid warnings. The error can still
        // be received by listening to event 'error'
      });

    this.worker = new QueueToStreamWorker(
      this.sourceQueue,
      streamName,
      this.opts,
    );

    this.closed = new Promise<void>((resolve) => {
      this.consumer.on('close', () => {
        resolve();
      });
    });

    for (const queue of this.targetQueues) {
      const groupName = `${this.sourceQueue}:${queue.name}`;
      this.consumer.consume(
        groupName,
        async (data: DataType, opts: JobsOptions) => {
          const renderedOptsOverride = this.opts?.optsOverride
            ? this.opts.optsOverride(data)
            : {};
          const mergedOpts = { ...opts, ...renderedOptsOverride };
          await queue.add('default', data, mergedOpts);
          debug('add', queue.name, data);
        },
      );
    }
    await this.closed;
  }

  async close(): Promise<void> {
    await this.worker?.close();
    await this.consumer?.close();
  }
}
