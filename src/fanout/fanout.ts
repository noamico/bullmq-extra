import { JobsOptions, Queue } from 'bullmq';
import { FanoutOptions } from './fanout-options';
import { Consumer } from './consumer';
import { QueueToStreamWorker } from './queue-to-stream-worker';
import * as _debug from 'debug';

const debug = _debug('bullmq:fanout:consumer');

export class Fanout<DataType = any> {
  private consumer: Consumer;
  private worker: QueueToStreamWorker;
  private closed: Promise<void>;

  constructor(sourceQueue: string, opts?: FanoutOptions) {
    const streamName = `bullmq__fanout__${sourceQueue}`;
    this.consumer = new Consumer(streamName, {
      blockingConnection: false,
      ...opts,
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

    this.worker = new QueueToStreamWorker(sourceQueue, streamName, opts);

    this.closed = new Promise<void>((resolve) => {
      this.consumer.on('close', () => {
        resolve();
      });
    });
  }

  async fanout(
    group: string,
    targetQueues: Queue<DataType>[],
    opts?: (data: DataType) => JobsOptions,
  ): Promise<void> {
    for (const queue of targetQueues) {
      const groupName = `${group}:${queue.name}`;
      this.consumer.consume(groupName, async (data: DataType) => {
        const renderedOpts = opts ? opts(data) : undefined;
        await queue.add('default', data, renderedOpts);
        debug('fanout.add', queue.name, data);
      });
    }
    await this.closed;
  }

  async close(): Promise<void> {
    await this.worker.close();
    await this.consumer.close();
  }
}
