import { JobsOptions, Queue } from 'bullmq';
import { RouterOptions } from './router-options';
import { Consumer } from './consumer';
import { QueueToStreamWorker } from './queue-to-stream-worker';
import * as _debug from 'debug';

const debug = _debug('bullmq:router:router');

export class Router<DataType = any> {
  private consumers: Consumer[] = [];
  private workers: QueueToStreamWorker[] = [];
  private closed: Promise<void>;
  private closedCount: number = 0;
  private sourceQueues: string[] = [];
  private targetQueues: Queue<DataType>[] = [];
  private opts?: RouterOptions = { connection: null };

  addSources(...queueNames: string[]): Router {
    this.sourceQueues.push(...queueNames);
    return this;
  }

  addTargets(...queues: Queue<DataType>[]): Router {
    this.targetQueues.push(...queues);
    return this;
  }

  setOptions(opts: RouterOptions): Router {
    this.opts = opts;
    return this;
  }

  async run(): Promise<void> {
    try {
      for (const sourceQueue of this.sourceQueues) {
        const streamName = `bullmq__router_${sourceQueue}`;
        const consumer = new Consumer(streamName, {
          blockingConnection: false,
          ...this.opts,
        });

        this.consumers.push(consumer);

        consumer
          .waitUntilReady()
          .then(() => {
            // Nothing to do here atm
          })
          .catch(() => {
            // We ignore this error to avoid warnings. The error can still
            // be received by listening to event 'error'
          });

        this.workers.push(
          new QueueToStreamWorker(sourceQueue, streamName, this.opts),
        );

        this.closed = new Promise<void>((resolve) => {
          consumer.on('close', () => {
            this.closedCount++;
            if (this.closedCount === this.consumers.length) {
              resolve();
            }
          });
        });

        for (const queue of this.targetQueues) {
          const groupName = `${sourceQueue}:${queue.name}`;
          consumer.consume(
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
      }
      await this.closed;
    } catch (e) {
      debug('error', e);
    }
  }

  async close(): Promise<void> {
    for (const worker of this.workers) {
      await worker.close();
    }
    for (const consumer of this.consumers) {
      await consumer.close();
    }
  }
}
