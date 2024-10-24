import { Job, RedisConnection, Worker, WorkerOptions } from 'bullmq';
import { Producer } from './producer';
import * as _debug from 'debug';

const debug = _debug('bullmq:fanout:queue-to-stream-worker');

export class QueueToStreamWorker extends Worker {
  constructor(
    name: string,
    streamName: string,
    opts?: WorkerOptions,
    Connection?: typeof RedisConnection,
  ) {
    const producer = new Producer(
      streamName,
      { connection: undefined },
      Connection,
    );
    const processor = async (job: Job) => {
      await producer.produce(job.data, job.opts);
      debug('produce', job.data, job.opts);
    };
    super(name, processor, opts, Connection);
  }
}
