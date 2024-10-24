import { Job, RedisConnection, Worker, WorkerOptions } from 'bullmq';
import { StreamProducer } from './stream-producer';
import * as _debug from 'debug';

const debug = _debug('bullmq:fanout:consumer');

export class QueueToStreamWorker extends Worker {
  constructor(
    name: string,
    streamName: string,
    opts?: WorkerOptions,
    Connection?: typeof RedisConnection,
  ) {
    const producer = new StreamProducer(
      streamName,
      { connection: undefined },
      Connection,
    );
    const processor = async (job: Job) => {
      await producer.produce(job.data);
      debug('queue-to-stream-worker.produce', job.data);
    };
    super(name, processor, opts, Connection);
  }
}
