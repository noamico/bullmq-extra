import { QueueBase, RedisConnection } from 'bullmq';
import { ProducerOptions } from './producer-options';
import * as _debug from 'debug';

const debug = _debug('bullmq:fanout:consumer');

export class StreamProducer<DataType = any> extends QueueBase {
  constructor(
    streamName: string,
    opts?: ProducerOptions,
    Connection?: typeof RedisConnection,
  ) {
    super(
      streamName,
      {
        blockingConnection: false,
        ...opts,
      },
      Connection,
    );

    this.waitUntilReady()
      .then(() => {
        // Nothing to do here atm
      })
      .catch(() => {
        // We ignore this error to avoid warnings. The error can still
        // be received by listening to event 'error'
      });
  }

  async produce(data: DataType): Promise<void> {
    const client = await this.client;
    await client.xadd(this.name, '*', 'data', JSON.stringify(data));
    debug('stream-producer.produce', data);
  }
}
