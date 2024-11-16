import { JobsOptions, QueueBase } from 'bullmq';
import { ProducerOptions } from './producer-options';
import * as _debug from 'debug';

const debug = _debug('bullmq:router:producer');

export class Producer<DataType = any> extends QueueBase {
  constructor(streamName: string, opts?: ProducerOptions) {
    super(streamName, {
      blockingConnection: false,
      ...opts,
    });

    this.waitUntilReady()
      .then((client) => {
        debug(`ready on ${client.options['port']}`);
      })
      .catch(() => {
        // We ignore this error to avoid warnings. The error can still
        // be received by listening to event 'error'
      });
  }

  async produce(data: DataType, opts?: JobsOptions): Promise<void> {
    const client = await this.client;
    await client.xadd(
      this.name,
      '*',
      'data',
      JSON.stringify(data),
      'opts',
      JSON.stringify(opts || {}),
    );
    debug('produce', data);
  }
}
