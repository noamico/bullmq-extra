import { ConnectionOptions, Queue, Worker } from 'bullmq';
import * as _debug from 'debug';

const debug = _debug('bullmq:reqrep:responder');

export class Responder<RequestType = any, ResponseType = any> {
  private responderName: string;
  private replyTos: Map<string, Queue<ResponseType>> = new Map();

  constructor(
    private opts: {
      opts: { connection: ConnectionOptions };
      responderName: string;
    },
  ) {
    this.responderName = opts.responderName;
  }

  public processRequests(
    processor: (data: RequestType) => Promise<ResponseType>,
  ) {
    new Worker(
      this.getFullResponderName(),
      async (job) => {
        const { data, replyTo } = job.data;
        const response = await processor(data);
        await this.getReplyTo(replyTo).add('response', response);
      },
      this.opts.opts,
    );
  }

  private getReplyTo(replyTo: string) {
    if (!this.replyTos.has(replyTo)) {
      this.replyTos.set(replyTo, new Queue(replyTo, this.opts.opts));
    }
    return this.replyTos.get(replyTo);
  }

  private getFullResponderName() {
    return `bullmq__responder__${this.responderName}`;
  }
}
