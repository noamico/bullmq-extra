import { ConnectionOptions, Queue, Worker } from 'bullmq';

export class Requester<RequestType = any, ResponseType = any> {
  private requesterName: string;
  private target: Queue<{ data: RequestType; replyTo: string }>;

  constructor(
    private opts: {
      opts: { connection: ConnectionOptions };
      requesterName: string;
      responderName: string;
    },
  ) {
    this.requesterName = opts.requesterName;
    this.target = new Queue(
      `bullmq__responder__${opts.responderName}`,
      opts.opts,
    );
  }

  public processResponses(onResponse: (data: ResponseType) => Promise<void>) {
    new Worker(
      this.getReplyTo(),
      async (job) => {
        await onResponse(job.data);
      },
      this.opts.opts,
    );
  }

  public async request(data: RequestType) {
    await this.target.add('request', {
      data,
      replyTo: this.getReplyTo(),
    });
  }

  private getReplyTo() {
    return `bullmq__replyTo__${this.requesterName}`;
  }
}
