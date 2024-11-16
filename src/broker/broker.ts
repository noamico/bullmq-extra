import * as _debug from 'debug';
import Fastify from 'fastify';
import {
  ConnectionOptions,
  Job,
  JobsOptions,
  Queue,
  QueueOptions,
  Worker,
  WorkerOptions,
} from 'bullmq';
import axios from 'axios';
import { Router, RouterOptions } from '../router';

const debug = _debug('bullmq:broker');

export type JobBody = {
  name: string;
  data: any;
  opts: JobsOptions;
};

export type QueueBody = {
  name: string;
  opts: QueueOptions;
};

export type WorkerBody = {
  name: string;
  callback: string;
  opts: WorkerOptions;
};

export type RouterBody = {
  name: string;
  sources: string[];
  targets: { name: string; opts?: QueueOptions }[];
  opts: RouterOptions;
};

export class Broker {
  private server;
  private queues: Map<string, Queue> = new Map();
  private workers: Map<string, Worker> = new Map();
  private routers: Map<string, Router> = new Map();

  constructor(private opts: { connection?: ConnectionOptions }) {
    this.server = Fastify();
    this.setupRoutes();
  }

  private setupRoutes() {
    this.server.post('/queue', async (request: { body: QueueBody }, reply) => {
      this.createQueue(request.body);
      reply.send({ created: true });
    });

    this.server.post('/job', async (request: { body: JobBody }, reply) => {
      const { name, data, opts } = request.body;
      const queue = this.getQueue(name);
      const job = await queue.add(name, data, opts);
      reply.send({ jobId: job.id });
    });

    this.server.post(
      '/worker',
      async (request: { body: WorkerBody }, reply) => {
        this.startWorker(request.body);
        reply.send({ running: true });
      },
    );

    this.server.post(
      '/router',
      async (request: { body: RouterBody }, reply) => {
        this.startRouter(request.body);
        reply.send({ running: true });
      },
    );

    // TODO: add joins, accumulation, etc
  }

  public async start(port: number) {
    try {
      await this.server.listen({ port, host: '0.0.0.0' });
      debug(`Broker server started on port ${port}`);
    } catch (err) {
      debug('Error starting server:', err);
      process.exit(1);
    }
  }

  private createQueue(body: QueueBody) {
    const { name, opts } = body;
    if (!this.queues.has(name)) {
      const queue = new Queue(name, {
        ...opts,
        connection: this.opts.connection,
      });
      this.queues.set(name, queue);
    }
    return this.queues.get(name);
  }

  private getQueue(name: string) {
    return this.queues.get(name);
  }

  private startWorker(body: WorkerBody) {
    const { name, callback, opts } = body;
    if (!this.workers.has(name)) {
      const worker = new Worker(
        name,
        async (job: Job) => {
          const { data } = job;
          await axios.post(
            callback,
            { name, data },
            {
              headers: { 'Content-Type': 'application/json' },
            },
          );
        },
        { ...opts, connection: this.opts.connection },
      );
      this.workers.set(name, worker);
    }
  }

  private startRouter(body: RouterBody) {
    if (!this.routers.has(body.name)) {
      const { sources, targets, opts } = body;
      const router = new Router({
        sources,
        targets: targets.map((t) => new Queue(t.name, t.opts)),
        opts: { ...opts, connection: this.opts.connection },
      });
      router
        .run()
        .then()
        .catch((err) => {
          debug('Error starting router:', err);
          process.exit(1);
        });
      this.routers.set(body.name, router);
    }
  }

  public async stop() {
    await this.server.close();
  }
}
