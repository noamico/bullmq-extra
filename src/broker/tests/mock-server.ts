import Fastify from 'fastify';

export class MockServer {
  private server;
  constructor() {
    this.server = Fastify();
  }

  public async start(
    port: number,
    processor: (a: number, b: number) => Promise<void>,
  ) {
    this.server.post('/job', async (request, reply) => {
      const { a, b } = request.body;
      await processor(a, b);
      reply.send({ success: true });
    });
    await this.server.listen({ port });
  }

  public async stop() {
    await this.server.close();
  }
}
