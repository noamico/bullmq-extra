import { v4 } from 'uuid';
import { delay, Queue, RedisOptions } from 'bullmq';
import { GenericContainer, Wait } from 'testcontainers';
import { Consumer } from './consumer';
import { Producer } from './producer';
import { Router } from './router';

jest.setTimeout(60000);

describe('router', function () {
  let connection: RedisOptions;
  beforeAll(async function () {
    const redisContainerSetup = new GenericContainer('redis:7.4.0')
      .withExposedPorts(6379)
      .withWaitStrategy(
        Wait.forLogMessage(/.*Ready to accept connections tcp.*/, 1),
      );
    const redisContainer = await redisContainerSetup.start();
    const mappedPort = redisContainer.getMappedPort(6379);
    connection = {
      port: mappedPort,
      maxRetriesPerRequest: null,
    };
  });

  describe('produce-consume', () => {
    describe('when consuming', () => {
      it('should trim to retention', async () => {
        const streamName = `test-${v4()}`;
        const consumerGroup = `test-${v4()}`;
        const producer = new Producer(streamName, {
          connection,
        });
        const consumer = new Consumer(streamName, {
          connection,
          maxRetentionMs: 100,
          trimIntervalMs: 100,
        });
        const jobs = 10;
        const processed: any[] = [];
        consumer.consume(consumerGroup, async (job) => {
          processed.push(job);
        });

        for (let iterations = 0; iterations < 5; iterations++) {
          for (let i = 1; i <= jobs; i++) {
            await producer.produce({ idx: i * iterations });
          }
          const length = await consumer.getLength();
          expect(length).toBeLessThanOrEqual(jobs * 2); // trimming is not exact but it does happen
          await delay(1000);
        }

        await consumer.close();
      });

      it('should respect batchSize', async () => {
        const streamName = `test-${v4()}`;
        const consumerGroup = `test-${v4()}`;
        const producer = new Producer(streamName, {
          connection,
        });
        const consumer = new Consumer(streamName, {
          connection,
          batchSize: 10,
        });
        const jobs = 10;
        const processed: any[] = [];
        consumer.consume(consumerGroup, async (job) => {
          processed.push(job);
        });

        for (let i = 1; i <= jobs; i++) {
          await producer.produce({ idx: i });
        }
        while (processed.length < jobs) {
          await delay(50);
        }
        expect(processed.length).toEqual(jobs);
        expect(processed.map((job) => job.idx)).toEqual(
          Array.from(Array(jobs).keys()).map((i) => i + 1),
        );
        await consumer.close();
      });
    });

    describe('when jobs produced with an active consumer', () => {
      it('should process the jobs', async () => {
        const streamName = `test-${v4()}`;
        const consumerGroup = `test-${v4()}`;
        const producer = new Producer(streamName, {
          connection,
        });
        const consumer = new Consumer(streamName, {
          connection,
        });
        const jobs = 10;
        const processed: any[] = [];
        consumer.consume(consumerGroup, async (job) => {
          processed.push(job);
        });

        for (let i = 1; i <= jobs; i++) {
          await producer.produce({ idx: i });
        }
        while (processed.length < jobs) {
          await delay(50);
        }
        expect(processed.length).toEqual(jobs);
        expect(processed.map((job) => job.idx)).toEqual(
          Array.from(Array(jobs).keys()).map((i) => i + 1),
        );
        await consumer.close();
      });
    });

    describe('when jobs produced with a late consumer', () => {
      it('should process the jobs', async () => {
        const streamName = `test-${v4()}`;
        const consumerGroup = `test-${v4()}`;
        const producer = new Producer(streamName, {
          connection,
        });
        const consumer = new Consumer(streamName, {
          connection,
        });
        const jobs = 10;
        const processed: any[] = [];

        for (let i = 1; i <= jobs / 2; i++) {
          await producer.produce({ idx: i });
        }
        consumer.consume(consumerGroup, async (job) => {
          processed.push(job);
        });
        for (let i = jobs / 2 + 1; i <= jobs; i++) {
          await producer.produce({ idx: i });
        }
        while (processed.length < jobs) {
          await delay(50);
        }
        expect(processed.length).toEqual(jobs);
        expect(processed.map((job) => job.idx)).toEqual(
          Array.from(Array(jobs).keys()).map((i) => i + 1),
        );
        await consumer.close();
      });
    });
  });

  describe('router', () => {
    describe('one->many', () => {
      describe('when jobs produced with an active router', () => {
        it('should route to defined queues', async () => {
          const sourceQueueName = `test-${v4()}`;
          const sourceQueue = new Queue(sourceQueueName, {
            connection,
          });
          const targetQueues = [
            new Queue(`test-${v4()}`, { connection }),
            new Queue(`test-${v4()}`, { connection }),
          ];
          const router = new Router({
            sources: [sourceQueueName],
            targets: targetQueues,
            opts: { connection },
          });
          const jobs = 10;

          router.run().then();

          for (let i = 1; i <= jobs; i++) {
            await sourceQueue.add('default', { idx: i });
          }
          while (
            (await targetQueues[0].count()) < jobs ||
            (await targetQueues[1].count()) < jobs
          ) {
            await delay(50);
          }
          for (const queue of targetQueues) {
            expect(await queue.count()).toEqual(jobs);
            expect(
              (await queue.getWaiting()).map((job) => job.data.idx),
            ).toEqual(Array.from(Array(jobs).keys()).map((i) => i + 1));
          }
          await router.close();
        });
      });

      describe('when jobs produced with job options', () => {
        it('should set options on target jobs', async () => {
          const sourceQueueName = `test-${v4()}`;
          const sourceQueue = new Queue(sourceQueueName, {
            connection,
          });
          const targetQueues = [
            new Queue(`test-${v4()}`, { connection }),
            new Queue(`test-${v4()}`, { connection }),
          ];
          const router = new Router({
            sources: [sourceQueueName],
            targets: targetQueues,
            opts: { connection },
          });
          const jobs = 10;

          router.run().then();

          for (let i = 1; i <= jobs; i++) {
            await sourceQueue.add(
              'default',
              { idx: i },
              { jobId: `test-${i}` },
            );
          }
          while (
            (await targetQueues[0].count()) < jobs ||
            (await targetQueues[1].count()) < jobs
          ) {
            await delay(50);
          }
          for (const queue of targetQueues) {
            expect(await queue.count()).toEqual(jobs);
            expect((await queue.getWaiting()).map((job) => job.opts)).toEqual(
              Array.from(Array(jobs).keys()).map((i) => ({
                jobId: `test-${i + 1}`,
                attempts: 0,
                backoff: undefined,
              })),
            );
          }
          await router.close();
        });

        it('should override options on target jobs', async () => {
          const sourceQueueName = `test-${v4()}`;
          const sourceQueue = new Queue(sourceQueueName, {
            connection,
          });
          const targetQueues = [
            new Queue(`test-${v4()}`, { connection }),
            new Queue(`test-${v4()}`, { connection }),
          ];
          const router = new Router({
            sources: [sourceQueueName],
            targets: targetQueues,
            opts: {
              connection,
              optsOverride: (data) => ({ jobId: `test-${data.idx + 10}` }),
            },
          });

          const jobs = 10;

          router.run().then();

          for (let i = 1; i <= jobs; i++) {
            await sourceQueue.add('default', { idx: i });
          }
          while (
            (await targetQueues[0].count()) < jobs ||
            (await targetQueues[1].count()) < jobs
          ) {
            await delay(50);
          }
          for (const queue of targetQueues) {
            expect(await queue.count()).toEqual(jobs);
            expect((await queue.getWaiting()).map((job) => job.opts)).toEqual(
              Array.from(Array(jobs).keys()).map((i) => ({
                jobId: `test-${i + 1 + 10}`,
                attempts: 0,
                backoff: undefined,
              })),
            );
          }
          await router.close();
        });
      });

      describe('when jobs produced with a late router', () => {
        it('should route to defined queues', async () => {
          const sourceQueueName = `test-${v4()}`;
          const sourceQueue = new Queue(sourceQueueName, {
            connection,
          });
          const targetQueues = [
            new Queue(`test-${v4()}`, { connection }),
            new Queue(`test-${v4()}`, { connection }),
          ];
          const router = new Router({
            sources: [sourceQueueName],
            targets: targetQueues,
            opts: { connection },
          });

          const jobs = 10;

          for (let i = 1; i <= jobs / 2; i++) {
            await sourceQueue.add('default', { idx: i });
          }

          router.run().then().catch(console.error);

          for (let i = jobs / 2 + 1; i <= jobs; i++) {
            await sourceQueue.add('default', { idx: i });
          }

          while (
            (await targetQueues[0].count()) < jobs ||
            (await targetQueues[1].count()) < jobs
          ) {
            await delay(50);
          }
          for (const queue of targetQueues) {
            expect(await queue.count()).toEqual(jobs);
            expect(
              (await queue.getWaiting()).map((job) => job.data.idx),
            ).toEqual(Array.from(Array(jobs).keys()).map((i) => i + 1));
          }
          await router.close();
        });
      });

      describe('when router is stopped and restarted', () => {
        it('should not consume acked messages', async () => {
          const sourceQueueName = `test-${v4()}`;
          const sourceQueue = new Queue(sourceQueueName, {
            connection,
          });
          const targetQueues = [
            new Queue(`test-${v4()}`, { connection }),
            new Queue(`test-${v4()}`, { connection }),
          ];
          const router = new Router({
            sources: [sourceQueueName],
            targets: targetQueues,
            opts: { connection },
          });
          const laterRouter = new Router({
            sources: [sourceQueueName],
            targets: targetQueues,
            opts: { connection },
          });
          const jobs = 10;

          router.run().then().catch(console.error);

          for (let i = 1; i <= jobs; i++) {
            await sourceQueue.add('default', { idx: i });
          }

          while (
            (await targetQueues[0].count()) < jobs ||
            (await targetQueues[1].count()) < jobs
          ) {
            await delay(50);
          }
          await router.close();

          laterRouter.run().then().catch(console.error);

          for (const queue of targetQueues) {
            await queue.clean(0, 1000, 'wait');
          }

          for (let i = 1; i <= jobs; i++) {
            await sourceQueue.add('default', { idx: i + 20 });
          }

          while (
            (await targetQueues[0].count()) < jobs ||
            (await targetQueues[1].count()) < jobs
          ) {
            await delay(50);
          }

          for (const queue of targetQueues) {
            expect(await queue.count()).toEqual(jobs);
            expect(
              (await queue.getWaiting()).map((job) => job.data.idx),
            ).toEqual(Array.from(Array(jobs).keys()).map((i) => i + 1 + 20));
          }
          await laterRouter.close();
        });
      });
    });

    describe('many->one', () => {
      describe('when jobs produced with an active router', () => {
        it('should route to defined queues', async () => {
          const sourceQueues = [
            new Queue(`test-${v4()}`, { connection }),
            new Queue(`test-${v4()}`, { connection }),
          ];
          const targetQueueName = `test-${v4()}`;
          const targetQueue = new Queue(targetQueueName, {
            connection,
          });
          const router = new Router({
            sources: sourceQueues.map((q) => q.name),
            targets: [targetQueue],
            opts: { connection },
          });
          const jobs = 10;

          router.run().then().catch(console.error);

          for (const queue of sourceQueues) {
            for (let i = 1; i <= jobs; i++) {
              await queue.add('default', { idx: i });
            }
          }

          while ((await targetQueue.count()) < jobs * sourceQueues.length) {
            await delay(50);
          }
          expect(await targetQueue.count()).toEqual(jobs * sourceQueues.length);
          expect(
            (await targetQueue.getWaiting())
              .map((job) => job.data.idx)
              .sort((a, b) => a - b),
          ).toEqual(
            Array.from(Array(jobs).keys())
              .map((i) => i + 1)
              .flatMap((i) => Array(sourceQueues.length).fill(i)),
          );
          await router.close();
        });
      });
    });

    describe('many->many', () => {
      describe('when jobs produced with an active router', () => {
        it('should route to defined queues', async () => {
          const sourceQueues = [
            new Queue(`test-${v4()}`, { connection }),
            new Queue(`test-${v4()}`, { connection }),
          ];
          const targetQueues = [
            new Queue(`test-${v4()}`, { connection }),
            new Queue(`test-${v4()}`, { connection }),
          ];
          const router = new Router({
            sources: sourceQueues.map((q) => q.name),
            targets: targetQueues,
            opts: { connection },
          });
          const jobs = 10;

          router.run().then().catch(console.error);

          for (const queue of sourceQueues) {
            for (let i = 1; i <= jobs; i++) {
              await queue.add('default', { idx: i });
            }
          }

          while (
            (await targetQueues[0].count()) < jobs * sourceQueues.length ||
            (await targetQueues[1].count()) < jobs * sourceQueues.length
          ) {
            await delay(50);
          }
          for (const queue of targetQueues) {
            expect(await queue.count()).toEqual(jobs * sourceQueues.length);
            expect(
              (await queue.getWaiting())
                .map((job) => job.data.idx)
                .sort((a, b) => a - b),
            ).toEqual(
              Array.from(Array(jobs).keys())
                .map((i) => i + 1)
                .flatMap((i) => Array(sourceQueues.length).fill(i)),
            );
          }
          await router.close();
        });
      });
    });
  });
});
