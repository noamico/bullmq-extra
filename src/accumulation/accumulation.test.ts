import { v4 } from 'uuid';
import { default as IORedis } from 'ioredis';
import { delay, Queue } from 'bullmq';
import { GenericContainer, Wait } from 'testcontainers';
import { Accumulation } from './accumulation';

jest.setTimeout(60000);

describe('accumulation', function () {
  const redisPort = 6380;
  let connection: IORedis;
  beforeAll(async function () {
    const redisContainerSetup = new GenericContainer('redis:7.4.0')
      .withExposedPorts(redisPort)
      .withEnvironment({ REDIS_PORT: redisPort.toString() })
      .withCommand(['redis-server', '--port', redisPort.toString()])
      .withWaitStrategy(
        Wait.forLogMessage(/.*Ready to accept connections tcp.*/, 1),
      );
    const redisContainer = await redisContainerSetup.start();
    const mappedPort = redisContainer.getMappedPort(redisPort);
    connection = new IORedis({
      port: mappedPort,
      maxRetriesPerRequest: null,
    });
  });

  describe('when completing within timeout', () => {
    it('should send complete result', async () => {
      const sourceQueueName = `test-${v4()}`;
      const sourceQueuePrefix = 'accumulation';
      const sourceQueue = new Queue(sourceQueueName, {
        connection,
        prefix: sourceQueuePrefix,
      });
      const accumulationName = `test-${v4()}`;
      const target = new Queue(`test-${v4()}`, {
        connection,
        prefix: 'different',
      });
      const source = {
        queue: sourceQueueName,
        getGroupKey: (data) => data.accumulationKey,
      };

      const accumulation = new Accumulation({
        accumulationName,
        onComplete: (data) => {
          const sum = data.reduce((acc, val) => {
            return acc + val.value;
          }, 0);
          return { sum };
        },
        isComplete: async (data) => {
          return data.length === 10;
        },
        opts: { connection },
        source: {
          queue: sourceQueueName,
          getGroupKey: source.getGroupKey,
          prefix: sourceQueuePrefix,
        },
        target,
        timeout: 10000,
      });
      accumulation.run();

      const jobs = 10;

      for (let i = 1; i <= jobs; i++) {
        await sourceQueue.add('test', { accumulationKey: 1, value: i });
      }

      while ((await target.count()) < 1) {
        await delay(50);
      }

      expect(await target.count()).toEqual(1);
      const waiting = (await target.getWaiting())[0];
      expect(waiting.data).toEqual({ sum: 55 });
    });
  });

  describe('when not completing within timeout', () => {
    it('should send partial result', async () => {
      const redisPrefix = 'another-one';
      const accumulationName = `test-${v4()}`;
      const target = new Queue(`test-${v4()}`, {
        connection,
        prefix: redisPrefix,
      });
      const source = {
        queue: new Queue(`test-${v4()}`, {
          connection,
          prefix: redisPrefix,
        }),
        getGroupKey: (data) => data.accumulationKey,
      };

      const accumulation = new Accumulation({
        accumulationName,
        onComplete: (data) => {
          const sum = data.reduce((acc, val) => {
            return acc + val.value;
          }, 0);
          return { sum };
        },
        opts: { connection },
        source: {
          queue: source.queue.name,
          getGroupKey: source.getGroupKey,
          prefix: redisPrefix,
        },
        target,
        timeout: 100,
      });
      accumulation.run();

      const jobs = 10;

      for (let i = 1; i <= jobs - 1; i++) {
        await source.queue.add('test', { accumulationKey: 1, value: i });
      }

      while ((await target.count()) < 1) {
        await delay(50);
      }

      expect(await target.count()).toEqual(1);
      const waiting = (await target.getWaiting())[0];
      expect(waiting.data).toEqual({ sum: 45 });
    });
  });
});
