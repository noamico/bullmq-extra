import { v4 } from 'uuid';
import { default as IORedis } from 'ioredis';
import { delay, Queue } from 'bullmq';
import { GenericContainer, Wait } from 'testcontainers';
import { Accumulation } from './accumulation';

jest.setTimeout(60000);

describe('accumulation', function () {
  let connection: IORedis;
  beforeAll(async function () {
    const redisContainerSetup = new GenericContainer('redis:7.4.0')
      .withExposedPorts(6379)
      .withWaitStrategy(
        Wait.forLogMessage(/.*Ready to accept connections tcp.*/, 1),
      );
    const redisContainer = await redisContainerSetup.start();
    const mappedPort = redisContainer.getMappedPort(6379);
    connection = new IORedis({
      port: mappedPort,
      maxRetriesPerRequest: null,
    });
  });

  describe('when completing within timeout', () => {
    it('should send complete result', async () => {
      const accumulationName = `test-${v4()}`;
      const target = new Queue(`test-${v4()}`, {
        connection,
      });
      const source = {
        queue: new Queue(`test-${v4()}`, {
          connection,
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
        isComplete: async (data) => {
          return data.length === 10;
        },
        opts: { connection },
        source: {
          queue: source.queue.name,
          getGroupKey: source.getGroupKey,
        },
        target,
        timeout: 10000,
      });
      accumulation.run();

      const jobs = 10;

      for (let i = 1; i <= jobs; i++) {
        await source.queue.add('test', { accumulationKey: 1, value: i });
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
      const accumulationName = `test-${v4()}`;
      const target = new Queue(`test-${v4()}`, {
        connection,
      });
      const source = {
        queue: new Queue(`test-${v4()}`, {
          connection,
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
