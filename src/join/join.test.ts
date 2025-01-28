import { v4 } from 'uuid';
import { default as IORedis } from 'ioredis';
import { delay, Queue } from 'bullmq';
import { GenericContainer, Wait } from 'testcontainers';
import { Join } from './join';

jest.setTimeout(60000);

describe('join', function () {
  let connection: IORedis;
  beforeAll(async function () {
    const redisPort = 6379;
    const redisContainerSetup = new GenericContainer('redis:7.4.0')
      .withExposedPorts(redisPort)
      .withEnvironment({ REDIS_PORT: redisPort.toString() })
      .withCommand(['redis-server', '--port', redisPort.toString()])
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
      const joinName = `test-${v4()}`;
      const target = new Queue(`test-${v4()}`, {
        connection,
        prefix: `{${joinName}}`,
      });
      const sources = [
        {
          queue: new Queue(`test-${v4()}`, {
            connection,
            prefix: `{${joinName}}`,
          }),
          getJoinKey: (data) => data.joinKey,
        },
        {
          queue: new Queue(`test-${v4()}`, {
            connection,
            prefix: `{${joinName}}`,
          }),
          getJoinKey: (data) => data.joinKey,
        },
      ];

      const join = new Join({
        joinName,
        onComplete: (data) => {
          const sum = data.reduce((acc, val) => {
            return acc + val.val.value;
          }, 0);
          return { sum };
        },
        opts: { connection },
        sources: sources.map((source) => ({
          queue: source.queue.name,
          getJoinKey: source.getJoinKey,
        })),
        target,
        timeout: 10000,
      });
      join.run();

      const jobs = 10;

      for (let i = 1; i <= jobs; i++) {
        for (const source of sources) {
          await source.queue.add('test', { joinKey: i, value: i });
        }
      }

      while ((await target.count()) < jobs) {
        await delay(50);
      }

      expect(await target.count()).toEqual(jobs);
      const waiting = await target.getWaiting();
      expect(
        waiting.map((job) => job.data).sort((a, b) => a.sum - b.sum),
      ).toEqual(
        Array.from(Array(jobs).keys())
          .map((i) => ({
            sum: (i + 1) * 2,
          }))
          .sort((a, b) => a.sum - b.sum),
      );
    });

    it('should not send complete result (null key)', async () => {
      const joinName = `test-${v4()}`;
      const target = new Queue(`test-${v4()}`, {
        connection,
        prefix: `{${joinName}}`,
      });
      const sources = [
        {
          queue: new Queue(`test-${v4()}`, {
            connection,
            prefix: `{${joinName}}`,
          }),
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          getJoinKey: (data) => undefined,
        },
        {
          queue: new Queue(`test-${v4()}`, {
            connection,
            prefix: `{${joinName}}`,
          }),
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          getJoinKey: (data) => undefined,
        },
      ];

      const timeoutPeriod = 10000;
      const join = new Join({
        joinName,
        onComplete: (data) => {
          const sum = data.reduce((acc, val) => {
            return acc + val.val.value;
          }, 0);
          return { sum };
        },
        opts: { connection },
        sources: sources.map((source) => ({
          queue: source.queue.name,
          getJoinKey: source.getJoinKey,
        })),
        target,
        timeout: timeoutPeriod,
      });
      join.run();

      const jobs = 10;

      for (let i = 1; i <= jobs; i++) {
        for (const source of sources) {
          await source.queue.add('test', { joinKey: i, value: i });
        }
      }

      // give enough to make sure it is not processing anything
      await delay(timeoutPeriod * 2 + 100);

      expect(await target.count()).toEqual(0);
    });
  });

  describe('when not completing within timeout', () => {
    it('should send partial result', async () => {
      const joinName = `test-${v4()}`;
      const target = new Queue(`test-${v4()}`, {
        connection,
      });
      const sources = [
        {
          queue: new Queue(`test-${v4()}`, {
            connection,
            prefix: `{${joinName}}`,
          }),
          getJoinKey: (data) => data.joinKey,
        },
        {
          queue: new Queue(`test-${v4()}`, {
            connection,
            prefix: `{${joinName}}`,
          }),
          getJoinKey: (data) => data.joinKey,
        },
      ];

      const join = new Join({
        joinName,
        onComplete: (data) => {
          const sum = data.reduce((acc, val) => {
            return acc + val.val.value;
          }, 0);
          return { sum };
        },
        opts: { connection },
        sources: sources.map((source) => ({
          queue: source.queue.name,
          getJoinKey: source.getJoinKey,
        })),
        target,
        timeout: 100,
      });
      join.run();

      const jobs = 10;

      for (let i = 1; i <= jobs; i++) {
        await sources[0].queue.add('test', { joinKey: i, value: i });
      }

      while ((await target.count()) < jobs) {
        await delay(50);
      }

      expect(await target.count()).toEqual(jobs);
      const waiting = await target.getWaiting();
      expect(
        waiting.map((job) => job.data).sort((a, b) => a.sum - b.sum),
      ).toEqual(
        Array.from(Array(jobs).keys())
          .map((i) => ({
            sum: i + 1,
          }))
          .sort((a, b) => a.sum - b.sum),
      );
    });
  });

  describe('when only 1 source', () => {
    it('should send complete result', async () => {
      const joinName = `test-${v4()}`;
      const target = new Queue(`test-${v4()}`, {
        connection,
      });
      const sources = [
        {
          queue: new Queue(`test-${v4()}`, {
            connection,
            prefix: `{${joinName}}`,
          }),
          getJoinKey: (data) => data.joinKey,
        },
      ];

      const join = new Join({
        joinName,
        onComplete: (data) => {
          const sum = data.reduce((acc, val) => {
            return acc + val.val.value;
          }, 0);
          return { sum };
        },
        opts: { connection },
        sources: sources.map((source) => ({
          queue: source.queue.name,
          getJoinKey: source.getJoinKey,
        })),
        target,
        timeout: 10000,
      });
      join.run();

      const jobs = 10;

      for (let i = 1; i <= jobs; i++) {
        for (const source of sources) {
          await source.queue.add('test', { joinKey: i, value: i });
        }
      }

      while ((await target.count()) < jobs) {
        await delay(50);
      }

      expect(await target.count()).toEqual(jobs);
      const waiting = await target.getWaiting();
      expect(
        waiting.map((job) => job.data).sort((a, b) => a.sum - b.sum),
      ).toEqual(
        Array.from(Array(jobs).keys())
          .map((i) => ({
            sum: i + 1,
          }))
          .sort((a, b) => a.sum - b.sum),
      );
    });
  });
});
