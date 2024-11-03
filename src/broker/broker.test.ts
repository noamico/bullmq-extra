import { default as IORedis } from 'ioredis';
import { delay } from 'bullmq';
import { GenericContainer, Wait } from 'testcontainers';
import { MockServer } from './tests/mock-server';
import { Broker } from './broker';
import axios from 'axios';
import { v4 } from 'uuid';

jest.setTimeout(60000);

describe('broker', function () {
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

  describe('when starting a worker and producing jobs', () => {
    it('should process jobs', async () => {
      const name = `test-${v4()}`;
      const results = [];
      const mockServer = new MockServer();
      mockServer
        .start(3000, async (a: number, b: number) => {
          results.push(a + b);
        })
        .then();

      const broker = new Broker({ connection });
      await broker.start(3001);

      await axios.post('http://localhost:3001/queue', {
        name,
        opts: {},
      });

      await axios.post('http://localhost:3001/worker', {
        name,
        callback: 'http://localhost:3000/job',
        opts: {},
      });

      const jobs = 10;

      for (let i = 1; i <= jobs; i++) {
        await axios.post('http://localhost:3001/job', {
          name,
          data: { a: i, b: i },
          opts: {},
        });
      }

      while (results.length < jobs) {
        await delay(50);
      }

      expect(results).toEqual(
        Array.from(Array(jobs).keys()).map((i) => i + 1 + (i + 1)),
      );

      await broker.stop();
      await mockServer.stop();
    });
  });

  describe('when some jobs fail', () => {
    it('should process other jobs', async () => {
      const name = `test-${v4()}`;
      const results = [];
      const mockServer = new MockServer();
      mockServer
        .start(3002, async (a: number, b: number) => {
          if (a < 4) throw new Error('a < 4');
          results.push(a + b);
        })
        .then();

      const broker = new Broker({ connection });
      await broker.start(3003);

      await axios.post('http://localhost:3003/queue', {
        name,
        opts: {},
      });

      await axios.post('http://localhost:3003/worker', {
        name,
        callback: 'http://localhost:3002/job',
        opts: {},
      });

      const jobs = 10;

      for (let i = 1; i <= jobs; i++) {
        await axios.post('http://localhost:3003/job', {
          name,
          data: { a: i, b: i },
          opts: {},
        });
      }

      while (results.length < jobs - 3) {
        await delay(50);
      }

      expect(results).toEqual(
        Array.from(Array(jobs - 3).keys()).map((i) => i + 4 + (i + 4)),
      );

      await broker.stop();
      await mockServer.stop();
    });
  });

  // TODO: test router
});
