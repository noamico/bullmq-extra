import { v4 } from 'uuid';
import { default as IORedis } from 'ioredis';
import { delay } from 'bullmq';
import { GenericContainer, Wait } from 'testcontainers';
import { Requester } from './requester';
import { Responder } from './responder';

jest.setTimeout(60000);

describe('reqrep', function () {
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

  describe('when requesting', () => {
    it('should reply', async () => {
      const requesterName = `test-${v4()}`;
      const responderName = `test-${v4()}`;

      const responder = new Responder({
        opts: { connection },
        responderName,
      });
      const requester = new Requester({
        opts: { connection },
        requesterName,
        responderName,
      });

      responder.processRequests(async (data) => {
        const { value } = data;
        return { value: value + 1 };
      });

      const responses: { value: number }[] = [];

      requester.processResponses(async (data) => {
        responses.push(data);
      });

      for (let i = 0; i < 10; i++) {
        await requester.request({ value: i });
      }

      while (responses.length < 10) {
        await delay(50);
      }

      expect(responses).toEqual(
        Array.from(Array(10).keys()).map((i) => ({
          value: i + 1,
        })),
      );
    });
  });
});
