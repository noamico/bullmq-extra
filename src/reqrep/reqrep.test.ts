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

  describe('when requesting from multiple requesters', () => {
    it('should reply separately to each', async () => {
      const requesterName1 = `test-${v4()}`;
      const requesterName2 = `test-${v4()}`;
      const responderName = `test-${v4()}`;

      const responder = new Responder({
        opts: { connection },
        responderName,
      });
      const requester1 = new Requester({
        opts: { connection },
        requesterName: requesterName1,
        responderName,
      });
      const requester2 = new Requester({
        opts: { connection },
        requesterName: requesterName2,
        responderName,
      });

      responder.processRequests(async (data) => {
        const { value } = data;
        return { value: value + 1 };
      });

      const responses1: { value: number }[] = [];
      const responses2: { value: number }[] = [];

      requester1.processResponses(async (data) => {
        responses1.push(data);
      });

      requester2.processResponses(async (data) => {
        responses2.push(data);
      });

      for (let i = 0; i < 10; i++) {
        await requester1.request({ value: i });
        await requester2.request({ value: i * 20 });
      }

      while (responses1.length < 10 || responses2.length < 10) {
        await delay(50);
      }

      expect(responses1).toEqual(
        Array.from(Array(10).keys()).map((i) => ({
          value: i + 1,
        })),
      );

      expect(responses2).toEqual(
        Array.from(Array(10).keys()).map((i) => ({
          value: i * 20 + 1,
        })),
      );
    });
  });
});
