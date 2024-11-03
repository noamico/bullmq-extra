import { Broker } from './broker';

(async () => {
  const broker = new Broker({
    connection: { host: process.env.REDIS_HOST, port: +process.env.REDIS_PORT },
  });
  await broker.start(+process.env.BROKER_PORT);
})();
