import { ConnectionOptions } from 'bullmq';
import * as IORedis from 'ioredis';

export class GetRedisInstance {
  public static getIORedisInstance(
    connection: ConnectionOptions,
  ): IORedis.Redis | IORedis.Cluster {
    if (
      connection instanceof IORedis.Redis ||
      connection instanceof IORedis.Cluster
    ) {
      return connection;
    } else if (Array.isArray(connection)) {
      return new IORedis.Cluster(connection);
    } else {
      return new IORedis.Redis(connection);
    }
  }
}
