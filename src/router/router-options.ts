import { ConnectionOptions, JobsOptions, QueueBaseOptions } from 'bullmq';

/**
 * Base Consumer options
 */
export interface ConsumerBaseOptions {
  /**
   * Options for connecting to a Redis instance.
   */
  connection: ConnectionOptions;

  /**
   * Denotes commands should retry indefinitely.
   */
  blockingConnection?: boolean;

  /**
   * Prefix for all queue keys.
   */
  prefix?: string;

  /**
   * Avoid version validation to be greater or equal than v5.0.0.
   * @defaultValue false
   */
  skipVersionCheck?: boolean;
}

/**
 * Options for the Consumer class.
 */
export interface RouterOptions extends QueueBaseOptions {
  /**
   * Skip Meta update.
   *
   * If true, the queue will not update the metadata of the queue.
   * Useful for read-only systems that do should not update the metadata.
   *
   * @defaultValue false
   */
  skipMetasUpdate?: boolean;

  batchSize?: number;

  blockTimeMs?: number;

  maxRetentionMs?: number;

  trimIntervalMs?: number;

  optsOverride?: (data: any) => JobsOptions;
}
