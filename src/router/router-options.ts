import { JobsOptions, QueueBaseOptions } from 'bullmq';

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
