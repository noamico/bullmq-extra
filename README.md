# BullMQ Extra

BullMQ Extra is a set of additional features and extensions for BullMQ, designed to enhance message queue handling in Node.js. The library provides specialized patterns like Fanout, enabling advanced distribution of jobs across multiple queues.

## Installation:

```bash
npm install bullmq-extra
```

# Features

## Fanout: 
Broadcast a job to multiple queues, ideal for implementing publish-subscribe models or parallel processing workflows.

### Basic Usage:

```typescript
import { Queue, Worker } from 'bullmq';
import { Fanout } from 'bullmq-extra';

// Create a source queue somewhere in your application
const sourceQueue = new Queue('queue1');

// Create a fanout to process jobs from the source queue and distribute them to target queues
const targetQueue1 = new Queue('queue2');
const targetQueue2 = new Queue('queue3');
const fanout1 = new Fanout()
  .setSource('queue1')
  .addTargets(targetQueue1,targetQueue2);

fanout1.run().then().catch();

// Create multiple fanouts on the same source queue
const targetQueue3 = new Queue('queue4');
const targetQueue4 = new Queue('queue5');
const fanout2 = new Fanout()
  .setSource('queue1')
  .addTargets(targetQueue3,targetQueue4);
fanout2.run().then().catch();
```

### Advanced Options (`FanoutOptions`):

**batchSize:** The number of jobs to process in a single batch. Default is 1.

**blockTimeMs:** The time to wait before polling after an empty batch. Default is 1 second.

**maxRetentionMs:** The maximum time to retain a job in the fanout stream. Default is 24 hours.

**trimIntervalMs:** The interval in which to trim the fanout stream. Default is 1 minute.

**optsOverride:** A function that takes the job data and returns an object with options to override the default options for the job.

### Caution:
 - Do not use the same queue as both a source and target queue in a fanout. This can lead to infinite loops.
 - Do not use the same queue as a target queue in multiple fanouts unless you really intend to. This can lead to job duplication.
