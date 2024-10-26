# BullMQ Extra

BullMQ Extra is a set of additional features and extensions for BullMQ, designed to enhance message queue handling in Node.js. The library provides specialized patterns like Pubsub, enabling advanced distribution of jobs across multiple queues.

## Installation:

```bash
npm install bullmq-extra
```

# Features

## Pubsub: 
Broadcast a job to multiple queues, ideal for implementing publish-subscribe models or parallel processing workflows.
Because under the hood the `PubSub` component relies on `Redis Streams` you basically get the same publish-subscribe capability as in Kafka, including retention and message replay, 
but at a fraction of the complexity. The cost/performance ratio is yet to be benchmarked.

### Basic Usage:

```typescript
import { Queue, Worker } from 'bullmq';
import { Pubsub } from 'bullmq-extra';

// Create a source queue somewhere in your application
const sourceQueue = new Queue('queue1');

// Create a pubsub to process jobs from the source queue and distribute them to target queues
const targetQueue1 = new Queue('queue2');
const targetQueue2 = new Queue('queue3');
const pubsub1 = new Pubsub()
  .setSource('queue1')
  .addTargets(targetQueue1,targetQueue2);

pubsub1.run().then().catch();

// Create multiple pubsubs on the same source queue
const targetQueue3 = new Queue('queue4');
const targetQueue4 = new Queue('queue5');
const pubsub2 = new Pubsub()
  .setSource('queue1')
  .addTargets(targetQueue3,targetQueue4);
pubsub2.run().then().catch();
```

### Advanced Options (`PubsubOptions`):

- **batchSize:** The number of jobs to process in a single batch. Default is 1.

- **blockTimeMs:** The time to wait before polling after an empty batch. Default is 1 second.

- **maxRetentionMs:** The maximum time to retain a job in the pubsub stream. Default is 24 hours.

- **trimIntervalMs:** The interval in which to trim the pubsub stream. Default is 1 minute.

- **optsOverride:** A function that takes the job data and returns an object with options to override the default options for the job.

### Caution:
 - Do not use the same queue as both a source and target queue in a pubsub. This can lead to infinite loops.
 - Do not use the same queue as a target queue in multiple pubsubs unless you really intend to. This can lead to job duplication.
 - The package is new so breaking changes are to be expected until version 1.0.0.

## Roadmap:
 - **Joins:** Create joins between queues and output the result to a new queue.
 - **Aggregations:** Accumulate messages from a queue and output aggregations to a new queue.
 - **BullMQ Connect:** Similiar to Kafka Connect, a way to connect BullMQ to other systems. Will probably be a separate package or several.

## Contributing:
 - Feel free to open issues or fork and open pull requests. We are open to suggestions and improvements.
 - We are looking for maintainers to help us with the project.
