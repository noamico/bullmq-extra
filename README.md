# BullMQ Extra

BullMQ Extra is a set of additional features and extensions for BullMQ, designed to enhance message queue handling in Node.js. The library provides specialized patterns like Router, enabling advanced distribution of jobs across multiple queues.

## Installation:

```bash
npm install bullmq-extra
```

# Features

## Router: 
Routers allow you to distribute jobs from one or more source queues to one or more target queues. This is useful for implementing:
- fan-out (1->N) patterns, where a single job is processed by multiple workers in parallel.
- fan-in (N->1) patterns, where multiple job queues are combined and processed by a single worker.
- fan-in to fan-out (N->N) patterns, where multiple job queues are combined and processed by multiple workers.

Under the hood the `Router` component leverages `Redis Streams` so you basically get the same publish-subscribe capability as in Kafka, 
including retention, consumer groups and message replay,
but at a fraction of the complexity. And the additional useful patterns mentioned above.
The cost/performance ratio is yet to be benchmarked.

### Basic Usage:

```typescript
import { Queue, Worker } from 'bullmq';
import { Router } from 'bullmq-extra';

// Create source queues somewhere in your application
const sourceQueue1 = new Queue('source1');
const sourceQueue2 = new Queue('source2');

// Create a router to process jobs from the source queue and distribute them to target queues
const targetQueue1 = new Queue('target1');
const targetQueue2 = new Queue('target2');
const router1 = new Router()
  .addSources('source1','source2')
  .addTargets(targetQueue1,targetQueue2);

router1.run().then().catch();

// Create multiple routers on the same source queues to different targets
const targetQueue3 = new Queue('queue4');
const targetQueue4 = new Queue('queue5');
const router2 = new Router()
  .addSources('source1')
  .addTargets(targetQueue3,targetQueue4);
router2.run().then().catch();
```

### Advanced Options (`RouterOptions`):

- **batchSize:** The number of jobs to process in a single batch. Default is 1.

- **blockTimeMs:** The time to wait before polling after an empty batch. Default is 1 second.

- **maxRetentionMs:** The maximum time to retain a job in the router stream. Default is 24 hours.

- **trimIntervalMs:** The interval in which to trim the router stream. Default is 1 minute.

- **optsOverride:** A function that takes the job data and returns an object with options to override the default options for the job.

### Caution:
 - Beware of circular dependencies when using routers. This can lead to infinite loops which will overload your Redis.
 - The package is new so breaking changes are to be expected until version 1.0.0.

## Roadmap:
 - **Joins:** Create joins between queues and output the result to a new queue.
 - **Aggregations:** Accumulate messages from a queue and output aggregations to a new queue.
 - **BullMQ Connect:** Similiar to Kafka Connect, a way to connect BullMQ to other systems. Will probably be a separate package or several.

## Contributing:
 - Feel free to open issues for questions, suggestions and feedback.
 - To contribute code just fork and open pull requests.

 ## Thanks! 🚀
