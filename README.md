# BullMQ Extra

BullMQ Extra is a set of additional features and extensions for the much beloved [BullMQ](https://www.npmjs.com/package/bullmq). 
The library currently provides specialized patterns like Routing and Joining, with more useful features to come which are currently not available in the core BullMQ library.

## Installation:

```bash
npm install bullmq-extra
```

# Features
- [Router](#router)
- [Join](#join)
- [Accumulation](#accumulation)
- [Request-Response](#request-response)

## Router:
`Routers` allow you to distribute jobs from one or more source queues to one or more target queues. This is useful for implementing:
- fan-out (1->N) patterns, where a single job is processed by multiple workers in parallel.
- fan-in (N->1) patterns, where multiple job queues are combined and processed by a single worker.
- fan-in to fan-out (N->N) patterns, where multiple job queues are combined and processed by multiple workers.

Under the hood the `Router` component leverages `Redis Streams` so you basically get the same publish-subscribe capability as in Kafka, 
including retention, consumer groups and message replay,
but at a fraction of the complexity. And the additional useful patterns mentioned above.
Also, as everything ends up in a BullMQ queue, you can use all the features of BullMQ like retries, priorities, etc.
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
const targetQueue3 = new Queue('target3');
const targetQueue4 = new Queue('target4');
const router2 = new Router()
  .addSources('source1')
  .addTargets(targetQueue3,targetQueue4);
router2.run().then().catch();

// Add jobs to the source queues
sourceQueue1.add('job', { data: 'data1' });
sourceQueue2.add('job', { data: 'data2' });

// The jobs will be delivered to all the target queues
```

### Advanced Options (`RouterOptions`):

- **batchSize:** The number of jobs to process in a single batch. Default is 1.

- **blockTimeMs:** The time to wait before polling after an empty batch. Default is 1 second.

- **maxRetentionMs:** The maximum time to retain a job in the router stream. Default is 24 hours.

- **trimIntervalMs:** The interval in which to trim the router stream. Default is 1 minute.

- **optsOverride:** A function that takes the job data and returns an object with options to override the default options for the job.

### Caution:
- Beware of circular dependencies when using routers. This can lead to infinite loops which will overload your Redis.

## Join:
`Joins` allows you to combine jobs from multiple queues into a single queue while joining them on a common key. 
This is a common pattern in ETL and data processing pipelines and is now available for BullMQ users as well.

### Basic Usage:

```typescript
import { Queue } from 'bullmq';
import { Join } from 'bullmq-extra';

const join = new Join({
  joinName: 'join1',
  onComplete: (data) => {
    const sum = data.reduce((acc, val) => {
      return acc + val.value;
    }, 0);
    return { sum };
  },
  redis: new IORedis(),
  sources: [
    {
      queue: 'source1',
      getJoinKey: (data) => data.joinKey,
    },
    {
      queue: 'source2',
      getJoinKey: (data) => data.joinKey,
    },
  ],
  target: new Queue('target1'),
  timeout: 1000,
});
join.run();

// Add jobs to the source queues
sourceQueue1.add('job', { joinKey: 'key1', value: 1 });
sourceQueue2.add('job', { joinKey: 'key1', value: 2 });

// The result of the onComplete function will be added to the target queue

```
## Accumulation:
`Accumulations` allow you to accumulate messages from a queue and output aggregations to a new queue.

### Basic Usage:

```typescript
import { Queue } from 'bullmq';
import { Accumulation } from 'bullmq-extra';

const accumulation = new Accumulation({
  accumulationName: 'accumulation1',
  onComplete: (data) => {
    const sum = data.reduce((acc, val) => {
      return acc + val.value;
    }, 0);
    return { sum };
  },
  opts: { connection },
  source: {
    queue: 'source1',
    getGroupKey: (data) => data.groupKey, 
  },
  target: new Queue('target1'),
  timeout: 1000,
  expectedItems: 2,
});
accumulation.run();

// Add jobs to the source queue
sourceQueue1.add('job', { groupKey: 'key1', value: 1 });
sourceQueue1.add('job', { groupKey: 'key1', value: 2 });

// The result of the onComplete function will be added to the target queue
```

## Request-Response:
`Requesters` and `Responders` allow you to create a request-response pattern with BullMQ.
The `Requester` sends a job to a queue and waits for a response on a dedicated response queue.
The `Responder` listens for requests on its own queue and sends each response to the appropriate response queue.

### Basic Usage:

```typescript
import { Requester, Responder } from 'bullmq-extra';

// Somewhere in you application create a responder
const responder = new Responder({
  responderName: 'additionJob',
  opts: { connection },
});
responder.processRequests((data: { a: number, b: number }) => {
  return { result: data.a + data.b };
});

// Create a requester or several in other places in your application
const requester1 = new Requester({
  requesterName: 'additionJob1',
  responderName: 'additionJob',
  opts: { connection },
});
requester1.request({ a: 1, b: 2 });
requester1.processResponses((data) => {
  console.log(data); // {result: 3}
});

const requester2 = new Requester({
  requesterName: 'additionJob2',
  responderName: 'additionJob',
  opts: { connection },
});
requester2.request({ a: 1, b: 5 });
requester2.processResponses((data) => {
  console.log(data); // {result: 6}
});
```

## Caution:
 - The package is new so breaking changes are to be expected until version 1.0.0.

## Roadmap:
 - **BullMQ Connect:** Similiar to Kafka Connect, a way to connect BullMQ to other systems. Will probably be a separate package or several.
 - **BullMQ Broker Sidecar:** A broker designed to run as a sidecar that wraps around BullMQ and exposes a thin API for producing and consuming. On top of it several thin clients will be available in various languages like Java and Go to bring the power of BullMQ to those languages and allow integrating BullMQ into legacy codebases. In the farther future there is even the possibility of these brokers supporting the Kafka protocol and acting as drop-in Kafka replacements.

## Contributing:
 - Feel free to open issues for questions, suggestions and feedback. And Issues...
 - To contribute code just fork and open pull requests.

 ## Thanks! 🚀
