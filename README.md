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
import { source } from '@angular-devkit/schematics';

// Create a source queue somewhere in your application
const sourceQueue = new Queue('queue1');

// Create a fanout to process jobs from the source queue and distribute them to target queues
const targetQueue1 = new Queue('queue2');
const targetQueue2 = new Queue('queue3');
const fanout1 = new Fanout(sourceQueue, [targetQueue1, targetQueue2],'fanout-group1');
fanout1.run().then().catch();

// Create multiple fanouts on the same source queue
const targetQueue3 = new Queue('queue4');
const targetQueue4 = new Queue('queue5');
const fanout2 = new Fanout(sourceQueue, [targetQueue3, targetQueue4],'fanout-group2');
fanout2.run().then().catch();
```

### Advanced Options (`FanoutOptions`):

**optsOverride:** A function that takes the job data and returns an object with options to override the default options for the job.


