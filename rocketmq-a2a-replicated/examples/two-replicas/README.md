# Two-Replica LiteTopic Demo

Runnable demo verifying `LiteTopicReplicationStrategy` against a real A2A SDK stack and a real
RocketMQ LiteTopic deployment. It simulates two A2A server replicas (A and B) inside one JVM: each
replica holds its own consumer group, task store, event bus and `ReplicatedQueueManager`; the only
shared state is the LiteTopic channel on the broker.

## Scenario

1. Replica B handles `task-1` and enqueues a status event; a client opening the task stream on B
   (`LiteTopicTaskStreamLifecycleHook.onSubscribe`) subscribes channel `t-task-1`.
2. Replica A enqueues a non-final status event for the same task — A does not subscribe (no client
   on A); the broker delivers once per group, so B's mirror queue receives it (A drops its own copy).
3. Replica A sends the poison pill (`QueueClosedEvent`, mirroring the SDK's `onTaskFinalized`
   path) — B receives it and unsubscribes.

Prints `=== DEMO RESULT: PASS ===` when all three steps succeed.

Note: final task states (completed/failed/...) skip the SDK enqueue hook; they are replicated via
`ReplicatedQueueManager.onTaskFinalized`. The demo therefore uses a non-final state for step 2 and
a direct `strategy.send` for step 3.

## Prerequisites

- A RocketMQ LiteTopic deployment with a parent topic and two consumer groups pre-created:

  | Resource | Value | Notes |
  | --- | --- | --- |
  | Topic | `a2a-parent-topic` | LiteTopic parent topic, 1 queue |
  | Group | `a2a-replication-group-0` | MessageModel `LITE_SELECTIVE` |
  | Group | `a2a-replication-group-1` | MessageModel `LITE_SELECTIVE` |

- Build and install the main module first:

```bash
mvn -B clean install -f ../../pom.xml
```

## Run

```bash
mvn -B compile dependency:build-classpath -Dmdep.outputFile=/tmp/demo-cp.txt -Dmdep.includeScope=runtime
java -cp "target/classes:$(cat /tmp/demo-cp.txt)" \
    org.apache.rocketmq.a2a.replication.litetopic.TwoReplicaDemo \
    <endpoints> <parentTopic> <accessKey> <secretKey>
```

Arguments: gRPC endpoints (e.g. `your-broker-host:8080`), parent topic name, access key and
secret key (omit the last two for anonymous access).
