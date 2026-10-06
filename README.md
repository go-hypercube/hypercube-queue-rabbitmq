# Hypercube RabbitMQ Queue

A RabbitMQ implementation of the [go-hypercube queue contract](https://pkg.go.dev/github.com/go-hypercube/go-hypercube/queue), using [`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go).

It implements:

- the core `queue.Queue` interface
- the optional `queue.RedriveProvider` interface
- a concrete `Close` method for outstanding delivery channels

It intentionally does **not** implement `queue.VisibilityExtender` or `queue.StatsProvider`.

> **Important operational limitations**
>
> - Every message ID permanently creates and retains one durable classic single-slot reservation queue. IDs can never be reused within the same RabbitMQ virtual host and name prefix, even after `Ack`. These tombstones grow without bound, and an interrupted `Push` can leave a reservation without a work message.
> - Every distinct positive delay in milliseconds creates a durable quorum delay queue for that logical queue. Delay queues are not removed after they drain, so unbounded delay values cause unbounded topology growth.
> - RabbitMQ has no per-message visibility deadline for an unacknowledged delivery. `VisibilityTimeout` is preserved but not enforced, and visibility cannot be extended.
> - `Retry`, `DeadLetter`, and `Redrive` require a confirmed publish followed by an acknowledgement of the source delivery. Those steps are deliberately ordered to favor at-least-once safety, but they are not one atomic transaction and can leave duplicates in failure or lost-response windows.
> - Driver-owned topology must not be deleted, purged, redeclared, consumed from, published to, or otherwise changed externally. External topology or policy mutation is unsupported and can cause loss, duplication, stale tokens, or permanent ID inconsistencies.
>
> This driver is at-least-once, not exactly-once. Handlers must be idempotent, and callers must treat connection loss, context cancellation, and other ambiguous broker failures as unknown outcomes.

## Installation

```sh
go get github.com/go-hypercube/hypercube-queue-rabbitmq
```

## Requirements

- A RabbitMQ release that supports **at-least-once dead lettering for quorum queues**.
- The `stream_queue` feature flag enabled when required by the RabbitMQ release and cluster upgrade state.
- A connection whose user has `configure`, `write`/publish, and `read`/consume permissions for all driver-created resources in the selected virtual host.
- Enough broker capacity for durable work, dead-letter, delay, and per-ID reservation queues. The reservation and delay topology described above is intentionally persistent.

The driver declares its own topology. It is not compatible with externally predeclared resources whose queue types or arguments differ from the driver's declarations.

## Usage

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/go-hypercube/go-hypercube/queue"
    rabbitmqqueue "github.com/go-hypercube/hypercube-queue-rabbitmq"
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    q := rabbitmqqueue.New(conn,
        rabbitmqqueue.WithNamePrefix("myapp:queue"),
        rabbitmqqueue.WithPollingTimeout(2*time.Second),
        rabbitmqqueue.WithPollingInterval(25*time.Millisecond),
    )
    // Close the queue before closing the caller-owned AMQP connection.
    defer q.Close()

    ctx := context.Background()
    msg := &queue.Message{
        QueueName: "emails",
        Namespace: "app",
        JobName:   "send-welcome-email",
        Payload:   []byte(`{"user_id":42}`),
    }
    if err := q.Push(ctx, msg); err != nil {
        log.Fatal(err)
    }

    deliveries, err := q.Pop(ctx, "emails", 10)
    if errors.Is(err, queue.ErrEmpty) {
        return
    }
    if err != nil {
        log.Fatal(err)
    }

    for _, delivery := range deliveries {
        // Process delivery.Payload. The handler must be idempotent.
        if err := q.Ack(ctx, delivery); err != nil {
            log.Fatal(err)
        }
    }
}
```

`New` accepts an existing `*amqp.Connection` and zero or more options:

```go
func New(conn *amqp.Connection, options ...Option) *Queue
```

The caller owns the connection. `Queue.Close` closes outstanding delivery channels and invalidates their delivery tokens, but it does not close the supplied connection. Stop workers, settle or intentionally abandon outstanding deliveries, close the queue, and then close the connection.

## Configuration

| Option | Default | Behavior |
| --- | --- | --- |
| `WithNamePrefix` | `hypercube:queue` | Namespaces all driver-owned RabbitMQ resources. Message-ID uniqueness and permanent reservations are scoped to the virtual host plus this prefix. |
| `WithPollingTimeout` | `1s` | Maximum time an empty `Pop` polls before returning `queue.ErrEmpty`. |
| `WithPollingInterval` | `25ms` | Delay between empty `Basic.Get` polls. |

Workers that are intended to share queues must use the same RabbitMQ virtual host and name prefix. Use a distinct prefix or virtual host for isolated workloads.

## Topology and logical names

Logical queue names are application values and are not used directly as RabbitMQ queue names. The driver maps arbitrary logical queue names to SHA-256-based physical names under the configured prefix. Physical names and declaration details are driver-owned implementation details, not a public integration API.

For each logical queue, the driver uses:

- a durable quorum work queue with `x-delivery-limit=-1`
- a durable quorum dead-letter queue with `x-delivery-limit=-1`
- durable quorum delay queues as needed for delayed `Push` and `Retry`

Setting `x-delivery-limit=-1` prevents RabbitMQ's quorum delivery limit from becoming an application retry policy. The caller remains responsible for inspecting `Message.Attempt` and choosing `Ack`, `Retry`, or `DeadLetter`.

### Delays

A positive `Message.Delay` on `Push` or `Retry` is rounded **up** to a whole millisecond. Each `(logical queue, rounded delay in milliseconds)` pair has its own durable quorum delay queue, configured with `x-message-ttl` and at-least-once dead lettering into the work queue.

RabbitMQ accepts queue TTLs as non-negative AMQP integers, including long-long integers, so the driver supports the full positive Go `time.Duration` range. Broker load, recovery, and availability can make a message arrive later than its requested delay; delay is scheduling behavior, not a real-time deadline.

Delay queues remain declared after their messages drain. Reusing a bounded set of delay values bounds this part of the topology; generating unbounded distinct millisecond values does not.

## Delivery, channels, and visibility

`Pop` polls with AMQP `Basic.Get`. A successful returned batch keeps one AMQP channel open for that batch until every delivery from the batch has been settled. Consequently:

- callers should settle every returned delivery promptly
- one unsettled delivery keeps the batch channel alive
- many concurrently outstanding batches can consume many RabbitMQ channels
- closing the channel or queue requeues unsettled deliveries and invalidates every affected delivery token

Settlement is delivery-specific. Preserve `Message.DriverData` and all other protected fields exactly as returned by `Pop`; an ID alone is not sufficient to acknowledge, retry, or dead-letter a delivery.

RabbitMQ does not provide a per-message visibility timeout for an unacknowledged AMQP delivery. The driver serializes and preserves `Message.VisibilityTimeout` so message state survives retry, dead-lettering, and redrive, but it does **not** schedule redelivery from that value. An unacknowledged delivery normally remains on its channel until it is settled or the channel/connection closes. There is therefore no `ExtendVisibility` method and no `queue.VisibilityExtender` capability.

Channel or connection closure causes RabbitMQ to requeue unacknowledged deliveries, subject to broker availability. A worker can finish its side effect and lose the channel before its acknowledgement is known to have succeeded, so duplicate processing remains possible.

## Attempt counting

`Message.Attempt` is derived from RabbitMQ delivery metadata:

- RabbitMQ 4.3 and later use `x-acquired-count` when available.
- Older or fallback paths use `x-delivery-count`.

The first acquisition is presented as attempt 1. These headers are broker-maintained delivery metadata, not a globally transactional application audit log. Use attempts to drive application retry policy, but do not use them as proof that a side effect ran exactly a particular number of times across broker, connection, or republish failure windows.

## Retry, dead-lettering, and redrive

RabbitMQ messages are immutable after publication, while the queue contract allows a caller to update `Payload`, `Extra`, and `Delay` before settlement. The driver therefore cannot implement these transitions by merely rejecting or negatively acknowledging the original message.

For `Retry` and `DeadLetter`, the driver:

1. publishes the updated message to the appropriate active/delay or dead-letter queue
2. waits for publisher confirmation
3. acknowledges the original work delivery

For `Redrive`, it similarly confirms publication to the active queue before acknowledging the source dead-letter delivery. `Redrive` preserves the logical message ID and caller-controlled state, resets `Attempt` and `Delay`, and makes the replacement eligible for normal delivery.

These publish-then-ack sequences avoid acknowledging the only known copy before the replacement is confirmed, but they are not failure-atomic. For example, the replacement may be confirmed while the source acknowledgement is lost or never observed, leaving both copies available. A failed or canceled call can therefore have partially taken effect; retry and reconciliation logic must use IDs and tolerate duplicates. Publisher confirms do not turn these transitions into exactly-once operations or protect against every broker/cluster durability failure.

## Permanent ID reservations

Duplicate detection is implemented with one **durable classic single-slot queue per SHA-256 message ID**. Each tombstone message stores only the opaque ID bytes—not the job payload or metadata. The reservation namespace is the RabbitMQ virtual host plus `WithNamePrefix` value.

Reservations are permanent tombstones:

- an ID is never reusable in that namespace, including after `Ack`; do not place sensitive data in IDs because those bytes are retained in tombstones
- dead-lettering and redrive retain the same reservation
- successful traffic creates a permanently growing number of classic queues
- if `Push` creates the reservation but is interrupted before the work publication completes, the orphan reservation still permanently blocks that ID

This design is required to prevent an old at-least-once copy from resurfacing after the same ID has been assigned to a different logical message. The reservation and work publication cannot be made one RabbitMQ transaction with the required topology and confirmation semantics, so `Push` is not failure-atomic.

The reservation queues are operationally expensive and do not have the same replication/high-availability characteristics as the quorum queues holding work and dead letters. Durability of the classic reservation does not make it equivalent to quorum data during node or cluster failures. Capacity planning must include queue-process, metadata-store, file-descriptor, recovery-time, monitoring, and management-API costs—not only message bytes.

There is no supported in-place garbage collection for reservation tombstones or drained delay queues. If permanent growth is unacceptable, this driver is not a suitable backend for that workload.

## External mutation is unsupported

The driver must be the sole owner of its generated topology and messages. Do not externally:

- delete, purge, rename, bind, or redeclare generated queues
- change queue arguments, policies, operator policies, queue types, dead-letter settings, or limits that affect them
- publish directly into work, dead-letter, delay, or reservation resources
- consume or acknowledge messages outside this driver
- selectively remove reservation or delay resources to reclaim topology

Passive monitoring through RabbitMQ's management and metrics interfaces is fine. Mutation is not. Partial cleanup can defeat duplicate detection, strand or prematurely expose messages, cause declaration failures, and break settlement of live deliveries. Changing the prefix does not migrate existing data; it creates a separate namespace.

The driver intentionally does not expose `queue.StatsProvider`. Use RabbitMQ observability for operational metrics, while remembering that physical queue names and topology are implementation details and broker counts are snapshots rather than correctness primitives.

## Failure semantics

RabbitMQ operations cross process and network boundaries. In particular:

- a context cancellation or connection error does not prove that a publish, confirmation, or acknowledgement did not occur
- `amqp091-go` has cancellable confirmation waits but no context-aware API for channel creation, topology declarations, `Basic.Get`, acknowledgement, or channel close; the driver checks context between those calls and while polling, but cancellation cannot interrupt an AMQP RPC already blocked inside the client, so configure a finite connection heartbeat and monitor blocked operations
- quorum availability and confirmed-write durability depend on RabbitMQ cluster health and configuration
- connection recovery does not make an old delivery token valid on a new channel
- at-least-once dead lettering and publisher confirms reduce loss windows but do not provide distributed transactions or exactly-once processing

Keep handlers idempotent, use stable message IDs, retain application-level reconciliation data where needed, and avoid automatically retrying ambiguous operations as though they definitely had no effect.

## Testing

```sh
make test
make race
make lint
# Or:
go test -race -count=1 ./...
go vet ./...
```

Broker-backed integration tests are gated by `RABBITMQ_URL` and skip when it is unset, so `make test` runs without a RabbitMQ server. To run integration tests against an isolated virtual host:

```sh
RABBITMQ_URL='amqp://guest:guest@localhost:5672/' make test
```

Do not point integration tests at a shared or production virtual host: tests may create persistent work, dead-letter, delay, and reservation topology under their test prefixes.

## License

MIT. See [LICENSE](LICENSE).
