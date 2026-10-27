---
title: actor-attribution-fulfillment-events
authors:
  - juan.hernandez@redhat.com
creation-date: 2026-10-27
last-updated: 2026-10-27
tracking-link:
  - "https://redhat.atlassian.net/browse/OSAC-6856"
prd:
  - "prd.md"
see-also:
  - "/enhancements/OSAC-983-reliable-event-distribution"
replaces:
  - "N/A"
superseded-by:
  - "N/A"
---

# Add actor attribution to Fulfillment Events

## Summary

This design adds a private, typed actor to Fulfillment `Event` messages and
carries it from request context through the PostgreSQL changes outbox to the
existing event publisher and Kafka path. See [PRD](prd.md) for detailed
requirements.

## Motivation

The current `Event` contains an identifier, event type, timestamp, and object
payload, but no actor. The database trigger and separate publisher already
provide a durable path from an object change to Kafka; the request's actor must
be captured in the same transaction so it remains available after the request
context is gone. [PRD: §Problem Statement]

The existing object `creator` value is not a substitute: public attribution can
resolve to a user ID, while private attribution assigns `system`. It does not
reliably identify the caller or distinguish service accounts. [Codebase:
`fulfillment-service/internal/auth/default_attribution_logic.go`,
`fulfillment-service/internal/auth/system_attribution_logic.go`]

### Goals

- Carry typed actor identity from object-changing requests to private Events and
  Kafka while keeping the existing event identifier, ordering, and retry
  behavior. [PRD: §In Scope, bullets 1–3]
- Distinguish user, service-account, and system actors; system actors have no
  name and reveal no system-component detail. [User]
- Populate the request's actor identity during authentication/authorization and
  let a context callback derive the event actor without auth-specific DAO logic.
  [User]
- Keep the actor field private to the current private API and Kafka contract.
  [PRD: §In Scope, bullet 1] [User]

### Non-Goals

- Expose the actor field in the generated public Event API in this Feature.
  [User]
- Add source IP, user agent, region, or other actor metadata. [PRD: §Out of Scope]
- Change object payloads, event types, Watch delivery guarantees, or Kafka
  topic/message-key behavior. [Codebase: `proto/private/osac/private/v1/event_type.proto`, `fulfillment-service/internal/servers/event_publisher.go`]
- Change downstream consumer processing or billing behavior. [User]

## Proposal

Add an `Event.actor` message to the private protobuf contract. The gRPC server
configures a generic DAO local-variable callback that converts request actor
context to an `EventActor` and stores its JSON form in a transaction-local
PostgreSQL setting. The existing object-table trigger copies that value into a
new nullable `changes.actor` JSONB column; the separate publisher decodes it and
attaches it to the private Event before protobuf serialization to Kafka. The
public Event projection remains unchanged. [PRD: §In Scope, bullets 1–3] [User]

### Workflow Description

The sequence shows the Fulfillment path for an object change. Authentication
and authorization attach an `auth.Subject` to the request context before the
handler runs; the DAO callback reads that subject to determine the actor. A
Tenant User, Tenant Admin, Cloud Provider Admin, or internal service account
can initiate an object change. The Cloud Provider Admin can receive the
resulting actor-bearing Event through the private Events Watch API. [PRD:
§In Scope, bullets 1–2, 4] [User]

```mermaid
sequenceDiagram
    actor Writer as Tenant User, Tenant Admin, Cloud Provider Admin, or service account
    participant RPC as Fulfillment gRPC server
    participant DAO as Generic DAO
    database PG as PostgreSQL
    participant Trigger as enqueue_change trigger
    participant Publisher as EventPublisher
    queue Kafka
    participant Watch as Private Events Watch
    actor Admin as Cloud Provider Admin

    Admin->>Watch: Subscribe to private Events
    Writer->>RPC: Submit an object-change request
    RPC->>RPC: Authenticate and authorize
    RPC->>RPC: Attach auth.Subject to request context
    RPC->>DAO: Run mutation with request context
    DAO->>DAO: AddLocalVariable reads auth.Subject
    DAO->>PG: Set transaction-local osac.actor
    DAO->>PG: Insert, update, or delete object
    PG->>Trigger: Run after-row trigger
    Trigger->>PG: Insert change row with actor JSONB
    Trigger-->>PG: Notify changes channel
    RPC->>PG: Commit transaction
    PG-->>Publisher: Make committed change available
    Publisher->>PG: Claim change row
    Publisher->>Publisher: Decode actor and build Event
    Publisher->>Kafka: Publish protobuf Event
    Watch->>Kafka: Consume Event
    Kafka-->>Watch: Event with actor
    Watch-->>Admin: Stream private Event
```

The transaction-local setting is only a bridge to the trigger. The outbox row
makes the actor durable across the process boundary to EventPublisher. [Codebase:
`fulfillment-service/internal/database/database_tx_interceptor.go`,
`fulfillment-service/internal/database/dao/generic_dao_signal.go`]

If no actor is available, the callback clears the local value and the trigger
writes no actor JSON; the object Event is still published. [PRD: §In Scope,
bullet 3]

### API Extensions

| ID | Extension | Change | PRD trace |
|---|---|---|---|
| IC-1 | Private Event protobuf | Add `Event.actor` and `EventActor`; both remain private in generated APIs. | §In Scope, bullets 1–3 |
| IC-2 | Private Events Watch and Kafka | Existing Event surfaces carry the actor; no new RPC or topic is added. | §In Scope, bullets 1 and 4 |
| IC-3 | Auth subject context | Add `auth.Subject.Tenant`, populated during authentication/authorization and available in request context. | §In Scope, bullets 2–3 |
| IC-4 | Generic DAO builder | Add internal `AddLocalVariable` configuration for transaction-local values. | §In Scope, bullets 2–3 |
| IC-5 | Changes outbox | Add nullable JSONB actor column and copy the local value in the trigger. | §In Scope, bullets 2–3 |
| IC-6 | EventPublisher | Read and decode the outbox actor into the private Event before Kafka publication. | §In Scope, bullets 1–3 |

The public Event and public Watch response do not change. CleanAPI generates the
public Event from the private source; the actor field and type are filtered from
that projection. [Codebase: `fulfillment-service/docs/API.md`,
`fulfillment-service/internal/servers/events_server.go`]

The private Watch CEL environment is built from the private Event descriptor, so
private consumers can filter on `actor.type` and `actor.name`. Existing private
API authorization and visibility rules continue to govern access. [User]
[Codebase: `fulfillment-service/internal/servers/private_events_server.go`]

### Implementation Details/Notes/Constraints

#### Private protobuf contract

Add the actor type and field to
`proto/private/osac/private/v1/event_type.proto`. The current Event uses field
numbers through 38, so 39 is the next available number at this checkout; verify
it again before implementation. Private-only fields belong at the end of the
message. [Codebase: `proto/private/osac/private/v1/event_type.proto`,
`fulfillment-service/docs/API.md`]

```protobuf
enum EventActorType {
  option (cleanapi.enum).private = true;

  EVENT_ACTOR_TYPE_UNSPECIFIED = 0;
  EVENT_ACTOR_TYPE_USER = 1;
  EVENT_ACTOR_TYPE_SYSTEM = 2;
  EVENT_ACTOR_TYPE_SERVICE_ACCOUNT = 3;
}

message EventActor {
  option (cleanapi.message).private = true;

  EventActorType type = 1;
  optional string name = 2;
}

message Event {
  // Existing fields omitted.
  EventActor actor = 39 [(cleanapi.field).private = true];
}
```

`name` is present for user and service-account actors and absent for a system
actor. `Event.actor` itself is absent when identity is genuinely unavailable.
The unspecified enum value is a protobuf default and must not be emitted as a
valid actor. [User] [PRD: §In Scope, bullets 2–3]

Marking the actor message, enum, and Event field private keeps them out of
generated public protos. Regenerate `proto/public/` and `proto/gen/` from
`proto/private/`; do not hand-edit generated output. [Codebase: `proto/AGENTS.md`,
`fulfillment-service/docs/CLEANAPI.md`]

#### Actor resolution and DAO local variables

Add `Tenant string` to `auth.Subject` and populate it during the existing
authentication/authorization phase before the handler runs. `Subject.Tenant`
identifies the tenant the subject belongs to; the existing `Subject.Tenants`
set continues to represent the tenants the subject can see. The subject is
already stored in request context, so the DAO callback reads it with
`auth.SubjectFromContext` and does not need an auth-specific DAO interface.
[User] [Codebase: `fulfillment-service/internal/auth/auth_subject.go`]

`auth.Subject` has custom JSON serialization. Extend its internal JSON shape to
include `Tenant` and preserve it through marshal/unmarshal; older serialized
subjects without that property decode with an empty `Tenant`.

The actor callback maps that context as follows:

| Request context | EventActor |
|---|---|
| `Subject.Tenant == auth.SystemTenant` | `SYSTEM`, with `name` absent |
| JWT authentication with a named subject | `USER`, with the user's name |
| Service-account authentication with a named subject | `SERVICE_ACCOUNT`, with the service-account name |
| Guest/anonymous authentication with no actor identity | No `Event.actor` |
| Identity is present but cannot be classified | Return an error; do not silently omit an available identity |

The system-tenant check takes precedence over authentication method. The
callback does not infer actor type from object `creator` or from the subject's
visibility set. Existing `SystemAttributionLogic` remains unrelated: it sets
object `creator` to `system` for private writes. [User] [Codebase:
`fulfillment-service/internal/auth/auth_context.go`,
`fulfillment-service/internal/auth/auth_subject.go`,
`fulfillment-service/internal/auth/system_attribution_logic.go`]

For USER, the actor name comes from `auth.Subject.User`. For SERVICE_ACCOUNT,
the callback uses the authenticated service-account identity; the implementation
selects whether its event name uses the short account name or complete subject.
This formatting choice does not change the actor type or private Event contract.
[Codebase: `fulfillment-service/internal/auth/auth_context_extraction.go`]

Add a callback type to the generic DAO configuration, for example:

```go
type LocalVariableValue func(context.Context) (any, error)

func (b *GenericDAOBuilder[O]) AddLocalVariable(
    name string,
    value LocalVariableValue,
) *GenericDAOBuilder[O]
```

The context parameter lets the callback read the request subject without adding
subject-specific logic to the DAO. For `osac.actor`, the callback returns an
`EventActor` or no value for a genuinely unidentified request.
GenericServerBuilder passes registered local variables to the DAO it constructs;
registration must also configure any supporting DAO that writes object tables.
[User] [Codebase: `fulfillment-service/internal/servers/generic_server.go`,
`fulfillment-service/internal/cmd/service/start/grpcserver/register_servers.go`]

Evaluate configured callbacks once at the start of each mutating DAO request,
before object DML. Encode scalar values as text and `proto.Message` values with
`protojson.MarshalOptions{UseProtoNames: true}`. Reject unsupported values and
propagate callback, encoding, or SQL errors so the transaction rolls back before
the object change is committed. For an absent actor, set the local value to an
empty string so a prior value in the same transaction cannot be reused. [User]
[Codebase: `fulfillment-service/internal/database/dao/generic_dao.go`,
`fulfillment-service/internal/database/dao/generic_dao_request.go`]

Use a transaction-local PostgreSQL setting. The DAO can call
`set_config($1, $2, true)` with the configured name and encoded value, which has
`SET LOCAL` semantics while binding both values instead of interpolating SQL.
Validate local-variable names at builder configuration time. The actor setting
name is the fixed string `osac.actor`. [User] [Codebase:
`fulfillment-service/internal/database/dao/generic_dao_signal.go`]

Apply the hook to all mutating DAO operations that can generate an object
change, including Create, Update, Delete/archive, and Signal. Read-only DAO
operations do not need to set actor state. Configure the same callback on
supporting DAOs used by production write helpers, not just the DAO built by
GenericServerBuilder. Current direct mutation paths include automatic
ExternalIP creation in the ComputeInstance, Cluster, and BareMetalInstance
servers; ExternalIPPool capacity updates; ExternalIP/ExternalIPAttachment and
NATGateway lifecycle updates/deletes; Tenant secret cleanup; and ClusterVersion
default-swap updates. A direct SQL write without request context has no actor
unless the identity is separately provided. [Codebase:
`fulfillment-service/internal/servers/private_compute_instances_server.go`,
`fulfillment-service/internal/servers/private_clusters_server.go`,
`fulfillment-service/internal/servers/private_baremetal_instances_server.go`,
`fulfillment-service/internal/servers/external_ip_pool_selector.go`,
`fulfillment-service/internal/servers/external_ip_lifecycle.go`,
`fulfillment-service/internal/servers/private_tenants_server.go`,
`fulfillment-service/internal/servers/private_cluster_versions_server.go`]

The callback must be wired through each affected server builder and every
supporting DAO builder; read-only supporting DAOs do not need actor configuration.
[Codebase:
`fulfillment-service/internal/database/dao/generic_dao_create.go`,
`fulfillment-service/internal/database/dao/generic_dao_update.go`,
`fulfillment-service/internal/database/dao/generic_dao_delete.go`,
`fulfillment-service/internal/database/dao/generic_dao_signal.go`]

#### Outbox migration and trigger

Add a new numbered migration after the current latest migration; do not modify
migrations 117 or 118. Add nullable `actor JSONB` to `changes` and update
`enqueue_change()` to copy the transaction-local actor value into the new
column. A missing or empty setting becomes SQL `NULL`; a system actor is a JSON
object with `type=SYSTEM` and no `name`. Existing outbox rows remain `NULL` and
are published without an actor; no historical actor backfill is proposed.
[User] [Codebase:
`fulfillment-service/internal/database/migrations/117_add_changes.up.sql`,
`fulfillment-service/internal/database/migrations/118_move_watches_to_kafka.up.sql`]

The trigger continues to use `osac.signal` to classify signal events. The actor
setting is independent and is present for the same transaction as the object
mutation. PostgreSQL validates the stored JSONB cast; malformed callback output
therefore aborts the mutation transaction rather than creating an invalid
outbox row. [Codebase:
`fulfillment-service/internal/database/migrations/118_move_watches_to_kafka.up.sql`]

#### EventPublisher and Watch

Add the actor column to the publisher query and `eventPublisherChange`. When the
column is non-null, decode it with `protojson.Unmarshal` into `EventActor` and
set `Event.actor`; when null, leave the field absent. The publisher continues to
serialize the Event with `proto.Marshal`, preserve the current topic, key, and
event ID, and delete the outbox row only after successful Kafka publication.
[Codebase: `fulfillment-service/internal/servers/event_publisher.go`]

The private Events Watch returns the private Event and therefore carries actor
data. The public Events server maps to the generated public Event; CleanAPI
filters out the private actor field. [Codebase:
`fulfillment-service/internal/servers/private_events_server.go`,
`fulfillment-service/internal/servers/events_server.go`]

Update `fulfillment-service/docs/API.md`'s Events section to document the
private actor types, name-presence rules, and absent-identity behavior. The
public Event documentation and generated public schema remain unchanged.

### Security Considerations

- The actor is computed from server-side request context; callers cannot set or
  spoof it through an object payload. [User] [Codebase:
  `fulfillment-service/internal/auth/grpc_authz_interceptor.go`]
- The actor field and `EventActor` type remain private. The public Events server
  continues to expose the generated public Event, which has no actor field.
  [User] [Codebase: `fulfillment-service/docs/API.md`]
- `SYSTEM` actors have no name, so events do not disclose which system component
  performed an action. Do not log actor values. [User]
- Actor names are identity data. Existing private API authorization and Kafka
  access controls continue to govern their visibility; no new role is proposed.

### Failure Handling and Recovery

- If actor resolution, value encoding, or the transaction-local setting fails,
  the DAO returns an error. The gRPC transaction rolls back, so neither the
  object change nor its outbox event is committed.
- If no actor is available, `osac.actor` is empty and the trigger writes SQL
  `NULL`; the Event remains publishable without `Event.actor`.
- If PostgreSQL cannot cast the actor value to JSONB, the trigger fails and the
  object transaction rolls back.
- If a legacy outbox row has a null actor, EventPublisher emits the Event with
  `actor` absent.
- If EventPublisher cannot decode a non-null actor JSON value, the existing
  conversion-error path logs the failure, increments
  `event_publish_errors_total`, and deletes the row so the drain can continue.
  The object Event is lost, matching the publisher's existing policy for
  permanently unconvertible rows. TC-PRDIS1-05 covers this behavior. [User]
- Kafka send failures continue to retain the outbox row for retry. Event IDs,
  order, and message keys remain unchanged. [Codebase:
  `fulfillment-service/internal/servers/event_publisher.go`]

### RBAC / Tenancy

No new roles or object-ownership semantics are proposed. The actor is added to
the existing private Event path; current authorization and event visibility
rules remain in effect. The public Event schema does not expose actor data.

### Observability and Monitoring

No new metrics are proposed. Use the existing event-publisher error metric for
actor conversion failures. Log error category and outbox ID, not the actor
value. [Codebase: `fulfillment-service/internal/servers/event_publisher.go`]

### Risks and Mitigations

1. **Incorrect actor classification.** User, service-account, and system
   identities can be confused if classification uses object `creator` or the
   subject's visibility set. Mitigation: classify `SYSTEM` from
   `auth.Subject.Tenant == auth.SystemTenant`; classify other identified
   subjects by authentication method; test each category. Owner: OSAC-Core.
2. **Missed write path.** A direct DAO or SQL object write may not configure
   `osac.actor`. Mitigation: configure the generic and specialized write DAO
   builders listed in Implementation Details, and test their outbox rows in
   TC-PRDIS2-05 and TC-PRDIS2-06. Owner: OSAC-Core.
3. **Schema/startup skew.** A new EventPublisher query may run before the gRPC
   server migration adds `changes.actor`. Mitigation: make publisher readiness
   check for the required column before fetching rows. Owner: OSAC-Core.
4. **Actor JSON conversion errors.** Current publisher behavior drops a change
   row on permanent conversion errors. Mitigation: serialize in the request
   transaction, keep the existing drop-and-count behavior for malformed stored
   JSON, and cover it in TC-PRDIS1-05.
5. **Identity exposure.** Actor names may reveal personal or service-account
   information. Mitigation: keep the field private, omit system names, and do
   not log actor values. Owner: OSAC-Core.

### Drawbacks

- The generic DAO gains a reusable local-variable mechanism and each mutating
  write path must be configured correctly.
- Actor data is encoded at the request boundary, stored in PostgreSQL JSONB,
  decoded by a separate process, and serialized again in the Event. This adds
  processing and another persisted column.
- The initial user identity is a name, not a separate stable identifier; names
  may change over time. This follows the PRD's current requirement.
- Omitting a system-component name reduces diagnostic detail by design.

## Alternatives (Not Implemented)

### Keep current Events unchanged

- **Pros:** No protobuf, DAO, database, or publisher changes.
- **Cons:** User- and system-initiated object changes remain unattributed, so
  the PRD's actor-attribution requirement is unmet.
- **Reason not selected:** This does not deliver OSAC-6154. [PRD: §In Scope]

### Derive the event actor from object `creator`

- **Pros:** The creator is already stored with the object row.
- **Cons:** Public attribution can resolve to a user ID, while private
  attribution assigns `system` for private writes. It does not reliably
  identify the caller or distinguish service accounts.
- **Reason not selected:** The event actor is derived from request context
  separately from object creator metadata. [Codebase:
  `fulfillment-service/internal/auth/default_attribution_logic.go`,
  `fulfillment-service/internal/auth/system_attribution_logic.go`]

### Pass actor arguments through every DAO operation

- **Pros:** Each write explicitly carries actor data in its call signature.
- **Cons:** It changes many DAO methods and call sites and couples mutation
  APIs to actor plumbing.
- **Reason not selected:** The user-proposed `AddLocalVariable` callback keeps
  actor resolution outside the generic DAO and applies it uniformly to
  configured writes. [User]

### Interpolate a `SET LOCAL` statement

- **Pros:** It uses the same SQL form as the existing `osac.signal` setting.
- **Cons:** A configurable setting name cannot be passed as a normal SQL value
  parameter and would need safe identifier validation and query construction.
- **Reason not selected:** Use parameterized `set_config(name, value, true)`,
  which has the same transaction-local behavior without interpolating the
  setting name or value. [Codebase:
  `fulfillment-service/internal/database/dao/generic_dao_signal.go`]

### Include a stable user ID instead of a name

- **Pros:** A stable identifier can remain the same if the user's name changes.
- **Cons:** It requires a separate identity-resolution contract and adds data
  beyond the PRD's user-name requirement.
- **Reason not selected:** The initial EventActor contract uses the user's name;
  other identity fields can be considered separately if a later requirement
  needs them. [PRD: §In Scope, bullet 2] [User]

### Store actor inside the changed object JSON

- **Pros:** No separate outbox column is needed.
- **Cons:** Actor data becomes mixed with the object snapshot and could be
  confused with object-owned fields or included in payload conversion.
- **Reason not selected:** Store event-envelope metadata separately in
  `changes.actor`. [User]

### Expose actor in the public Event now

- **Pros:** All Event consumers could use the same public contract.
- **Cons:** It expands public identity data beyond the current Feature's
  private-only scope.
- **Reason not selected:** Keep the field private; public exposure requires a
  later Feature. [User]

## Test Plan

The detailed, requirement-anchored testplan is in
[`testplan.md`](testplan.md). The primary boundaries are private Event
contract/privacy, transaction-local actor capture, JSONB outbox persistence,
Kafka publication, and the private Watch stream.

### Unit Tests

- Actor resolution maps JWT subjects to `USER`, service-account subjects to
  `SERVICE_ACCOUNT`, subjects with `Tenant == auth.SystemTenant` to `SYSTEM`
  without a name, and genuinely unavailable identity to no Event actor.
- `AddLocalVariable` encodes supported scalars and protobuf messages, rejects
  unsupported values, and propagates callback/encoding errors before object DML.

### Integration Tests

- A real PostgreSQL migration test verifies that the trigger stores the
  transaction-local actor JSON in `changes.actor`, preserves `NULL` when absent,
  and does not leak a value across transactions.
- A PostgreSQL+Kafka EventPublisher test verifies actor round-tripping for every
  registered object payload while preserving existing event types and payloads.
- Kafka-backed private Watch tests verify that the actor reaches private
  clients; public Watch tests verify that CleanAPI projection omits it.

### E2E Tests

No separate deployed cross-component user journey changes. The component
integration cases exercise the Fulfillment database-to-Kafka pipeline and the
private Watch stream.

## Graduation Criteria

The target is **Dev Preview (0.5)**. The Dev Preview exit criteria are:

- All 12 planned unit and component-integration test cases pass.
- User, service-account, and system actors are attributed across the covered
  generic and specialized write paths; actor is absent only when identity is
  genuinely unavailable.
- The actor survives the PostgreSQL outbox, EventPublisher, Kafka, and private
  Watch path, while generated public Events remain actor-free.
- Proto generation and lint pass, and `fulfillment-service/docs/API.md`
  documents the private actor contract.

Tech Preview and GA graduation criteria are outside this release's target.

## Upgrade / Downgrade Strategy

- Add a new numbered migration after the current latest migration; do not modify
  migrations 117 or 118. Add `changes.actor` as nullable JSONB. Existing
  pending outbox rows have no actor and are published with `Event.actor` absent;
  no actor backfill is possible from object snapshots.
- Keep the new column nullable so old and new rows coexist. Apply the migration
  before actor-aware writers are enabled.
- OSAC does not support in-place upgrades. The migration is additive; rollback
  to an older binary leaves the extra column in place. Older publishers ignore
  it and therefore publish without actor. No production installation exists
  for this release, so there is no existing production version to sequence or
  upgrade.

## Version Skew Strategy

- `Event.actor` is an additive protobuf field. Older protobuf consumers ignore
  the unknown field; newer consumers accept old Events with no actor.
- The generated public Event remains unchanged because CleanAPI removes the
  private actor field and type.
- Actor-aware EventPublisher code must wait for the `changes.actor` column. The
  current startup check waits only for the `changes` table; extend it to verify
  the required column before running the new fetch query.
- No production deployment exists for this initial release; mixed-version
  rollout ordering is not a release requirement.

## Support Procedures

- Monitor the existing unpublished-change count and
  `event_publish_errors_total`; actor conversion errors use the existing
  publisher error path.
- If actor values are missing unexpectedly, verify that the migration
  completed, the actor callback is configured on the writing DAO, and the
  EventPublisher uses the schema-compatible version.
- Do not log raw actor JSON or system-component names. The design intentionally
  omits a name for `SYSTEM` actors.
- No runtime feature flag is proposed. Rolling back to an older application
  version leaves the additive database column in place and resumes existing
  event publication without actor attribution.

## Infrastructure Needed

No new production infrastructure. The design uses the existing PostgreSQL,
Fulfillment EventPublisher, and Kafka deployments. Integration tests reuse the
existing PostgreSQL and Kafka test containers.

---

## Provenance

Authored: revise @ design 0.12.0 - 7b2b8de, workspace feat/osac-6637-reader-role-access-docs @ 700534eab
Phases: draft, revise, revise, revise, revise, revise, revise

<!-- ai-workflow-provenance:{"schema_version":1,"provenance_kind":"session","workflow":"design","workflow_version":"0.12.0","ai_workflows":"7b2b8de","source_repo":"700534eab","source_repo_branch":"feat/osac-6637-reader-role-access-docs","commits_behind_main":0,"commits_ahead_main":1,"main_ref":"main","phases":["draft","revise","revise","revise","revise","revise","revise"],"authoring_modes":["skill"],"context_changed":false,"origin_untracked":false} -->
