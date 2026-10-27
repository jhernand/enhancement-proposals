# Testplan — OSAC-6154

## Overview

- **Feature:** OSAC-6154 — Add actor attribution to Fulfillment Events
- **Design task:** OSAC-6856
- **Planned test cases:** 12
- **PRD In Scope anchors with planned Fulfillment-surface cases:** 4 / 4
- **Design interface changes with planned cases:** 6 / 6
- **Execution status:** Planned only; no tests were run for this draft.

The OSAC PRD template has no FR/NFR identifiers. This testplan uses `PRD-IS1`
through `PRD-IS4` as local anchors for the four In Scope bullets; these are
traceability labels for this testplan, not new PRD requirement IDs.

| Anchor | PRD behavior |
|---|---|
| PRD-IS1 | All object changes have actor attribution through the private API and Kafka. |
| PRD-IS2 | User and system changes have the specified actor identity. |
| PRD-IS3 | An Event remains valid when actor identity is unavailable. |
| PRD-IS4 | Fulfillment makes attributed Events available through its private API and Kafka. |

## Coverage Matrix

| PRD / IC | Behavior and required boundary | Tier / owner | Case | Suite, command, and real/faked dependencies |
|---|---|---|---|---|
| PRD-IS1, IC-1 | Private Event actor contract and public projection privacy. | Component integration / [DEV] OSAC-Core | TC-PRDIS1-01 | Existing `fulfillment-service/internal/servers/events_server_test.go`; from `fulfillment-service/`, `ginkgo run --timeout 10m internal/servers`. Kafka test container and in-process gRPC `bufconn` are real; database, Keycloak, and deployed service are omitted. Proto generation/lint are separate static validation. |
| PRD-IS1, PRD-IS4, IC-5, IC-6 | Outbox actor is decoded into the private protobuf Event and published to Kafka. | Component integration / [DEV] OSAC-Core | TC-PRDIS1-02 | Existing `fulfillment-service/internal/servers/event_publisher_test.go`; same command. PostgreSQL test instance and Kafka test container are real; the outbox row is seeded by the test. |
| PRD-IS1, PRD-IS4, IC-2 | Private Events Watch streams the actor from Kafka to an internal client. | Component integration / [DEV] OSAC-Core | TC-PRDIS1-03 | Existing `fulfillment-service/internal/servers/private_events_server_test.go`; same command. Kafka test container and in-process gRPC `bufconn` are real; database and live authentication are omitted. |
| PRD-IS1, IC-1, IC-2 | Private Watch CEL filters can select Events by `actor.type` and `actor.name`. | Component integration / [DEV] OSAC-Core | TC-PRDIS1-04 | Existing `fulfillment-service/internal/servers/private_events_server_test.go`; from `fulfillment-service/`, `ginkgo run --timeout 10m internal/servers`. Kafka test container and in-process gRPC are real; database and live authentication are omitted. |
| PRD-IS1, IC-5, IC-6 | Malformed non-null actor JSON follows the publisher's permanent-conversion-error policy. | Component integration / [DEV] OSAC-Core | TC-PRDIS1-05 | Existing `fulfillment-service/internal/servers/event_publisher_test.go`; same command. PostgreSQL and Kafka test containers are real; the malformed outbox row is seeded by the test. |
| PRD-IS2, IC-3 | `auth.Subject.Tenant` survives custom JSON serialization and is attached to request context, separate from the visible-tenant set. | Unit / [DEV] OSAC-Core | TC-PRDIS2-01 | Existing `fulfillment-service/internal/auth/auth_subject_test.go` and `grpc_authz_interceptor_test.go`; from `fulfillment-service/`, `ginkgo run -r internal`. Auth subjects, tokens, and policy results are fixtures; no Keycloak, PostgreSQL, or Kafka. |
| PRD-IS2, IC-3 | Actor callback maps `Subject.Tenant == system` to SYSTEM, otherwise uses authentication method to distinguish USER and SERVICE_ACCOUNT, and omits identity only when unavailable. | Unit / [DEV] OSAC-Core | TC-PRDIS2-02 | Proposed `fulfillment-service/internal/cmd/service/start/grpcserver/actor_context_test.go`; from `fulfillment-service/`, `ginkgo run -r internal`. Contexts are fixtures; no Keycloak, PostgreSQL, or Kafka. The test asserts the service-account name selected by implementation. |
| PRD-IS2, IC-4 | Local-variable callback encodes values and returns errors before DML. | Unit / [DEV] OSAC-Core | TC-PRDIS2-03 | Proposed `fulfillment-service/internal/database/dao/generic_dao_local_variable_test.go`; `ginkgo run -r internal`. A transaction test double is used; no external services run for the case. |
| PRD-IS2, PRD-IS3, IC-5 | Trigger persists actor JSONB, leaves it null when absent, and does not leak a local value across transactions. | Component integration (database) / [DEV] OSAC-Core | TC-PRDIS2-04, TC-PRDIS3-01 | Proposed `fulfillment-service/internal/database/migrations/136_add_actor_to_changes_test.go` (136 is the next migration number at this checkout); `ginkgo run -r internal` from `fulfillment-service/`. A real PostgreSQL test container runs; Kafka and Keycloak are omitted. |
| PRD-IS1, PRD-IS2, IC-3–IC-6 | Actor from a DAO mutation reaches the Kafka Event through the trigger and publisher. | Component integration / [DEV] OSAC-Core | TC-PRDIS2-05 | Proposed extension to `fulfillment-service/internal/servers/event_publisher_test.go`; `ginkgo run --timeout 10m internal/servers`. PostgreSQL and Kafka test containers are real; the auth subject is a fixture, not a live Keycloak request. |
| PRD-IS1, PRD-IS2, IC-4–IC-6 | Specialized server/helper DAO write paths persist the request actor in their outbox rows. | Component integration / [DEV] OSAC-Core | TC-PRDIS2-06 | Proposed `fulfillment-service/internal/servers/event_actor_write_paths_test.go`; from `fulfillment-service/`, `ginkgo run --timeout 10m internal/servers`. PostgreSQL testcontainer is real; auth subject is fixture data, and Kafka is not needed to inspect the outbox. |

Run from the repository root after the proto change:

```bash
make -C proto generate
make -C proto lint
```

These are schema/build validation, not integration evidence. Review generated
`proto/public/` and `proto/gen/` output and confirm the actor is absent from the
public Event. Review `fulfillment-service/docs/API.md` to confirm the private
actor semantics are documented without changing the public Event contract.

## Test Cases

### PRD-IS1 — Actor is available through the private Event surfaces

#### TC-PRDIS1-01: Public Event projection omits the private actor

| Interface Change | Priority | Automation |
|---|---|---|
| IC-1 | critical | automated |

##### Preconditions

- The private Event contains an actor; the generated public Event does not.
- The Kafka test broker and public Events server are running.

##### Steps

1. Publish a private Event with a user actor to the test topic.
2. Read the Event through the public Events Watch API.
3. Inspect the generated public Event and its protobuf JSON representation.

##### Expected Results

- The public Watch response contains the object Event and no actor field.
- The private source Event retains its actor.

##### Evidence

- **Tier / owner:** Component integration / [DEV] OSAC-Core.
- **Suite:** Existing `fulfillment-service/internal/servers/events_server_test.go`;
  extend it with the actor projection case.
- **Command / working directory:** From `fulfillment-service/`,
  `ginkgo run --timeout 10m internal/servers`.
- **Real / faked:** Kafka test container and gRPC `bufconn` are real. Database,
  Keycloak, and a deployed service are omitted.

#### TC-PRDIS1-02: EventPublisher publishes the actor from the outbox

| Interface Change | Priority | Automation |
|---|---|---|
| IC-5, IC-6 | critical | automated |

##### Preconditions

- The actor-column migration is applied to the test database.
- PostgreSQL and the Kafka test broker are running.

##### Steps

1. Use table-driven change rows for each registered object payload and valid
   actor JSON for USER, SERVICE_ACCOUNT, and SYSTEM.
2. Include create, update, delete, and signal operations where applicable.
3. Run EventPublisher, consume the Kafka Events, and inspect actor type and name.

##### Expected Results

- User and service-account Events contain the expected actor type and name.
- The system Event has type SYSTEM and no name.
- Every supported object payload carries the same actor metadata; Event ID,
  object payload, EventType, topic, key, and ordering are unchanged.

##### Evidence

- **Tier / owner:** Component integration / [DEV] OSAC-Core.
- **Suite:** Existing `fulfillment-service/internal/servers/event_publisher_test.go`;
  extend the outbox-to-Kafka cases.
- **Command / working directory:** From `fulfillment-service/`,
  `ginkgo run --timeout 10m internal/servers`.
- **Real / faked:** PostgreSQL test instance and Kafka test container are real.
  Outbox rows are seeded directly; request authentication is not exercised.

#### TC-PRDIS1-03: Private Events Watch streams the actor

| Interface Change | Priority | Automation |
|---|---|---|
| IC-2 | high | automated |

##### Preconditions

- A Kafka topic and private Events Watch server are available in the test.

##### Steps

1. Start a private Watch subscription.
2. Publish a protobuf Event with an actor to Kafka.
3. Read the corresponding Watch response.

##### Expected Results

- The private Watch response contains the same actor type and name as the
  published Event.
- The Event ID and object payload are unchanged.

##### Evidence

- **Tier / owner:** Component integration / [DEV] OSAC-Core.
- **Suite:** Existing `fulfillment-service/internal/servers/private_events_server_test.go`.
- **Command / working directory:** From `fulfillment-service/`,
  `ginkgo run --timeout 10m internal/servers`.
- **Real / faked:** Kafka test container is real; gRPC runs in-process using
  `bufconn`; database and live authentication are omitted.

#### TC-PRDIS1-04: Private Watch CEL filters actor fields

| Interface Change | Priority | Automation |
|---|---|---|
| IC-1, IC-2 | high | automated |

##### Preconditions

- The private Event descriptor includes `actor` and the private Watch CEL
  environment is built from that descriptor.
- The Kafka test broker and private Events Watch server are running.

##### Steps

1. Start a Watch with filter `event.actor.type == EVENT_ACTOR_TYPE_USER` and
   publish matching and nonmatching Events.
2. Repeat with filter `event.actor.name == "alice"` and distinct actor names.

##### Expected Results

- Each filter compiles and returns only Events whose actor field matches.
- Events without an actor do not match either actor-field filter.

##### Evidence

- **Tier / owner:** Component integration / [DEV] OSAC-Core.
- **Suite:** Existing `fulfillment-service/internal/servers/private_events_server_test.go`;
  add CEL actor-field cases.
- **Command / working directory:** From `fulfillment-service/`,
  `ginkgo run --timeout 10m internal/servers`.
- **Real / faked:** Kafka test container and in-process gRPC are real; database
  and live authentication are omitted.

#### TC-PRDIS1-05: Malformed actor JSON is dropped by EventPublisher

| Interface Change | Priority | Automation |
|---|---|---|
| IC-5, IC-6 | high | automated |

##### Preconditions

- The actor-column migration is applied to the test database.
- PostgreSQL and the Kafka test broker are running.

##### Steps

1. Seed a change row whose `actor` JSONB is syntactically valid JSON but cannot
   be decoded as `EventActor`, followed by a valid change row.
2. Run EventPublisher and inspect Kafka, the outbox, and the error metric.

##### Expected Results

- No Event is published for the malformed actor row; the publisher logs the
  conversion error, increments `event_publish_errors_total`, and deletes that
  row so draining continues.
- The following valid change is published and removed from the outbox.

##### Evidence

- **Tier / owner:** Component integration / [DEV] OSAC-Core.
- **Suite:** Existing `fulfillment-service/internal/servers/event_publisher_test.go`;
  add a permanent actor-conversion failure case.
- **Command / working directory:** From `fulfillment-service/`,
  `ginkgo run --timeout 10m internal/servers`.
- **Real / faked:** PostgreSQL test instance and Kafka test container are real;
  the malformed JSONB row is seeded directly.

### PRD-IS2 — Actor identity follows the request context

#### TC-PRDIS2-01: Authorization attaches the subject tenant to request context

| Interface Change | Priority | Automation |
|---|---|---|
| IC-3 | critical | automated |

##### Preconditions

- The authz interceptor builds an `auth.Subject` from an authorization decision.

##### Steps

1. Marshal and unmarshal subjects with a populated `Tenant` and `Tenants` set;
   also unmarshal a legacy subject JSON value without `Tenant`.
2. Invoke the authz interceptor with representative user and system-tenant
   inputs, then inspect `auth.Subject` inside the downstream handler.

##### Expected Results

- `Subject.Tenant` contains the authenticated subject's tenant before the
  handler runs.
- `Subject.Tenants` remains the visibility set and is not substituted for
  `Subject.Tenant`.
- Marshal/unmarshal preserves `Tenant`; a legacy value without it decodes with
  an empty `Tenant`.
- A subject belonging to the system tenant has `Tenant == auth.SystemTenant`.

##### Evidence

- **Tier / owner:** Unit / [DEV] OSAC-Core.
- **Suite:** Existing `fulfillment-service/internal/auth/auth_subject_test.go` and
  `fulfillment-service/internal/auth/grpc_authz_interceptor_test.go`.
- **Command / working directory:** From `fulfillment-service/`, the documented
  `ginkgo run -r internal` command.
- **Real / faked:** Auth tokens and policy decisions use test fixtures; the
  interceptor runs in-process. No Keycloak, database, or Kafka is started.

#### TC-PRDIS2-02: Actor callback maps the request subject

| Interface Change | Priority | Automation |
|---|---|---|
| IC-3, IC-4 | critical | automated |

##### Preconditions

- The callback reads the `auth.Subject` and authentication method from context.

##### Steps

1. Provide JWT-user, service-account, system-tenant, guest/anonymous, and
   unclassifiable identified subjects.
2. Invoke the actor callback for each request context.

##### Expected Results

- JWT users map to USER and service accounts map to SERVICE_ACCOUNT, using the
  subject's name.
- `Subject.Tenant == auth.SystemTenant` maps to SYSTEM with no name, regardless
  of authentication method or visible-tenant set.
- Guest/anonymous identity maps to no actor; an available but unclassifiable
  identity returns an error.

##### Evidence

- **Tier / owner:** Unit / [DEV] OSAC-Core.
- **Suite:** Proposed `fulfillment-service/internal/cmd/service/start/grpcserver/actor_context_test.go`.
- **Command / working directory:** From `fulfillment-service/`, the documented
  `ginkgo run -r internal` command.
- **Real / faked:** Synthetic subjects and contexts are used. No external
  service is started. The case asserts the service-account name selected by
  the implementation from the authenticated subject.

#### TC-PRDIS2-03: Local-variable encoding is deterministic and bounded

| Interface Change | Priority | Automation |
|---|---|---|
| IC-4 | high | automated |

##### Preconditions

- `AddLocalVariable` and its value encoder are implemented.

##### Steps

1. Return scalar values and an EventActor protobuf message from callbacks.
2. Return nil, an unsupported value, and a callback or protobuf-marshal error.
3. Capture the SQL calls made by the DAO.

##### Expected Results

- Scalars have the documented PostgreSQL text form; EventActor is encoded with
  protojson.
- The GUC name and value are passed as query arguments, not interpolated into
  SQL.
- Nil clears the transaction-local value. Unsupported values and callback or
  marshal errors return an error before object DML.

##### Evidence

- **Tier / owner:** Unit / [DEV] OSAC-Core.
- **Suite:** Proposed `fulfillment-service/internal/database/dao/generic_dao_local_variable_test.go`.
- **Command / working directory:** From `fulfillment-service/`, the documented
  `ginkgo run -r internal` command.
- **Real / faked:** A transaction test double captures SQL and parameters; no
  external service is used.

#### TC-PRDIS2-04: Trigger persists actor JSONB in the outbox

| Interface Change | Priority | Automation |
|---|---|---|
| IC-5 | critical | automated |

##### Preconditions

- The actor-column migration and updated trigger are applied to an isolated
  test database.

##### Steps

1. Set `osac.actor` locally and perform insert, update, delete, and signal
   operations.
2. Inspect the resulting `changes.actor` values.
3. Repeat without an actor; also roll back a transaction after setting an actor.

##### Expected Results

- Trigger-written rows contain the JSON actor set in their transaction.
- A missing actor is stored as SQL NULL; rollback leaves no committed outbox
  row.
- A later transaction does not inherit a previous transaction's actor.
- Signal events retain their existing event type and actor data.

##### Evidence

- **Tier / owner:** Component integration (database) / [DEV] OSAC-Core.
- **Suite:** Proposed `fulfillment-service/internal/database/migrations/136_add_actor_to_changes_test.go`;
  136 is the next migration number at this checkout.
- **Command / working directory:** From `fulfillment-service/`, the documented
  `ginkgo run -r internal` command.
- **Real / faked:** A real PostgreSQL test container runs the migration and
  trigger. Kafka and Keycloak are omitted.

#### TC-PRDIS2-05: Actor reaches Kafka through the DAO write path

| Interface Change | Priority | Automation |
|---|---|---|
| IC-3–IC-6 | critical | automated |

##### Preconditions

- A GenericDAO is configured with the actor callback.
- PostgreSQL and Kafka test containers are available.

##### Steps

1. Create or update an object using a request context containing a user actor.
2. Exercise delete/archive and Signal using the same configured DAO/context
   path where applicable.
3. Run EventPublisher and consume the resulting Kafka Events.
4. Repeat with a service-account subject and a subject whose tenant is
   `auth.SystemTenant`.

##### Expected Results

- The Kafka Event actor matches the `auth.Subject` attached to the request.
- Create, update, delete/archive, and Signal paths carry the same callback
  semantics wherever they emit object-change Events.
- The database object mutation and actor-bearing outbox row commit or roll back
  together.
- A SYSTEM actor has no name.

##### Evidence

- **Tier / owner:** Component integration / [DEV] OSAC-Core.
- **Suite:** Proposed extension to `fulfillment-service/internal/servers/event_publisher_test.go`.
- **Command / working directory:** From `fulfillment-service/`,
  `ginkgo run --timeout 10m internal/servers`.
- **Real / faked:** PostgreSQL and Kafka test containers are real; auth context
  is a fixture. Live Keycloak and a deployed gRPC server are not exercised.

#### TC-PRDIS2-06: Specialized DAO write paths retain the actor

| Interface Change | Priority | Automation |
|---|---|---|
| IC-4, IC-5 | high | automated |

##### Preconditions

- The actor callback is configured on all supporting DAOs used by mutating
  server helpers.
- The PostgreSQL test database has the actor-column migration applied.

##### Steps

1. Exercise automatic ExternalIP creation from ComputeInstance, Cluster, and
   BareMetalInstance write paths.
2. Exercise ExternalIPPool capacity updates, ExternalIP/ExternalIPAttachment
   lifecycle mutations, Tenant secret cleanup, and ClusterVersion default-swap
   updates.
3. Query the transaction-created `changes` rows for each operation.

##### Expected Results

- Every helper-generated object change has the actor derived from the same
  request subject as the initiating mutation.
- The helper changes and their outbox actor values commit or roll back with the
  request transaction.

##### Evidence

- **Tier / owner:** Component integration / [DEV] OSAC-Core.
- **Suite:** Proposed `fulfillment-service/internal/servers/event_actor_write_paths_test.go`.
- **Command / working directory:** From `fulfillment-service/`,
  `ginkgo run --timeout 10m internal/servers`.
- **Real / faked:** PostgreSQL testcontainer is real. The auth subject is a
  fixture; Kafka is not required because the test inspects committed outbox
  rows directly.

### PRD-IS3 — Events remain valid without an actor

#### TC-PRDIS3-01: Event with no actor identity is still published

| Interface Change | Priority | Automation |
|---|---|---|
| IC-5, IC-6 | high | automated |

##### Preconditions

- The actor callback returns no actor for the request context.
- PostgreSQL and Kafka test containers are available.

##### Steps

1. Perform an object mutation without actor identity.
2. Run EventPublisher and consume the corresponding Kafka Event.

##### Expected Results

- The object change is committed and the Event is published.
- `changes.actor` is SQL NULL, and the protobuf Event has no actor field.

##### Evidence

- **Tier / owner:** Component integration / [DEV] OSAC-Core.
- **Suite:** Proposed extension to `fulfillment-service/internal/servers/event_publisher_test.go`.
- **Command / working directory:** From `fulfillment-service/`,
  `ginkgo run --timeout 10m internal/servers`.
- **Real / faked:** PostgreSQL and Kafka test containers are real; missing actor
  context is a fixture.

## Gaps

### Interface Change Coverage Gaps

- Proto generation/lint are static validation, not a runtime boundary test.
  TC-PRDIS1-01 covers the private-to-public Event projection in the
  Kafka-backed Events server. Review generated public output after
  `make -C proto generate`.
- No deployed cross-component user journey changes. Fulfillment's database,
  publisher, Kafka, and private Watch boundaries are covered by component
  integration cases; no separate QE E2E case is proposed.

## Summary

| Metric | Count |
|---|---:|
| Planned test cases | 12 |
| Critical | 6 |
| High | 6 |
| Medium | 0 |
| Low | 0 |
| Automated | 12 |
| Manual | 0 |
| PRD In Scope anchors with planned cases | 4 / 4 |
| Interface changes with planned cases | 6 / 6 |
