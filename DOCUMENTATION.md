# Documentation

Please refer to the [FAQ](README.md#FAQ), [README.md](README.md), the
[Ideas](#ideas) and the [Issues](https://github.com/jet/equinox/issues) for
background info on what's outstanding (aside from there being lots of room for
more and better docs).

# Background reading

While there is no canonical book on doing event-sourced Domain Modelling, there
are some must read books that stand alone as timeless books in general but are
also specifically relevant to the considerations involved in building such
systems:

_Domain Driven Design, Eric Evans, 2003_ aka 'The Blue Book'; not a leisurely
read but timeless and continues to be authoritative

_Domain Modelling Made Functional, Scott Wlaschin, 2018_; extremely well edited
traversal of Domain Modelling in F# which is a pleasure to read. Does not talk
specifically about event sourcing, but is still very relevant.

_Implementing Domain Driven Design, Vaughn Vernon, 2013_; aka 'The Red Book'.
Worked examples and a slightly different take on DDD (the appendix on
Functional Event sourcing is not far from what we have around these parts;
which is [not surprising given there's input from Jérémie Chassaing](https://github.com/thinkbeforecoding/FsUno.Prod))

- **Your link here** - Please add materials that helped you on your journey so
  far here via PRs!

# Overview

The following diagrams are based on the style defined in
[@simonbrowndotje](https://github.com/simonbrowndotje)'s [C4 model](https://c4model.com/),
rendered using [@skleanthous](https://github.com/skleanthous)'s
[PlantUmlSkin](https://github.com/skleanthous/C4-PlantumlSkin/blob/master/README.md).
It's highly recommended to view
[the talk linked from `c4model.com`](https://www.youtube.com/watch?v=x2-rSnhpw0g&feature=emb_logo).
See [README.md acknowledgments section](README.md#acknowledgements)

## Context Diagram

Equinox and Propulsion together provide a loosely related set of libraries that
you can leverage in an application as you see fit. These diagrams are intended
to give a rough orientation; what you actually build is up to you...

Equinox focuses on the **Consistent Processing** elements of building an
event-sourced system, offering tailored components that interact with a
specific **Consistent Event Store**, as laid out here in this
[C4](https://c4model.com) System Context Diagram:

![Equinox c4model.com Context Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/context.puml&fmt=svg)

:point_up: Propulsion elements (which we consider External to Equinox) support
the building of complementary facilities as part of an overall Application:

- **Ingesters**: read stuff from outside the Bounded Context of the System.
  This kind of service covers aspects such as feeding reference data into
  **Read Models**, ingesting changes into a consistent model via **Consistent
  Processing**. _These services are not acting in reaction to events emanating
  from the **Consistent Event Store**, as opposed to..._
- **Publishers**: react to events as they are arrive from the **Consistent
  Event Store** by filtering, rendering and producing to feeds for downstreams.
  _While these services may in some cases rely on synchronous queries via
  **Consistent Processing**, it's never transacting or driving follow-on work;
  which brings us to..._
- **Reactors**: drive reactive actions triggered by either upstream feeds, or
  events observed in the **Consistent Event Store**. _These services handle
  anything beyond the duties of **Ingesters** or **Publishers**, and will often
  drive follow-on processing via Process Managers and/or transacting via
  **Consistent Processing**. In some cases, a reactor app's function may be to
  progressively compose a notification for a **Publisher** to eventually
  publish._

## Container diagram

The Systems and Components involved break out roughly like this:

![Equinox c4model.com Container Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/container.puml&fmt=svg)

# Equinox.MemoryStore

Equinox encourages sticking with
[Test Pyramid principles](https://martinfowler.com/articles/practical-test-pyramid.html):
focus on unit testing things by default (based on calling `interpret`/`decide`,
`initial` and `fold` from the [Aggregate module](#aggregate-module))

However, the Equinox `MemoryStore` package can also be relevant as part of your
overall testing strategy. The aims are to:
- provide a mechanism where one can provide an empty and/or specifically
  prepared set of streams initialized in ways that make sense for your test
  suite
- allow one to test with fully configured `Service` types if necessary
- enable one to test flows or scenarios (e.g. Process Managers) crossing
  multiple `Service` types
- allow one to validate the above logic works well independent of the effects
  of any of the stores
- allow one to reduce reliance on mechanisms such as the CosmosDB simulator

**NOTE: `MemoryStore` is a complement to testing with a real store - it's
absolutely not a substitute for testing how your app really performs with your
load against your actual store**

A primary supported pattern is to be able to be able to define a test suite and
then run the suite with the right store for the context - e.g.:
-  for unit tests, you might opt to run some important scenarios with a
   `MemoryStore`
- for integration tests, you might run lots of iterations of a Property Based
  Test against a memory store, and a reduced number of iterations of the same
  test against your concrete store
- for acceptance Tests, you'll likely primarily focus on using your concrete
  store

## Container Diagram for `Equinox.MemoryStore`

This diagram shows the high level building blocks used in constructing an
integration test using `Equinox.MemoryStore`

![Equinox.MemoryStore c4model.com Container Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/MemoryStoreContainer.puml)

## Component Diagram for `Equinox.MemoryStore`

This breaks down the components involved internally with the layout above in
terms of the actual structures involved:

![Equinox.MemoryStore c4model.com Component Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/MemoryStore.puml?fmt=svg)

# Equinox.EventStore / Equinox.SqlStreamStore

From the point of view of Equinox, SqlStreamStore and EventStore have a lot in
common in terms of how Equinox interacts with them. For this reason, it's safe
to treat them as equivalent for the purposes of this overview.

## Component Diagram for Equinox.EventStore / Equinox.SqlStreamStore

![Equinox.EventStore/SqlStreamStore c4model.com Component Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/EventStore.puml)

## Code Diagrams for Equinox.EventStore / Equinox.SqlStreamStore

This diagram walks through the basic sequence of operations, where:
- this node has not yet read this stream (i.e. there's nothing in the Cache)
- when we do read it, it's empty (no events):

![Equinox.EventStore/SqlStreamStore c4model.com Code - first Time](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/EventStoreCode.puml&idx=0&fmt=svg)

Next, we extend the scenario to show:
- how the State held in the Cache influences the EventStore/SqlStreamStore APIs
  used
- how writes are managed:
  - when there's no conflict
  - when there's conflict and we're retrying (handle
    `WrongExpectedVersionException`, read the conflicting, loop using those)
  - when there's conflict and we're giving up (throw
    `MaxAttemptsExceededException`; no need to read the conflicting events)

![Equinox.EventStore/SqlStreamStore c4model.com Code - with cache, snapshotting](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/EventStoreCode.puml&idx=1&fmt=svg)

After the write, we circle back to illustrate the effect of the caching when we
have correct state

![Equinox.EventStore/SqlStreamStore c4model.com Code - next time; same process, i.e. cached](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/EventStoreCode.puml&idx=2&fmt=svg)

In other processes (when a cache is not fully in sync), the sequence runs
slightly differently:

![Equinox.EventStore/SqlStreamStore c4model.com Code - another process; using snapshotting](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/EventStoreCode.puml&idx=3&fmt=svg)

# Equinox.Cosmos

## Container Diagram for `Equinox.Cosmos`

![Equinox.Cosmos c4model.com Container Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/CosmosContainer.puml?fmt=svg)

## Component Diagram for `Equinox.Cosmos`

![Equinox.Cosmos c4model.com Component Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/CosmosComponent.puml?fmt=svg)

## Code Diagrams for `Equinox.Cosmos`

This diagram walks through the basic sequence of operations, where:
- this node has not yet read this stream (i.e. there's nothing in the Cache)
- when we do read it, the Read call returns `404` (with a charge of `1 RU`)

![Equinox.Cosmos c4model.com Code - first Time](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/CosmosCode.puml&idx=0&fmt=svg)

Next, we extend the scenario to show:
- how state held in the Cache influences the Cosmos APIs used
- How reads work when a snapshot is held within the _Tip_
- How reads work when the state is built form the events via a Query
- how writes are managed:
  - when there's no conflict (`Sync` stored procedure returns no conflicting
    events)
  - when there's conflict and we're retrying (re-run the decision the
    conflicting events the call to `Sync` yielded)
  - when there's conflict and we're giving up (throw
    `MaxAttemptsExceededException`)

![Equinox.Cosmos c4model.com Code - with cache, snapshotting](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/CosmosCode.puml&idx=1&fmt=svg)

After the write, we circle back to illustrate the effect of the caching when we
have correct state (we get a `304 Not Mofified` and pay only `1 RU`)

![Equinox.Cosmos c4model.com Code - next time; same process, i.e. cached](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/CosmosCode.puml&idx=2&fmt=svg)

In other processes (when a cache is not fully in sync), the sequence runs
slightly differently:
- we read the _Tip_ document, and can work from that snapshot
- the same fallback sequence shown in the initial read will take place if no
  suitable snapshot that passes the `isOrigin` predicate is found within the
  _Tip_

![Equinox.Cosmos c4model.com Code - another process; using snapshotting](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/CosmosCode.puml&idx=3&fmt=svg)

# Glossary

Event Sourcing is easier _and harder_ than you think. This document is not a
tutorial, and you can and will make a mess on your first forays. This glossary
attempts to map terminology from established documentation outside to terms
used in this documentation.

## Event-sourcing

Term                                                       | Description
-----------------------------------------------------------|------------
Aggregate                                                  | Boundary within which a set of Invariants are to be preserved across a set of related Entities and Value Objects
Append                                                     | Add Events reflecting a Decision to a Stream, contingent on an Optimistic Concurrency Check
Bounded Context                                            | Doman Driven Design term for a cohesive set of application functionality. Events should not pass directly between BCs (see Ingestion, Publishing)
Command                                                    | Arguments supplied to one of a Stream's Decision functions; may result in Events being Appended
CQRS                                                       | Command/Query Responsibility Segregation: Architectural principle critical to understand (_but not necessarily slavishly follow_) when building an Event Sourced System
Decision                                                   | Application logic function representing the mapping of an Aggregate State together with arguments reflecting a Command. Yields a response and/or Events to Append to the Stream in order to manifest the intent implied; the rules it considers in doing so are in effect the Invariants of the Aggregate
Event                                                      | Details representing the facts of something that has occurred, or a Decision that was made with regard to an Aggregate state as represented in a Stream
Eventually Consistent                                      | A Read Model can momentarily lag a Stream's current State as events are being Reacted to
Fold                                                       | FP Term used to describe process of building State from the sequence of Events observed on a Stream
[Idempotent](https://en.wikipedia.org/wiki/Idempotence)    | Multiple executions have the same net effect; can safely be processed >1 time without adverse effects
Ingestion                                                  | The act of importing and reconciling data from external systems into the Models and/or Read Models of a given Bounded Context
Invariants                                                 | Rules that an Aggregate's Fold and Decision process work to uphold
Optimistic Concurrency Check                               | (non-) Locking/transaction mechanism used to ensure that Appends to a Stream maintain the Aggregate's Invariants in the presence of multiple concurrent writers
Projection                                                 | Umbrella term for the production, emission or synchronization of models or outputs from the Events being Appended to Streams. Lots of ways of skinning this cat, see Reactions, Read Model, Query, Synchronous Query, Publishing
Publishing                                                 | Selectively rendering Rich Events for a downstream consumer to Ingest into an external Read Model (as opposed to Replication)
Query                                                      | Eventually Consistent read from a Read Model managed via Projections. See also Synchronous Query
Reactions                                                  | Work carried out as a Projection that drives ripple effects, including maintaining Read Models to support Queries or carrying out a chain of activities that conclude in the Publishing of Rich Events
Read Models                                                | Denormalized data maintained inside a Bounded Context as Reactions, honoring CQRS. As opposed to: Replication, Synchronous Query, Publishing
Replication                                                | Rendering Events as an unfiltered feed in order to facilitate generic comnsumption/syncing. Can be a useful tool to scale or decouple Publishing / Reactions from a Store's feed; _BUT: can just as easily be abused to be functionally equivalent to Database Integration -- maintaining a Read Model as a Reaction and/or deliberately Publishing Rich Events is preferred_
Rich Events                                                | Messages deliberately emitted from a Bounded Context (as opposed to Replication) via Publishing
State                                                      | Information inferred from traching the sequence of Events on a Stream in support of Decision (and/or Synchronous Queries)
Store                                                      | Holds Events for a Bounded Context as ordered Streams
Stream                                                     | Ordered sequence of Events in a Store
Synchronous Query                                          | Consistent read direct from Stream State (breaking CQRS and coupling implementation to the State used to support the Decision process). See CQRS, Query, Reactions

## CosmosDb

Term                     | Description
-------------------------|------------
Change Feed              | set of query patterns enabling the running of continuous queries reading Items (documents) in a Range (physical partition) in order of their last update
Change Feed Processor    | Library from Microsoft exposing facilities to Project from a Change Feed, maintaining Offsets per Range of the Monitored Container in a Lease Container
Container                | logical space in a CosmosDb holding [loosely] related Items (aka Documents). Items bear logical Partition Keys. Formerly Collection. Can be allocated Request Units.
CosmosDB                 | Microsoft Azure's managed document database system
Database                 | Group of Containers. Can be allocated Request Units.
DocumentDb               | Original offering of CosmosDB, now entitled the SQL Query Model, `Microsoft.Azure.DocumentDb.Client[.Core]`
Document `id`            | Identifier used to load a document (Item) directly as a _point read_ without a Query
Lease Container          | Container (separate from the Monitored Container to avoid feedback effects) that maintains a set of Offsets per Range, together with leases reflecting instances of the Change Feed Processors and their Range assignments (aka `aux` container)
Partition Key            | Logical key identifying a Stream (a Range is a set of logical partitions identified by such keys). A Logical Partition is limited to a max of 10GB (as is a Range)
Projector                | Process running a [set of] Change Feed Processors across the Ranges of a Monitored Container
Query                    | Using indices to walk a set of relevant items in a Container, yielding Items (documents). Normally confined to a single Partition Key (unless one enters into the parallel universe of _cross-partition queries_)
Range                    | Physical Partition managing a subset of the Partition Key space of a Container (based on hashing) consisting of colocated data running as an individual CosmosDB node. Can be split as part of scaling up the RUs allocated to a Container. Typically limited to a maximum capacity of 10 GB.
Replay                   | The ability to re-run the processing of the Change Feed from the oldest Item (document) forward at will
Request Units            | Pre-provisioned Virtual units representing used to govern the per-second capacity of a Range's query processor (while they are assigned at Container or Database level, the load shedding / rate limiting takes effect at the Range level)
Request Charge           | Number of Request Units charged for a specific action, apportioned against the RU cacity of the relevant Range for that second
Stored Procedure         | JavaScript code stored in a Container that (repeatedly) maps an input request to a set of actions to be transacted as a group within CosmosDB. Incurs equivalent Request Charges for work performed; can chain to a continuation internally after a read or write. Limited to 5 seconds of processing time per action.

## EventStore

Term                      | Description
--------------------------|------------
Category                  | Group of Streams bearing a common prefix `{Category}-{StreamId}`
Event                     | json or blob payload, together with an Event Type name representing an Event
EventStore                | [Open source](https://eventstore.org) Event Sourcing-optimized data store server and programming model with powerful integrated projection facilities
Rolling Snapshot          | Event written to an EventStore stream in order to ensure minimal store roundtrips when there is a Cache miss
Stream                    | Core abstraction presented by the API - an ordered sequence of Events
`WrongExpectedVersion`    | Low level exception thrown to convey the occurence of an Optimistic Concurrency Violation

## Equinox

Term       | Description
-----------|------------
Cache      | `System.Net.MemoryCache` or equivalent holding _State_ and/or `etag` information for a Stream with a view to reducing roundtrips, latency and/or Request Charges
Unfolds    | Snapshot information, stored in an appropriate storage location (not as a Stream's actual Events), _but represented as Events_, to minimize Queries and the attendant Request Charges when there is a Cache miss

# Programming Model

NB this has lots of room for improvement, having started as a placeholder in
[#50](https://github.com/jet/equinox/issues/50); **improvements are absolutely
welcome, as this is intended for an audience with diverse levels of familiarity
with event sourcing in general, and Equinox in particular**.

## Aggregate Module

All the code handling any given Aggregate’s Invariants, Commands and
Synchronous Queries should be [encapsulated within a single
`module`](https://en.wikipedia.org/wiki/Cohesion_(computer_science)). It's
highly recommended to use the following canonical skeleton layout:

```fsharp
module Aggregate

(* StreamName section *)

let [<Literal>] Category = "category"
let streamName id = FsCodec.StreamName.create Category (Id.toString id)

(* Optionally, Helpers/Types *)

// NOTE - these types and the union case names reflect the actual storage
//        formats and hence need to be versioned with care
[<RequiredQualifiedAccess>]
module Events =

    type Event =
        | ...
    // optionally: `encode`, `tryDecode` (only if you're doing manual decoding)
    let codec = FsCodec ... Codec.Create<Event>(...)
```    

Some notes about the intents being satisfied here:

- types and cases in `Events` cannot be used without prefixing with `Events.` -
  while it can make sense to assert in terms of them in tests, in general
  sibling code in adjacent `module`s should not be using them directly (in
  general interaction should be via the `type Service`)

```fsharp
module Fold =

    type State =
    let initial : State = ...
    let evolve state = function
        | Events.X -> (state update)
        | Events.Y -> (state update)
    let fold events = Seq.fold evolve events

    (* Storage Model helpers *)

    let isOrigin : Events.Event = function
       | Events.Snapshotted -> true
       | _ -> false
    let snapshot (state : State) : Event =
       Events.Snapshotted { ... }

let interpretX ... (state : Fold.State) : Events list = ...

type Decision =
    | Accepted
    | Failed of Reason

let decideY ... (state : Fold.State) : Decision * Events list = ...
```

- `interpret`, `decide` _and related input and output types / interfaces_ are
  public and top-level for use in unit tests (often unit tests will `open` the
  `module Fold` to use `initial` and `fold`)

```fsharp
type Service internal (resolve : Id -> Equinox.Stream<Events.Event, Fold.State) = ...`

    member __.Execute(id, command) : Async<unit> =
        let stream = resolve id
        stream.Transact(interpretX command)

    member __.Decide(id, inputs) : Async<Decision> =
        let stream = resolve id
        stream.Transact(decideX inputs)

let create resolve =
    let resolve id =
        let stream = resolve (streamName id)
        Equinox.Stream(Serilog.Log.ForContext<Service>(), stream, maxAttempts = 3)
    Service(resolve)
```

- `Service`'s constructor is `internal`; `create` is the main way in which one
  wires things up (using either a concrete store or a `MemoryStore`) - there
  should not be a need to have it implement an interface and/or go down mocking
  rabbit holes.

While not all sections are omnipresent, significant thought and discussion has
gone into arriving at this layout. Having everything laid out consistently is a
big win, so customizing your layout / grouping is something to avoid doing
until you have
[at least 3](https://en.wikipedia.org/wiki/Rule_of_three_(computer_programming))
representative aggregates of your own implemented.

### Storage Binding Module

Over the top of the Aggregate Module structure, one then binds this to a
concrete storage subsystem. For example:

Depending on how you structure your app, you may opt to maintain such module
either within the `module Aggregate`, or somewhere outside closer to the
[_Composition Root_](https://blog.ploeh.dk/2011/07/28/CompositionRoot/).

```fsharp
module EventStore =
    let accessStrategy =
        Equinox.EventStore.AccessStrategy.RollingSnapshots (Fold.isOrigin, Fold.snapshot)
    let create (context, cache) =
        let cacheStrategy =
            Equinox.EventStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver =
            Equinox.EventStore.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve

module Cosmos =
    let accessStrategy =
        Equinox.Cosmos.AccessStrategy.Snapshot (Fold.isOrigin, Fold.snapshot)
    let create (context, cache) =
        let cacheStrategy =
            Equinox.Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let resolver =
            Equinox.Cosmos.Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create resolver.Resolve
```

### `MemoryStore` Storage Binding Module

For integration testing higher level functionality in Application Services
(straddling multiple Domain `Service`s and/or layering behavior over them), you
can use the `MemoryStore` in the context of your tests:

```fsharp
module MemoryStore =
    let create (store : Equinox.MemoryStore.VolatileStore) =
        let resolver = Equinox.MemoryStore.Resolver(store, Events.codec, Fold.fold, Fold.initial)
        create resolver.Resolve
```

Typically that binding module can live with your test helpers rather than
making your Domain Assemblies depend on it.

## Core concepts

In F#, independent of the Store being used, the Equinox programming model
involves (largely by convention, see [FAQ](README.md#FAQ)), per aggregation of
events on a given category of stream:

- `Category`: the common part of the [Stream Name](https://github.com/fscodec#streamname),
  i.e., the `"Favorites"` part of the `"Favorites-clientId"`

- `streamName`: function responsible for mapping from the input elements aside
  from the `Category` (i.e., the `id` portion that you pass to
  `StreamName.create` or the elements one passes to `StreamName.compose` to
  generate a `FsCodec.StreamName`. In general, the inputs should be
  [strongly typed ids](https://github.com/jet/FsCodec#strongly-typed-stream-ids-using-fsharpumx))

- `'event`: a discriminated union representing all the possible Events from
  which a state can be `evolve`d (see `e`vents and `u`nfolds in the
  [Storage Model](#cosmos-storage-model)). Typically the mapping of the json to an
  `'event` `c`ase is
  [driven by a `UnionContractEncoder`](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/)

- `'state`: the rolling state maintained to enable Decisions or Queries to be
  made given a command and/or other context (not expected to be serializable or
  stored directly in a Store; can be held in a
  [.NET `MemoryCache`](https://docs.microsoft.com/en-us/dotnet/api/system.runtime.caching.memorycache))

- `initial: 'state`: the [implied] state of an empty stream. See also
  [Null Object Pattern](https://en.wikipedia.org/wiki/Null_object_pattern),
  [Identity element](https://en.wikipedia.org/wiki/Identity_element)

- `fold : 'state -> 'event seq -> 'state`: function used to fold one or more
  loaded (or proposed) events (real ones and/or unfolded ones) into a given
  running
  [persistent data structure](https://en.wikipedia.org/wiki/Persistent_data_structure)
  of type `'state`

- (`evolve: state -> 'event -> 'state` - the `folder` function from which
  `fold` is built, representing the application of a single delta that the
  `'event` implies for the model to the `state`. _Note: `evolve` is an
  implementation detail of a given Aggregate; `fold` is the function used in
  tests and used to parameterize the Category's storage configuration._.
  Sometimes named `apply`)

- `interpret: (context/command etc ->) 'state -> event' list` or `decide : (context/command etc ->) 'state -> 'result*'event list`: responsible for _Deciding_ (in an [idempotent](https://en.wikipedia.org/wiki/Idempotence) manner) how the intention represented by `context/command` should be mapped with regard to the provided `state` in terms of:
  a) the `'events` that should be written to the stream to record the decision
  b) (for the `'result` in the `decide` signature) any response to be returned to the invoker (NB returning a result likely represents a violation of the [CQS](https://en.wikipedia.org/wiki/Command%E2%80%93query_separation) and/or CQRS principles, [see Synchronous Query in the Glossary](#glossary))

When using a Store with support for synchronous unfolds and/or snapshots, one
will typically implement two further functions in order to avoid having every
`'event` in the stream be loaded and processed in order to build the `'state`
per Decision or Query  (versus a single cheap point read from CosmosDb to read
the _tip_):

- `isOrigin: 'event -> bool`: predicate indicating whether a given `'event` is
  sufficient as a starting point i.e., provides sufficient information for the
  `evolve` function to yield a correct `state` without any preceding event
  being supplied to the `evolve`/`fold` functions

- `unfold: 'state -> 'event seq`: function used to render events representing
  the `'state` which facilitate short circuiting the building of `state`, i.e.,
  `isOrigin` should be able to yield `true` when presented with this `'event`.
  (in some cases, the store implementation will provide a custom
  `AccessStrategy` where the `unfold` function should only produce a single
  `event`; where this is the case, typically this is referred to as
  `toSnapshot : 'state -> 'event`).

## Decision Flow

When running a decision process, we have the following stages:

1. establish a known `'state` (
   [as at](https://www.infoq.com/news/2018/02/retroactive-future-event-sourced)
   a given Position in the stream of Events)
2. present the _request/command_ and the `state` to the `interpret` function in
   order to determine appropriate _events_ (can be many, or none) that
   represent the _decision_ in terms of events
3. append to the stream, _contingent on the stream still being in the same
   State/Position it was in step 1_:
     a. if there is no conflict (nobody else decided anything since we decided
     what we'd do given that command and state), append the events to the
     stream (retaining the updated _position_ and _etag_)

     b. if there is a conflict, obtain the conflicting events [that other
     writers have produced] since the Position used in step 1, `fold` them into
     our `state`, and go back to 2 (aside: the CosmosDb stored procedure can
     send them back immediately at zero cost or latency, and there is [a
     proposal for EventStore to afford the same
     facility](https://github.com/EventStore/EventStore/issues/1652))

4. [if it makes sense for our scenario], hold the _state_, _position_ and
   _etag_ in our cache. When a Decision or Synchronous Query is needed, do a
   point-read of the _tip_ and jump straight to step 2 if nothing has been
   modified.

See [Cosmos Storage Model](#cosmos-storage-model) for a more detailed discussion of the role of the Sync Stored Procedure in step 3

## Canonical example Aggregate + Service

The following example is a minimal version of
[the Favorites model](samples/Store/Domain/Favorites.fs), with shortcuts for
brevity, that implements all the relevant functions above:

```fsharp
(* Event stream naming + schemas *)

let [<Literal>] Category =
    "Favorites"
let streamName (id : ClientId) =
    FsCodec.StreamName.create Category (ClientId.toString id)

type Item = { id: int; name: string; added: DateTimeOffset }
type Event =
    | Added of Item
    | Removed of itemId: int
    | Snapshotted of items: Item[]

(* State types/helpers *)

type State = Item list  // NB IRL don't mix Event and State types
let is id x = x.id = id

(* Folding functions to build state from events *)

let evolve state = function
    | Snapshotted items -> List.ofArray items
    | Added item -> item :: state
    | Removed id -> state |> List.filter (is id)
let fold state = Seq.fold evolve state

(*
 * Decision Processing to translate a Command's intent to Events that would
 * Make It So
 *)

type Command =
    | Add of Item  // IRL do not use Event records in Command types
    | Remove of itemId: int

let interpret command state =
    let has id = state |> List.exits (is id)
    match command with
    | Add item -> if has item.id then [] else [Added item]
    | Remove id -> if has id then [Removed id] else []

(*
 * Optional: Snapshot/Unfold-related functions to allow establish state
 * efficiently, without having to read/fold all Events in a Stream
 *)

let toSnapshot state = [Event.Snapshotted (Array.ofList state)]

(*
 * The Service defines operations in business terms, neutral to any concrete
 * store selection or implementation supplied only a `resolve` function that can
 * be used to map from ids (as supplied to the `streamName` function) to an
 * Equinox Stream typically the service should be a stateless Singleton
 *)

type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>) =
    let execute clientId command : Async<unit> =
        let stream = resolve clientId
        stream.Transact(interpret command)
    let read clientId : Async<string list> =
        let stream = resolve clientId
        stream.Query id

    member __.Execute(clientId, command) =
        execute clientId command
    member __.Favorite(clientId, skus) =
        execute clientId (Command.Favorite(DateTimeOffset.Now, skus))
    member __.Unfavorite(clientId, skus) =
        execute clientId (Command.Unfavorite skus)
    member __.List clientId : Async<Events.Favorited []> =
        read clientId

let create resolve : Service =
    let resolve id =
        Equinox.Stream(Serilog.Log.ForContext<Service>(), resolve (streamName id), maxAttempts = 3)
    Service(resolve)
```

<a name="api"></a>
# Equinox API Usage Guide

The most terse walkthrough of what's involved in using Equinox to do a
Synchronous Query and/or Execute a Decision process is in the
[Programming Model section](#programming-model). In this section we’ll walk
through how one implements common usage patterns using the Equinox `Stream` API
in more detail.

## ES, CQRS, Event-driven architectures etc

There are a plethora of
[basics underlying Event Sourcing](https://eventstore.org/docs/event-sourcing-basics/index.html)
and its cousin, the
[CQRS architectural style](https://docs.microsoft.com/en-us/azure/architecture/guide/architecture-styles/cqrs).
There are also myriad ways of arranging
[event driven processing across a system](https://docs.microsoft.com/en-us/azure/architecture/guide/architecture-styles/event-driven).

The goal of [CQRS](https://martinfowler.com/bliki/CQRS.html) is to enable
representation of the same data using multiple models. It’s
[not a panacea, and most definitely not a top level architecture](https://www.youtube.com/watch?v=LDW0QWie21s&feature=youtu.be&t=448),
but you should at least be
[considering whether it’s relevant in your context](https://vladikk.com/2017/03/20/tackling-complexity-in-cqrs/).
There are
[various ways of applying the general CQRS concept as part of an architecture](https://docs.microsoft.com/en-us/azure/architecture/patterns/cqrs).

## Applying Equinox in an Event-sourced architecture

There are many trade-offs to be considered along the journey from an initial
proof of concept implementation to a working system and evolving that over time
to a successful and maintainable model. There is no official magical
combination of CQRS and ES that is always correct, so it’s important to look at
the following information as a map of all the facilities available - it’s
certainly not a checklist; not all achievements must be unlocked.

At a high level we have:
- _Aggregates_ - a set of information (Entities and Value Objects) within which
  Invariants need to be maintained, leading us us to logically group them in
  order to consider them when making a Decision
- _Events_ - Events that have been accepted into the model as part of a
  Transaction represent the basic Facts from which State or Projections can be
  derived
- _Commands_ - taking intent implied by an upstream request or other impetus
  (e.g., automated synchronization based on an upstream data source) driving a
  need to sync a system’s model to reflect that implied need (while upholding
  the Aggregate's Invariants). The Decision process is responsible proposing
  Events to be appended to the Stream representing the relevant events in the
  timeline of that Aggregate.
- _State_ - the State at any point is inferred by _folding_ the events in
  order; this state typically feeds into the Decision with the goal of ensuring
  Idempotent handling (if its a retry and/or the desired state already
  pertained, typically one would expect the decision to be "no-op, no Events to
  emit")
- _Projections_ - while most State we'll fold from the events will be dictated
  by what we need (in addition to a Command's arguments) to be able to make the
  Decision implied by the command, the same Events that are _necessary_ for
  Command Handling to be able to uphold the Invariants can also be used as the
  basis for various summaries at the single aggregate level, Rich Events
  exposed as notification feeds at the boundary of the Bounded Context, and/or
  as denormalized representations forming a
  [Materialized View](https://docs.microsoft.com/en-us/azure/architecture/patterns/materialized-view).
- Queries - as part of the processing, one might wish to expose the state
  before or after the Decision and/or a computation based on that to the caller
  as a result. In its simplest form (just reading the value and emitting it
  without any potential Decision/Command even applying), such a _Synchronous
  Query_ is a gross violation of CQRS - reads should ideally be served from a
  _Read Model_

## Programming Model walkthrough

### Flows and Streams

Equinox’s Command Handling consists of < 200 lines including interfaces and
comments in https://github.com/jet/equinox/tree/master/src/Equinox - the
elements you'll touch in a normal application are:

- [`module Flow`](https://github.com/jet/equinox/blob/master/src/Equinox/Flow.fs#L34) -
  internal implementation of Optimistic Concurrency Control / retry loop used
  by `Stream`. It's recommended to at least scan this file as it defines the
  Transaction semantics everything is coming together in service of.
- [`type Stream`](https://github.com/jet/equinox/blob/master/src/Equinox/Equinox.fs#L11) -
  surface API one uses to `Transact` or `Query` against a specific stream
- [`type Target` Discriminated Union](https://github.com/jet/equinox/blob/master/src/Equinox/Equinox.fs#L42) -
  used to identify the Stream pertaining to the relevant Aggregate that
  `resolve` will use to hydrate a `Stream`

Its recommended to read the examples in conjunction with perusing the code in
order to see the relatively simple implementations that underlie the
abstractions; the 2 files can tell many of the thousands of words about to
follow!

#### Stream Members

```fsharp
type Equinox.Stream(stream : IStream<'event, 'state>, log, maxAttempts) =
StoreIntegration
    // Run interpret function with present state, retrying with Optimistic Concurrency
    member __.Transact(interpret : State -> Event list) : Async<unit>

    // Run decide function with present state, retrying with Optimistic Concurrency, yielding Result on exit
    member __.Transact(decide : State -> Result*Event list) : Async<Result>

    // Runs a Null Flow that simply yields a `projection` of `Context.State`
    member __.Query(projection : State -> View) : Async<View>
```

### Favorites walkthrough

In this section, we’ll use possibly the simplest toy example: an unbounded list
of items a user has favorited (starred) in an e-commerce system.

See [samples/Tutorial/Favorites.fsx](samples/Tutorial/Favorites.fsx). It’s
recommended to load this in Visual Studio and feed it into the F# Interactive
REPL to observe it step by step. Here, we'll skip some steps and annotate some
aspects with regard to trade-offs that should be considered.

#### `Event`s + `initial`(+`evolve`)+`fold`

```fsharp
type Event =
    | Added of string
    | Removed of string

let initial : string list = []
let evolve state = function
    | Added sku -> sku :: state
    | Removed sku -> state |> List.filter (fun x -> x <> sku)
let fold s xs = Seq.fold evolve s xs
```

Events are represented as an F# Discriminated Union; see the [article on the
`UnionContractEncoder`](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/)
for information about how that's typically applied to map to/from an Event
Type/Case in an underlying Event storage system.

The `evolve` function is responsible for computing the post-State that should
result from taking a given State and incorporating the effect that _single_
Event implies in that context and yielding that result _without mutating either
input_.

While the `evolve` function operates on a `state` and a _single_ event, `fold`
(named for the standard FP operation of that name) walks a chain of events,
propagating the running state into each `evolve` invocation. It is the `fold`
operation that's typically used a) in tests and b) when passing a function to
an Equinox `Resolver` to manage the behavior.

It should also be called out that Events represent Facts about things that have
happened - an `evolve` or `fold` should not throw Exceptions or log. There
should be absolutely minimal conditional logic.

In order to fulfill the _without mutating either input_ constraint, typically
`fold` and `evolve` either deep clone to a new mutable structure with space for
the new events, or use a [persistent data structure, such as F#'s `list`]
[https://en.wikipedia.org/wiki/Persistent_data_structure,]. The reason this is
necessary is that the result from `fold` can also be used for one of the
following reasons:

- computing a 'proposed' state that never materializes due to a failure to save
  and/or an Optimistic Concurrency failure
- the store can sometimes take a `state` from the cache and `fold`ing in
  different `events` when the conflicting events are supplied after having been
  loaded for the retry in the loop
- concurrent executions against the stream may be taking place in parallel
  within the same process; this is permitted, Equinox makes no attempt to
  constrain the behavior in such a case

#### `Command`s + `interpret`

```fsharp
type Command =
    | Add of string
    | Remove of string
let interpret command state =
    match command with
    | Add sku -> if state |> List.contains sku then [] else [Added sku]
    | Remove sku -> if state |> List.contains sku |> not then [] else [Removed sku]
```

Command handling should almost invariably be implemented in an
[Idempotent](https://en.wikipedia.org/wiki/Idempotence) fashion. In some cases,
a blind append can arguably be an OK way to do this, especially if one is doing
simple add/removes that are not problematic if repeated or reordered. However
it should be noted that:

- each write roundtrip (i.e. each `Transact`), and ripple effects resulting
  from all subscriptions having to process an event are not free either. As the
  cache can be used to validate whether an Event is actually necessary in the
  first instance, it's highly recommended to follow the convention as above and
  return an empty Event `list` in the case of a Command not needing to trigger
  events to move the model toward it's intended end-state

- understanding the reasons for each event typically yields a more correct
  model and/or test suite, which pays off in more understandable code

- under load, retries frequently bunch up, and being able to dedupicate them
  without hitting the store and causing a conflict can significantly reduce
  feedback effects that cause inordinate impacts on stability at the worst
  possible time

_It should also be noted that, when executing a Command, the `interpret`
function is expected to behave statelessly; as with `fold`, multiple concurrent
calls within the same process can occur.__

A final consideration to mention is that, even when correct idempotent handling
is in place, two writers can still produce conflicting events. In this
instance, the `Transact` loop's Optimistic Concurrency control will cause the
'loser' to re-`interpret` the Command with an updated `state` [incorporating
the conflicting events the other writer (thread/process/machine) produced] as
context

#### `Stream` usage

```fsharp
let [<Literal>] Category = Favorites
let streamName (clientId : String) = FsCodec.StreamName.create Category clientId

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =

    let execute clientId command : Async<unit> =
        let stream = resolve clientId
        stream.Transact(interpret command)

    let read clientId : Async<string list> =
        let stream = resolve clientId
        inner.Query id

let create resolve =
    let resolve clientId =
        let streamName = streamName clientId
        Equinox.Stream(log, resolve streamName, maxAttempts = 3)
    Service(resolve)
```

The `Stream`-related functions in a given Aggregate establish the access
patterns used across when Service methods access streams (see below). Typically
these are relatively straightforward calls forwarding to a `Equinox.Stream`
equivalent (see [`src/Equinox/Equinox.fs`](src/Equinox/Equinox.fs)), which in
turn use the Optimistic Concurrency retry-loop  in
[`src/Equinox/Flow.fs`](src/Equinox/Flow.fs).

`Read` above will do a roundtrip to the Store in order to fetch the most recent
state (in `AllowStale` mode, the store roundtrip can be optimized out by
reading through the cache). This Synchronous Read can be used to
[Read-your-writes](https://en.wikipedia.org/wiki/Consistency_model#Read-your-writes_Consistency)
to establish a state incorporating the effects of any Command invocation you
know to have been completed.

`Execute` runs an Optimistic Concurrency Controlled `Transact` loop in order to
effect the intent of the [write-only] Command. This involves:

1. establish state
2. use `interpret` to determine what (if any) Events need to be appended
3. submit the Events, if any, for appending
4. retrying b+c where there's a conflict (i.e., the version of the stream that
   pertained in (a) has been superseded)
5. after `maxAttempts` / `3` retries, a `MaxResyncAttemptsExceededException` is
   thrown, and an upstream can retry as necessary (depending on SLAs, a timeout
   may further constrain number of retries that can occur)

Aside from reading the various documentation regarding the concepts underlying
CQRS, it's important to consider that (unless you're trying to leverage the
Read-your-writes guarantee), doing reads direct from an event-sourced store is
generally not considered a best practice (the opposite, in fact). Any state you
surface to a caller is by definition out of date the millisecond you obtain it,
so in many cases a caller might as well use an eventually-consistent version of
the same state obtained via a [n eventually-consistent] Projection (see terms
above).

All that said, if you're in a situation where your cache hit ratio is going to
be high and/or you have reason to believe the underlying Event-Streams are not
going to be long, pretty good performance can be achieved nonetheless; just
consider that taking this shortcut _will_ impede scaling and, at worst, can
result in you ending up with a model that's potentially both:

- overly simplistic - you're passing up the opportunity to provide a Read Model
  that directly models the requirement by providing a Materialized View

- unnecessarily complex - the increased complexity of the `fold` function
  and/or any output from `unfold` (and its storage cost) is a drag on one's
  ability to read, test, extend and generally maintain the Command
  Handling/Decision logic that can only live on the write side

#### `Service` Members

```fsharp
    member __.Favorite(clientId, sku) =
        execute clientId (Add sku)
    member __.Unfavorite(clientId, skus) =
        execute clientId (Remove skus)
    member __.List clientId : Async<string list> =
        read clientId
```

- while the Stream-related logic in the `Service` type can arguably be
  extracted into a `Stream` or `Handler` type in order to separate the stream
  logic from the business function being accomplished, its been determined in
  the course of writing tutorials and simply explaining what things do that the
  extra concept count does not present sufficient value. This can be further
  exacerbated by the need to hover and/or annotate in order to understand what
  types are flowing about.

- while the Command pattern can help clarify a high level flow, there's no
  subsitute for representing actual business functions as well-named methods
  representing specific behaviors that are meaningful in the context of the
  application's Ubiquitous Language, can be documented and tested.

- the `resolve` parameter affords one a sufficient
  [_seam_](http://www.informit.com/articles/article.aspx?p=359417) that
  facilitates testing independently with a mocked or stubbed Stream (without
  adding any references), or a `MemoryStore` (which does necessitate a
  reference to a separate Assembly for clarity) as desired. 

### Todo[Backend] walkthrough

See [the TodoBackend.com sample](README.md#TodoBackend) for reference info
regarding this sample, and
[the `.fsx` file from where this code is copied](samples/Tutorial/Todo.fsx).
Note that the bulk if the design of the events stems from the nature of the
TodoBackend spec; there are many aspects of the implementation that constitute
less than ideal design; please note the provisos below...

#### `Event`s

```fsharp
module Events =
    type Todo = { id: int; order: int; title: string; completed: bool }
    type Event =
        | Added     of Todo
        | Updated   of Todo
        | Deleted   of int
        | Cleared
        | Compacted of Todo[]
```

The fact that we have a `Cleared` Event stems from the fact that the spec
defines such an operation. While one could implement this by emitting a
`Deleted` event per currently existing item, there many reasons to do model
this as a first class event:
1. Events should reflect user intent in its most direct form possible; if the
   user clicked Delete All, it's not the same to implement that as a set of
   individual deletes that happen to be united by having timestamp with a very
   low number of ms of each other.
2. Because the `Cleared` Event establishes a known State, one can have the
   `isOrigin` flag the event as being the furthest one needs to search
   backwards before starting to `fold` events to establish the state. This also
   prevents the fact that the stream gets long in terms of numbers of events
   from impacting the efficiency of the processing
3. While having a `Cleared` event happens to work, it also represents a
   technical trick in a toy domain and should not be considered some cure-all
   Pattern - real Todo apps don't have a 'declare bankruptcy' function. And
   example alternate approaches might be to represent each Todo list as it's
   own stream, and then have a `TodoLists` aggregate coordinating those.

The `Compacted` event is used to represent Rolling Snapshots (stored in-stream)
and/or Unfolds (stored in Tip document-Item); For a real Todo list, using this
facility may well make sense - the State can fit in a reasonable space, and the
likely number of Events may reach an interesting enough count to justify
applying such a feature:
1. it should also be noted that Caching may be a better answer - note
   `Compacted` is also an `isOrigin` event - there's no need to go back any
   further if you meet one.
2. we use an Array in preference to a [F#] `list`; while there are
   `ListConverter`s out there (notably not in
   [`FsCodec`](https://github.com/jet/FsCodec)), in this case an Array is
   better from a GC and memory-efficiency stance, and does not need any special
   consideration when using `Newtonsoft.Json` to serialize.

#### `State` + `initial` + `evolve`/`fold`

```fsharp
type State = { items : Todo list; nextId : int }
let initial = { items = []; nextId = 0 }
let evolve s = function
    | Added item -> { s with items = item :: s.items; nextId = s.nextId + 1 }
    | Updated value -> { s with items = s.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
    | Deleted id -> { s with items = s.items |> List.filter (fun x -> x.id <> id) }
    | Cleared -> { s with items = [] }
    | Snapshotted items -> { s with items = List.ofArray items }
let fold : State -> Events.Event seq -> State = Seq.fold evolve
let isOrigin = function Cleared | Compacted _ -> true | _ -> false
let snapshot state = Snapshotted (Array.ofList state.items)
```

- for `State` we use records and `list`s as the state needs to be a Persistent
  data structure.
- in general the `evolve` function is straightforward idiomatic F# - while
  there are plenty ways to improve the efficiency (primarily by implementing
  `fold` using mutable state), in reality this would reduce the legibility and
  malleability of the code.

#### `Command`s + `interpret`

```fsharp
type Command = Add of Todo | Update of Todo | Delete of id: int | Clear
let interpret c (state : State) =
    match c with
    | Add value -> [Added { value with id = state.nextId }]
    | Update value ->
        match state.items |> List.tryFind (function { id = id } -> id = value.id) with
        | Some current when current <> value -> [Updated value]
        | _ -> []
    | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then [Deleted id] else []
    | Clear -> if state.items |> List.isEmpty then [] else [Cleared]
```

- Note `Add` does not adhere to the normal idempotency constraint, being
  unconditional. If the spec provided an id or token to deduplicate requests,
  we'd track that in the `fold` and use it to rule out duplicate requests.

- For `Update`, we can lean on structural equality in `when current <> value`
  to cleanly rule out redundant updates

- The current implementation is 'good enough' but there's always room to argue
  for adding more features. For `Clear`, we could maintain a flag about whether
  we've just seen a clear, or have a request identifier to deduplicate,
  rather than risk omitting a chance to mark the stream clear and hence
  leverage the `isOrigin` aspect of having the event.

#### `Service`

```fsharp
type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>) =

    let execute clientId command : Async<unit> =
        let stream = resolve clientId
        stream.Transact(interpret command)
    let handle clientId command : Async<Todo list> =
        let stream = resolve clientId
        stream.Transact(fun state ->
            let events = interpret command state
            let state' = fold state events
            state'.items,events)
    let query clientId (projection : State -> T) : Async<T> =
        let stream = resolve clientId
        stream.Query projection

    member __.List clientId : Async<Todo seq> =
        query clientId (fun s -> s.items |> Seq.ofList)
    member __.TryGet(clientId, id) =
        query clientId (fun x -> x.items |> List.tryFind (fun x -> x.id = id))
    member __.Execute(clientId, command) : Async<unit> =
        execute clientId command
    member __.Create(clientId, template: Todo) : Async<Todo> = async {
        let! updated = handle clientId (Command.Add template)
        return List.head updated }
    member __.Patch(clientId, item: Todo) : Async<Todo> = async {
        let! updated = handle clientId (Command.Update item)
        return List.find (fun x -> x.id = item.id) updated }
```

- `handle` represents a command processing flow where we (idempotently) apply a
  command, but then also emit the state to the caller, as dictated by the needs
  of the call as specified in the TodoBackend spec. We use the `fold` function
  to compute the post-state, and then project from that, along with the
  (pending) events as computed.

- While we could theoretically use Projections to service queries from an
  eventually consistent Read Model, this is not in alignment with the
  Read-you-writes expectation embodied in the tests (i.e. it would not pass the
  tests), and, more importantly, would not work correctly as a backend for the
  app. Because we have more than one query required, we make a generic `query`
  method, even though a specific `read` method (as in the Favorite example)
  might make sense to expose too

- The main conclusion to be drawn from the Favorites and TodoBackend `Service`
  implementations's use of `Stream` Methods is that, while there can be
  commonality in terms of the sorts of transactions one might encapsulate in
  this manner, there's also It Depends factors; for instance:
    1. the design doesnt provide complete idempotency and/or follow the CQRS style
    2. the fact that this is a toy system with lots of artificial constraints
       and/or simplifications when compared to aspects that might present in a
       more complete implementation.

- the `AggregateId` and `Stream` Active Patterns provide succinct ways to map
  an incoming `clientId` (which is not a `string` in the real implementation
  but instead an id using
  [`FSharp.UMX`](https://github.com/fsprojects/FSharp.UMX) in an unobtrusive
  manner.

<a name="queries"></a>
# Queries

Queries are handled by `Equinox.Stream`s' `Query` function.

A _query_ projects a value from the `'state` of an Aggregate. Queries should be
used sparingly, as loading and folding the events each time is against the
general principle of Command Query Responsibility Segregation (CQRS). A query
should not simply expose the `'state` of an aggregate, as this will inevitably
lead to the leaking of decision logic outside of the Aggregate's `module`.

```fsharp
// Query function exposing part of the state
member __.ReadAddress(clientId) =
    let stream = resolve clientId
    stream.Query(fun state -> state.address)

// Return the entire state we hold for this aggregate (NOTE: generally not a good idea)
member __.Read(clientId) =
    let stream = resolve clientId
    stream.Query id
```

<a name="commands"></a>
# Command+Decision Handling functions

Commands or Decisions are handled via `Equinox.Stream`'s `Transact` method

## Commands (`interpret` signature)

The normal [command pattern](https://en.wikipedia.org/wiki/Command_pattern)
involves taking the execution context (e.g., the principal on behalf of which
the processing is happening), a command (with relevant parameters) reflecting
the intent and the present `'state` of the Aggregate into account and mapping
that to one or more Events that represent that intent as a decision on the
stream.

In this case, the Decision Process is `interpret`ing the _Command_ in the
context of a `'state`.

The function signature is:
`let interpret (context, command, args) state : Events.Event list`

Note the `'state` is the last parameter; it's computed and supplied by the
Equinox Flow.

If the _interpret function_ does not yield any events, there will be no trip to
the store them.

A command may be rejected
[by throwing](https://eiriktsarpalis.wordpress.com/2017/02/19/youre-better-off-using-exceptions/)
from within the `interpret` function.

_Note that emitting an event dictates that the processing may be rerun should a
conflicting write have taken place since the loading of the state_

```fsharp

let interpret (context, command) state : Events.Event list =
    match tryCommand context command state with
    | None ->
        [] // not relevant / already in effect
    | Some eventDetails -> // accepted, mapped to event details record
        [Event.HandledCommand eventDetails]

type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>)

    // Given the supplied context, apply the command for the specified clientId
    member __.Execute(clientId, context, command) : Async<unit> =
        let stream = resolve clientId
        stream.Transact(fun state -> interpretCommand (context, command) state)

    // Given the supplied context, apply the command for the specified clientId
    // Throws if this client's data is marked Read Only
    member __.Execute(clientId, context, command) : Async<unit> =
        let stream = resolve clientId
        stream.Transact(fun state ->
            if state.isReadOnly then raise AccessDeniedException() // Mapped to 403 externally
            interpretCommand (context, command) state)
```

## Decisions (`Transact`ing Commands that also emit a result using the `decide` signature)

In some cases, depending on the domain in question, it may be appropriate to
record some details of the request (that are represented as Events that become
Facts), even if the 'command' is logically ignored. In such cases, the
necessary function is a hybrid of a _projection_ and the preceding `interpret`
signature: you're both potentially emitting events and yielding an outcome or
projecting some of the 'state'.

In this case, the signature is: `let decide (context, command, args) state :
'result * Events.Event list`

Note that the return value is a _tuple_ of `('result,Event list):
- the `fst` element is returned from `stream.Transact`
- the `snd` element of the tuple represents the events (if any) that should
  represent the state change implied by the request.with

Note if the decision function yields events, and a conflict is detected, the
flow may result in the `decide` function being rerun with the conflicting state
until either no events are emitted, or there were on further conflicting writes
supplied by competing writers.

```fsharp
let decide (context, command) state : int * Events.Event list =
   // ... if `snd` contains event, they are written
   // `fst` (an `int` in this instance) is returned as the outcome to the caller

type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>) =

    // Given the supplied context, attempt to apply the command for the specified clientId
    // NOTE Try will return the `fst` of the tuple that `decide` returned
    // If >1 attempt was necessary (e.g., due to conflicting events), the `fst`
    // from the last attempt is the outcome
    member __.Try(clientId, context, command) : Async<int> =
        let stream = resolve clientId
        stream.Transact(fun state ->
            decide (context, command) state)
```

## DOs

- Identify Invariants you're seeking to maintain. Events are ordered and
  updates consistency checked for this reason; it'll also be an important part
  of how you test things.

- In general, you want to default to separating reads from writes for ease of
  understanding, testing, maintenance and scaling (see CQRS)

- Any command's processing should take into account the current `'state` of the
  aggregate, `interpreting` the state in an
  [idempotent](https://en.wikipedia.org/wiki/Idempotence) manner; applying the
  same Command twice should result in no events being written when the same
  logical request is made the second time.

## DONTs

- Write blindly: blind writes (ignoring
  [idempotence](https://en.wikipedia.org/wiki/Idempotence) principles)
  is normally a design smell

- Mixing Commands and Queries - in general, the read and write paths should be
  separated as much as possible (see CQRS)

<a name="testing-interpret"></a>
## Testing `interpret` functions

The canonical `interpret` and `decide` signatures above make unit testing
possible without imposing the use of any support libraries or DSLs.

Given an opening `state` and an `interpret` command, you can validate the
handling is idempotent as follows:

```fsharp
let fold, initial = Aggregate.Fold.fold, Aggregate.Fold.initial
// Alternately: open Aggregate.Fold

let validateInterpret contextAndOrArgsAndOrCommand state =
    let events = interpret contextAndOrArgsAndOrCommand state
    // TODO assert/match against the events to validate correct events
    //      considering the contextAndOrArgsAndOrCommand
    let state' = fold state events
    // TODO assert the events, when `fold`ed, yield the correct successor state
    //      (in general, prefer asserting against `events` than `state'`)
    state'

// Validate handling is idempotent in nature
let validateIdempotent contextAndOrArgsAndOrCommand state' =
    let events' = interpret contextAndOrArgsAndOrCommand state'
    match events' with
    | [] -> ()
    // TODO add clauses to validate edge cases that should still generate events on a re-run
    | xs -> failwithf "Not idempotent; Generated %A in response to %A" xs contextAndOrArgsAndOrCommand
```

### With [`FsCheck.xUnit`](https://fsharpforfunandprofit.com/posts/property-based-testing/)

Example FsCheck.xUnit test to validate command is always valid given the
Aggregate's `initial` state:

```fsharp
let [<Property>] properties contextAndOrArgsAndOrCommand =
    let state' = validateInterpret contextAndOrArgsAndOrCommand initial
    validateIdempotent contextAndOrArgsAndOrCommand state'
```

### With `xUnit` [`TheoryData`](https://blog.ploeh.dk/2019/09/16/picture-archivist-in-f/)

```fsharp
type InterpretCases() as this =
    inherit TheoryData()
    do this.Add( case1 )
    do this.Add( case2 )

let [<Theory; ClassData(nameof(InterpretCases)>] examples args =
    let state' = validateInterpret contextAndOrArgsAndOrCommand initial
    validateIdempotent contextAndOrArgsAndOrCommand state'
```

<a name="composed-commands"></a>
## Handling sequences of Commands as a single transaction

In some cases, a Command is logically composed of separable actions against the
aggregate. It's advisable in general to represent each aspect of the processing
in terms of the above `interpret` function signature. This allows that aspect
of the behavior to be unit tested cleanly. The overall chain of processing can
then be represented as a [composed method](https://wiki.c2.com/?ComposedMethod)
which can then summarize the overall transaction.

### Idiomatic approach - composed method based on side-effect free functions

There's an example of such a case in the
[Cart's Domain Service](https://github.com/jet/equinox/blob/master/samples/Store/Backend/Cart.fs#L53):

```fsharp
let interpretMany fold interpreters (state : 'state) : 'state * 'event list =
    ((state,[]),interpreters)
    ||> Seq.fold (fun (state : 'state, acc : 'event list) interpret ->
        let events = interpret state
        let state' = fold state events
        state', acc @ events)

type Service internal (resolve : CartId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Run(cartId, optimistic, commands : Command seq, ?prepare) : Async<Fold.State> =
        let stream = resolve (cartId,if optimistic then Some Equinox.AllowStale else None)
        stream.TransactAsync(fun state -> async {
            match prepare with None -> () | Some prep -> do! prep
            return interpretMany Fold.fold (Seq.map interpret commands) state })
```

<a name="accumulator"></a>
### Alternate approach - composing with an Accumulator encapsulating the folding

_NOTE: This is an example of an alternate approach provided as a counterpoint -
there's no need to read it as the preceding approach is the recommended one is
advised as a default strategy to use_

As illustrated in [Cart's Domain
Service](https://github.com/jet/equinox/blob/master/samples/Store/Backend/Cart.fs#L5),
an alternate approach is to encapsulate the folding (Equinox in V1 exposed an
interface that encouraged such patterns; this was removed in two steps, as code
written using the idiomatic approach is [intrinsically simpler, even if it
seems not as Easy](https://www.infoq.com/presentations/Simple-Made-Easy/) at
first)

```fsharp
/// Maintains a rolling folded State while Accumulating Events pended as part
/// of a decision flow
type Accumulator<'event, 'state>(fold : 'state -> 'event seq -> 'state, originState : 'state) =
    let accumulated = ResizeArray<'event>()

    /// The Events that have thus far been pended via the `decide` functions
    /// `Execute`/`Decide`d during the course of this flow
    member __.Accumulated : 'event list =
        accumulated |> List.ofSeq

    /// The current folded State, based on the Stream's `originState` + any
    /// events that have been Accumulated during the the decision flow
    member __.State : 'state =
        accumulated |> fold originState

    /// Invoke a decision function, gathering the events (if any) that it
    /// decides are necessary into the `Accumulated` sequence
    member __.Transact(interpret : 'state -> 'event list) : unit =
        interpret __.State |> accumulated.AddRange
    /// Invoke an Async decision function, gathering the events (if any) that
    /// it decides are necessary into the `Accumulated` sequence
    member __.TransactAsync(interpret : 'state -> Async<'event list>) : Async<unit> = async {
        let! events = interpret __.State
        accumulated.AddRange events }
    /// Invoke a decision function, while also propagating a result yielded as
    /// the fst of an (result, events) pair
    member __.Transact(decide : 'state -> 'result * 'event list) : 'result =
        let result, newEvents = decide __.State
        accumulated.AddRange newEvents
        result
    /// Invoke a decision function, while also propagating a result yielded as
    /// the fst of an (result, events) pair
    member __.TransactAsync(decide : 'state -> Async<'result * 'event list>) : Async<'result> = async {
        let! result, newEvents = decide __.State
        accumulated.AddRange newEvents
        return result }

type Service ... =
    member __.Run(cartId, optimistic, commands : Command seq, ?prepare) : Async<Fold.State> =
        let stream = resolve (cartId,if optimistic then Some Equinox.AllowStale else None)
        stream.TransactAsync(fun state -> async {
            match prepare with None -> () | Some prep -> do! prep
            let acc = Accumulator(Fold.fold, state)
            for cmd in commands do
                acc.Transact(interpret cmd)
            return acc.State, acc.Accumulated
        })
```

# Equinox Architectural Overview

There are virtually unlimited ways to build an event-sourced model. It's
critical that, for any set of components to be useful, that they are designed
in a manner where one combines small elements to compose a whole, [versus
trying to provide a hardwired end-to-end
'framework'](https://youtu.be/LDW0QWie21s?t=1928).

While going the library route leaves plenty seams needing to be tied together
at the point of consumption (with resulting unavoidable complexity), it's
unavoidable if one is to provide a system that can work in the real world.

This section outlines key concerns that the Equinox
[Programming Model](#programming-model) is specifically taking a view on, and
those that it is going to particular ends to leave open.

## Concerns leading to need for a programming model

F#, out of the box has a very relevant feature set for building Domain models
in an event sourced fashion (DUs, persistent data structures, total matching,
list comprehensions, async builders etc). However, there are still lots of ways
to split the process of folding the events, encoding them, deciding events to
produce etc.

In the general case, it doesnt really matter what way one opts to model the
events, folding and decision processing.

However, given one has a specific store (or set of stores) in mind for the
events, a number of key aspects need to be taken into consideration:

1. Coding/encoding events - Per aggregate or system, there is commonality in
   how one might wish to encode and/or deal with versioning of event
   representations. Per store, the most efficient way to bridge to that concern
   can vary. Per domain and encoding system, the degree to which one wants to
   unit or integration test this codec process will vary.

2. Caching - Per store, there are different tradeoffs/benefits for Caching. Per
   system, caching may or may not even make sense. For some stores, it makes
   sense to integrate caching into the primary storage.

3. Snapshotting - The store and/or the business need may provide a strong
   influence on whether or not (and how) one might employ a snapshotting
   mechanism.

## Store-specific concerns mapping to the programming model

This section enumerates key concerns feeding into how Stores in general, and
specific concrete Stores bind to the [Programming Model](#programming-model):

### EventStore

TL;DR caching not really needed, storing snapshots has many considerations in
play, projections built in

Overview: EventStore is a mature and complete system, explicitly designed to
address key aspects of building an event-sourced system. There are myriad
bindings for multiple languages and various programming models. The docs
present various ways to do snapshotting. The projection system provides ways in
which to manage snapshotting, projections, building read models etc.

Key aspects relevant to the Equinox programming model:
- In general, EventStore provides excellent caching and performance
  characteristics intrinsically by virtue of its design

- Projections can be managed by either tailing streams (including the synthetic
  `$all` stream) or using the Projections facility - there's no obvious reason
  to wrap it, aside from being able to uniformly target CosmosDb (i.e. one
  could build an `Equinox.EventStore.Projection` library and an `eqx project
  stats es` with very little code).

- In general event streams should be considered append only, with no mutations
  or deletes

- For snapshotting, one can either maintain a separate stream with a maximum
  count or TTL rule, or include faux _Compaction_ events in the normal streams
  (to make it possible to combine reading of events and a snapshot in a single
  roundtrip). The latter is presently implemented in `Equinox.EventStore`

- While there is no generic querying facility, the APIs are designed in such a
  manner that it's generally possible to achieve any typically useful event
  access pattern needed in an optimal fashion (projections, catchup
  subscriptions, backward reads, caching)

- While EventStore allows either json or binary data, its generally accepted
  that json (presented as UTF-8 byte arrays) is a good default for reasons of
  interoperability (the projections facility also strongly implies json)

### Azure CosmosDb concerns

TL;DR caching can optimize RU consumption significantly. Due to the intrinsic
ability to mutate easily, the potential to integrate rolling snapshots into
core storage is clear. Providing ways to cache and snapshot matter a lot on
CosmosDb, as lowest-common-denominator queries loading lots of events cost in
performance and cash. The specifics of how you use the changefeed matters more
than one might thing from the CosmosDb high level docs.

Overview: CosmosDb has been in production for >5 years and is a mature Document
database. The initial DocumentDb offering is at this point a mere projected
programming model atop a generic Document data store. Its changefeed mechanism
affords a base upon which one can manage projections, but there is no directly
provided mechanism that lends itself to building Projections that map directly
to EventStore's facilities in this regard (i.e., there is nowhere to maintain
consumer offsets in the store itself).

Key aspects relevant to the Equinox programming model:

- CosmosDb has pervasive optimization feedback per call in the form of a
  Request Charge attached to each and every action. Working to optimize one's
  request charges per scenario is critical both in terms of the effect it has
  on the amount of Request Units/s one you need to pre-provision (which
  translates directly to costs on your bill), and then live predictably within
  if one is not to be throttled with 429 responses. In general, the request
  charging structure can be considered a very strong mechanical sympathy
  feedback signal

- Point reads of single documents based on their identifier are charged as 1 RU
  plus a price per KB and are optimal. Queries, even ones returning that same
  single document, have significant overhead and hence are to be avoided

- One key mechanism CosmosDb provides to allow one to work efficiently is that
  any point-read request where one supplies a valid `etag` is charged at 1 RU,
  regardless of the size one would be transferring in the case of a cache miss
  (the other key benefit of using this is that it avoids unnecessarily clogging
  of the bandwidth, and optimal latencies due to no unnecessary data transfers)

- Indexing things surfaces in terms of increased request charges; at scale,
  each indexing hence needs to be justified

- Similarly to EventStore, the default ARS encoding CosmosDb provides, together
  with interoperability concerns, means that straight json makes sense as an
  encoding form for events (UTF-8 arrays)

- Collectively, the above implies (arguably counter-intuitively) that using the
  powerful generic querying facility that CosmosDb provides should actually be
  a last resort.

- See [Cosmos Storage Model](#cosmos-storage-model) for further information on
  the specific encoding used, informed by these concerns.

- Because reads, writes _and updates_ of items in the Tip document are charged
  based on the size of the item in units of 1KB, it's worth compressing and/or
  storing snapshots outside of the Tip-document (while those factors are also a
  concern with EventStore, the key difference is their direct effect of charges
  in this case).

The implications of how the changefeed mechanism works also have implications
for how events and snapshots should be encoded and/or stored:

- Each write results in a potential cost per changefeed consumer, hence one
  should minimize changefeed consumers count

- Each update of a document can have the same effect in terms of Request
  Charges incurred in tracking the changefeed (each write results in a document
  "moving to the tail" in the consumption order - if multiple writes occur
  within a polling period, you'll only see the last one)

- The ChangeFeedProcessor presents a programming model which needs to maintain
  a position. Typically one should store that in an auxiliary collection in
  order to avoid feedback and/or interaction between the changefeed and those
  writes

It can be useful to consider keeping snapshots in the auxiliary collection
employed by the changefeed in order to optimize the interrelated concerns of
not reading data redundantly, and not feeding back into the oneself (although
having separate roundtrips obviously has implications).

<a name="cosmos-storage-model"></a>
# `Equinox.Cosmos` CosmosDB Storage Model

This article provides a walkthrough of how `Equinox.Cosmos` encodes, writes and
reads records from a stream under its control.

The code (see [source](src/Equinox.Cosmos/Cosmos.fs#L6)) contains lots of
comments and is intended to be read - this just provides some background.

## Batches

Events are stored in immutable batches consisting of:

- `p`artitionKey: `string` // stream identifier, e.g. "Cart-{guid}"
- `i`ndex: `int64` // base index position of this batch (`0` for first event in a stream)
- `n`extIndex: `int64` // base index ('i') position value of the next record in
  the stream - NB this _always_ corresponds to `i`+`e.length` (in the case of
  the `Tip` record, there won't actually be such a record yet)
- `id`: `string` // same as `i` (CosmosDb forces every item (document) to have one[, and it must be a `string`])
- `e`vents: `Event[]` // (see next section) typically there is one item in the
  array (can be many if events are small, for RU and performance/efficiency
  reasons; RU charges are per 1024 byte block)
- `ts` // CosmosDb-intrinsic last updated date for this record (changes when
  replicated etc, hence see `t` below)

## Events

Per `Event`, we have the following:

- `c`ase - the case of this union in the Discriminated Union of Events this
  stream bears (aka Event Type)
- `d`ata - json data (CosmosDb maintains it as actual json; you are free to
  index it and/or query based on that if desired)
- `m`etadata - carries ancillary information for an event; also json
- `t` - creation timestamp 

## Tip [Batch]

The _tip_ is always readable via a point-read, as the `id` has a fixed,
well-known value: `"-1"`). It uses the same base layout as the aforementioned
Batch (`Tip` *isa* `Batch`), adding the following:

- `id`: always `-1` so one can reference it in a point-read GET request and not
  pay the cost and latency associated with a full indexed query
- `_etag`: CosmosDb-managed field updated per-touch (facilitates `NotModified`
  result, see below)
- `u`: Array of _unfold_ed events based on a point-in-time _state_ (see _State,
  Snapshots, Events and Unfolds_, _Unfolded Events_  and `unfold` in the
  programming model section). Not indexed. While the data is json, the actual
  `d`ata and `m`etadata fields are compressed and encoded as base64 (and hence
  can not be queried in any reasonable manner).

## State, Snapshots, Events and Unfolds

In an Event Sourced system, we typically distinguish between the following
basic elements

- _Events_ - Domain Events representing real world events that have occurred
  (always past-tense; it's happened and is not up for debate), reflecting the
  domain as understood by domain experts - see [Event
  Storming](https://en.wikipedia.org/wiki/Event_storming). Examples: _The
  customer favorited the item_, _the customer add SKU Y to their saved for
  later list_, _A charge of $200 was submitted successfully with transaction id
  X_.

- _State_ - derived representations established from Events. A given set of
  code in an environment will, in service of some decision making process,
  interpret the Events as implying particular state changes in a model. If we
  change the code slightly or add a field, you wouldn't necessarily expect a
  version of your code from a year ago to generate you equivalent state that
  you can simply blast into your object model and go. (But you can easily and
  safely hold a copy in memory as long as your process runs as this presents no
  such interoperability or versioning concerns). State is not necessarily
  always serializable, nor should it be.

- _Snapshots_ - A snapshot is an intentionally roundtrippable version of a
  State, that can be saved and restored. Typically one would do this to save
  the (latency, roundtrips, RUs, deserialization and folding) cost of loading
  all the Events in a long running sequence of Events to re-establish the
  State. The
  [EventStore folks have a great walkthrough on Rolling Snapshots](https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html).

- Projections - the term projection is *heavily* overloaded, meaning anything
  from the proceeds of a SELECT statement, the result of a `map` operation, an
  EventStore projection to an event being propagated via Kafka (no, further
  examples are not required!).

.... and:

- Unfolds - the term `unfold` is based on the well known 'standard' FP function
  of that name, bearing the signature `'state -> 'event seq`. **=> For
  `Equinox.Cosmos`, one might say `unfold` yields _projection_ s as _event_ s
  to _snapshot_ the _state_ as at that _position_ in the _stream_**.

## Generating and saving `unfold`ed events

Periodically, along with writing the _events_ that a _decision function_ yields
to represent the implications of a _command_ given the present _state_, we also
`unfold` the resulting `state'` and supply those to the `Sync` function too.
The `unfold` function takes the `state` and projects one or more
snapshot-events that can be used to re-establish a state equivalent to that we
have thus far derived from watching the events on the stream. Unlike normal
events, `unfold`ed events do not get replicated to other systems, and can also
be jetisonned at will (we also compress them rather than storing them as fully
expanded json).

_NB, in the present implementation, `u`nfolds are generated, transmitted and
updated upon every write; this makes no difference from a Request Charge
perspective, but is clearly suboptimal due to the extra computational effort
and network bandwidth consumption. This will likely be optimized by exposing
controls on the frequency at which `unfold`s are triggered_

## Reading from the Storage Model

The dominant pattern is that reads request _Tip_  with an `IfNoneMatch`
precondition citing the _etag_ it bore when we last saw it. That, when combined
with a cache means one of the following happens when a reader is trying to
establish the _state_ of a _stream_ prior to processing a _Command_:

- `NotModified` (depending on workload, can be the dominant case) - for `1` RU,
  minimal latency and close-to-`0` network bandwidth, we know the present state

- `NotFound` (there's nothing in the stream) - for equivalently low cost (`1`
  RU), we know the _state_ is `initial`

- `Found` - (if there are multiple writers and/or we don't have a cached
  version) - for the minimal possible cost (a point read, not a query), we have
  all we need to establish the state:
    - `i`: a version number
    - `e`: events since that version number
    - `u`: unfolded (auxiliary) events computed at the same time as the batch
      of events was sent (aka informally as snapshots) - (these enable us to
      establish the `state` without further queries or roundtrips to load and
      fold all preceding events)

### Building a state from the Storage Model and/or the Cache

Given a stream with:

```json
[
    { "id":0, "i":0, "e": [{"c":"c1", "d":"d1"}]},
    { "id":1, "i":1, "e": [{"c":"c2", "d":"d2"}]},
    { "id":2, "i":2, "e": [{"c":"c2", "d":"d3"}]},
    { "id":3, "i":3, "e": [{"c":"c1", "d":"d4"}]},
    { "id":-1,
        "i": 4,
        "e": [{"i":4, "c":"c3", "d":"d5"}],
        "u": [{"i":4, "c":"s1", "d":"s5Compressed"}, {"i":3, "c":"s2", "d":"s4Compressed"}],
        "_etag": "etagXYZ"
    }
]
```

If we have `state4` based on the events up to `{i:3, c:c1, d: d4}` and the Tip
Item-document, we can produce the `state` by folding in a variety of ways:

- `fold initial [ C1 d1; C2 d2; C3 d3; C1 d4; C3 d5 ]` (but would need a query to load the first 2 batches, with associated RUs and roundtrips)
- `fold state4 [ C3 d5 ]` (only need to pay to transport the _tip_ document as a point read)
- (if `isOrigin (S1 s5)` = `true`): `fold initial [S1 s5]` (point read + transport + decompress `s5`)
- (if `isOrigin (S2 s4)` = `true`): `fold initial [S2 s4; C3 d5]` (only need to pay to transport the _tip_ document as a point read and decompress `s4` and `s5`)

If we have `state3` based on the events up to `{i:3, c:c1, d: d4}`, we can
produce the `state` by folding in a variety of ways:

- `fold initial [ C1 d1; C2 d2; C3 d3; C1 d4; C3 d5 ]` (but query, round-trips)
- `fold state3 [C1 d4 C3 d5]` (only pay for point read+transport)
- `fold initial [S2 s4; C3 d5]` (only pay for point read+transport)
- (if `isOrigin (S1 s5)` = `true`): `fold initial [S1 s5]` (point read + transport + decompress `s5`)
- (if `isOrigin (S2 s4)` = `true`): `fold initial [S2 s4; C3 d5]` (only need to pay to transport the _tip_ document as a point read and decompress `s4` and `s5`)

If we have `state5` based on the events up to `C3 d5`, and (being the writer,
or a recent reader), have the etag: `etagXYZ`, we can do a `HTTP GET` with
`etag: IfNoneMatch etagXYZ`, which will return `302 Not Modified` with < 1K of
data, and a charge of `1.00` RU allowing us to derive the state as:

- `state5`

See [Programming Model](#programing-model) for what happens in the application
based on the events presented.

## Sync stored procedure

This covers what the most complete possible implementation of the JS Stored
Procedure (see
[source](https://github.com/jet/equinox/blob/tip-isa-batch/src/Equinox.Cosmos/Cosmos.fs#L302))
does when presented with a batch to be written. (NB The present implementation
is slightly simplified; see [source](src/Equinox.Cosmos/Cosmos.fs#L303).

The `sync` stored procedure takes as input, a document that is almost identical
to the format of the _`Tip`_ batch (in fact, if the stream is found to be
empty, it is pretty much the template for the first document created in the
stream). The request includes the following elements:

- `expectedVersion`: the position the requester has based their [proposed]
  events on (no,
  [providing an `etag` to save on Request Charges is not possible in the Stored Proc](https://stackoverflow.com/questions/53355886/azure-cosmosdb-stored-procedure-ifmatch-predicate))
- `e`: array of Events (see Event, above) to append if, and only if, the
  expectedVersion check is fulfilled
- `u`: array of `unfold`ed events (aka snapshots) that supersede items with
  equivalent `c`ase values  
- `maxEvents`: the maximum number of events in an individual batch prior to
  starting a new one. For example:

  - if `e` contains 2 events, the _tip_ document's `e` has 2 events and the
    `maxEvents` is `5`, the events get appended onto the tip
  - if the total length including the new `e`vents would exceed `maxEvents`,
    the Tip is 'renamed' (gets its `id` set to `i.toString()`) to become a
    batch, and the new events go into the new Tip-Batch, the _tip_ gets frozen
    as a `Batch`, and the new request becomes the _tip_ (as an atomic
    transaction on the server side)

- (PROPOSAL/FUTURE) `thirdPartyUnfoldRetention`: how many events to keep before
  the base (`i`) of the batch if required by lagging `u`nfolds which would
  otherwise fall out of scope as a result of the appends in this batch (this
  will default to `0`, so for example if a writer says maxEvents `10` and there
  is an `u`nfold based on an event more than `10` old it will be removed as
  part of the appending process)
- (PROPOSAL/FUTURE): adding an `expectedEtag` would enable competing writers to
  maintain and update `u`nfold data in a consistent fashion (backing off and
  retrying in the case of conflict, _without any events being written per state
  change_)

## Equinox.Cosmos.Core.Events

The `Equinox.Cosmos.Core` namespace provides a lower level API that can be used
to manipulate events stored within a Azure CosmosDb using optimized native
access patterns.

The higher level APIs (i.e. not `Core`), as demonstrated by the `dotnet new`
templates are recommended to be used in the general case, as they provide the
following key benefits:

- Domain logic is store-agnostic, leaving it easy to:
  - Unit Test in isolation (verifying decisions produce correct events)
  - Integration test using the `MemoryStore`, where relevant

- Decouples encoding/decoding of events from the decision process of what
  events to write (means your Domain layer does not couple to a specific
  storage layer or encoding mechanism)
- Enables efficient caching and/or snapshotting (providing Equinox with `fold`,
  `initial`, `isOrigin`, `unfold` and a codec allows it to manage this
  efficiently)
- Provides Optimistic Concurrency Control with retries in the case of
  conflicting events

### Example Code

```fsharp

open Equinox.Cosmos.Core
// open MyCodecs.Json // example of using specific codec which can yield UTF-8
                      // byte arrays from a type using `Json.toBytes` via Fleece
                      // or similar

type EventData with
    static member FromT eventType value =
        EventData.FromUtf8Bytes(eventType, Json.toBytes value)

// Load connection sring from your Key Vault (example here is the CosmosDb
// simulator's well known key)
let connectionString: string =
    "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;"

// Forward to Log (you can use `Log.Logger` and/or `Log.ForContext` if your app
// uses Serilog already)
let outputLog = LoggerConfiguration().WriteTo.NLog().CreateLogger()
// Serilog has a `ForContext<T>()`, but if you are using a `module` for the
// wiring, you might create a tagged logger like this:
let gatewayLog =
    outputLog.ForContext(Serilog.Core.Constants.SourceContextPropertyName, "Equinox")

// When starting the app, we connect (once)
let connector : Equinox.Cosmos.Connector =
    Connector(
        requestTimeout = TimeSpan.FromSeconds 5.,
        maxRetryAttemptsOnThrottledRequests = 1,
        maxRetryWaitTimeInSeconds = 3,
        log = gatewayLog)
let cnx =
    connector.Connect("Application.CommandProcessor", Discovery.FromConnectionString connectionString)
    |> Async.RunSynchronously

// If storing in a single collection, one specifies the db and collection
// alternately use the overload that defers the mapping until the stream one is
// writing to becomes clear
let containerMap = Containers("databaseName", "containerName")
let ctx = Context(cnx, containerMap, gatewayLog)

//
// Write an event
//

let expectedSequenceNumber = 0 // new stream
let streamName, eventType, eventJson = "stream-1", "myEvent", Request.ToJson event
let eventData = EventData.fromT(eventType, eventJson) |> Array.singleton

let! res =
    Events.append
        ctx
        streamName 
        expectedSequenceNumber 
        eventData
match res with
| AppendResult.Ok -> ()
| c -> failwithf "conflict %A" c
```
# Access Strategies

An Access Strategy defines any optimizations regarding how one arrives at a
State of an Aggregate based on the Events stored in a Stream in a Store.

The specifics of an Access Strategy depend on what makes sense for a given
Store, i.e. `Equinox.Cosmos` necessarily has a significantly different set of
strategies than `Equinox.EventStore` (although there is an intersection).

Access Strategies only affect performance; you should still be able to infer
the state of the aggregate based on the `fold` of all the `events` ever written
on top of an `initial` state

NOTE: its not important to select a strategy until you've actually actually
modelled your aggregate, see [what if I change my access
strategy](#changing-access-strategy)

## `Equinox.Cosmos.AccessStrategy`

TL;DR `Equinox.Cosmos`: (see also: [the storage
model](cosmos-storage-model) for a deep dive, and [glossary,
below the table](#access-strategy-glossary) for definition of terms)
- keeps all the Events for a Stream in a single [CosmosDB _logical
  partition_](https://docs.microsoft.com/en-gb/azure/cosmos-db/partition-data)

- Transaction guarantees are provided at the logical partition level only. As
  for most typical Event Stores, the mechanism is based on
  [Optimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control).
  _There's no holding of a lock involved - it's based on conveying your
  premise alongside with the proposed change_; In terms of what we are
  doing, you observe a `state`, propose `events`, and the store is
  responsible for applying the change, or rejecting it if the `state` turns
  out to longer be the case when you get around to `sync`ing the change.
  The change is rejected if your premise (things have not changed since I
  saw them) is invalidated (whereupon you loop, working from the updated
  state).

- always has a special 'index' document (we term it the `Tip` document), per
  logical partition/stream that is accessible via an efficient _point read_ (it
  always has a CosmosDB `id` value of `"-1"`)

- Events are stored in Batches in immutable documents in the logical partition,
  the `Tip` document is the only document that's ever updated (it's _always_
  updated, as it happens...).

- As all writes touch the `Tip`, we have a natural way to invalidate any cached
  State for a given Stream; we retain the `_etag` of the `Tip` document, and
  updates (or consistent reads) are contingent on it not having changed. 

- The (optimistic) concurrency control of updates is by virtue of the fact that
  every update to the `Tip` touches the document _and thus alters (invalidates)
  the `_etag` value_. This means that, in contrast to how many SQL based stores
  (and most CosmosDB based ones) implement concurrency control, we don't rely
  on primary key constraint to prevent two writers writing conflicting events
  to the same stream.

 - A secondary benefit of not basing consistency control on a primary key
   constraint or equivalent, is that we no longer having to insert an Event
   every time we are updating something. (This fact is crucial for the
   `RollingState` and `Custom` strategies).

- The `interpret`/`decide` function is expected to deduplicate writes by not
  producing `events` if the `state` implies such updates would be redundant.
  Equinox does not have any internal mechanism to deduplicate events, thus
  having correct deduplication is the key to reducing round-trips and hence
  minimizing RU consumption (and the adverse effects that the retry cycles due
  to contention have, which will most likely arise when load is at its
  highest).

- The `unfolds` maintained in `Tip` have the bodies (the `d` and `m` fields) 1)
  deflated 2) base64 encoded (as everyone is reading the Tip, its worthwhile
  having the writer take on the burden of compressing, with the payback being
  that write amplification effects are reduced by readers paying less RUs to
  read them). The snapshots can be inspected securely via the `eqx` tool's
  `dump` facility, or _unsecurely_ online via the [**decode** button on
  `jgraph`'s `drawio-tools` at
  https://jgraph.github.io/drawio-tools/tools/convert.html, _if the data is not
  sensitive_](https://jgraph.github.io/drawio-tools/tools/convert.html).

- The `Tip` document, (and the fact we hold its `_etag` in our cache alongside
  the State we have derived from the Events), is at the heart of why consistent
  reads are guaranteed to be efficient (Equinox does the read of the `Tip`
  document contingent on the `_etag` not having changed; a read of any size
  costs only 1 RU if the result is `304 NOT Modified`)
- Specific Access Strategies:
  - define what we put in the `Tip`
  - control how we short circuit the process of loading all the Events and
    `fold`ing them from the start, if we encounter a Snapshot or Reset Event
  - allow one to post-process the events we are writing as required for reasons
    of optimization

### `Cosmos` Access Strategy overviews

| Strategy | TL;DR | `Tip` document maintains | Best suited for |
| :--- | :--- | :--- | :--- |
| `Unoptimized` | Keep Events only (but there's still an (empty) `Tip` document) | Count of event (and ability to have any insertion invalidate the cache for any reader) | No load, event counts or event sizes worth talking about. <br/> initial implementations. |
| `LatestKnownEvent` | Special mode for when every event is completely independent, so we completely short-circuit loading the events and folding them and instead only use the latest event (if any) | A copy of the most recent event, together with the count | 1) Maintaining a local copy of a Summary Event representing information obtained from a partner service that is authoritative for that data <br/> 2) Tracking changes to a document where we're not really modelling events as such, but with optimal efficiency (every read is a point read of a single document) |
| `Snapshot` | Keep a (single) snapshot in `Tip` at all times, guaranteed to include _all_ events | A single unfold produced by `toSnapshot` based on the `state'` (i.e., including any events being written) every time, together with the event count | Typical event-sourced usage patterns. <br/> Good default approach. <br/> Very applicable where you have lots of small 'delta' events that you only consider collectively. |
| `MultiSnapshot` | As per `Snapshot`, but `toSnapshots` can return arbitrary (0, one or many) `unfold`s | Multiple (consistent) snapshots (and `_etag`/event count for concurrency control as per other strategies) | Blue-green deployments where an old version and a new version of the app cannot share a single snapshot schema | 
| `RollingState` | "No event sourcing" mode - no events, just `state`. Perf as good as `Snapshot`, but don't even store the events so we will never hit CosmosDB stream size limits | `toSnapshot state'`: the squashed `state` + `events` which replace the incoming `state` | Maintaining state of an Aggregate with lots of changes <br/> a) that you don't need a record of the individual changes of yet <br/> b) you would like to model, test and develop as if one did <br/> DO NOT use if <br> a) you want to be able to debug state transitions by looking at individual change events <br/> b) you need to react to and/or project events relating to individual updates (CosmosDB does not provide a way to provide a notification of every single update, even if it does have a guarantee of always showing the final state on the Change Feed eventually)|
| `Custom` | General form that all the preceding strategies are implemented in terms of | Anything `transmute` yields as the `fst` of its result (but, typically the equivalent of what Snapshot writes) | Limited by your imagination, e.g. [emitting events once per hour but otherwise like `RollingState`](https://github.com/jet/propulsion/search?q=transmute&unscoped_q=transmute) |

<a name="access-strategy-glossary"></a>
#### Glossary
- `decide`/`interpret`: Application function that inspects a `state` to propose
  `events`, under control of a `Transact` loop.

- `Transact` loop: Equinox Core function that runs your `decide`/`interpret`,
  and then `sync`s any generated `events` to the Store.

- `state`: The (potentially cached) version of the State of the Aggregate that
  `Transact` supplied to your `decide`/ `interpret`.

- `events`: The changes the `decide`/`interpret` generated (that `Transact` is
  trying to `sync` to the Store).

- `fold`: standard function, supplied per Aggregate, which is used to apply
  Events to a given State in order to
  [`evolve`](http://thinkbeforecoding.github.io/FsUno.Prod/Dynamical%20Systems.html)
  the State per implications of each Event that has occurred

- `state'`: The State of an Aggregate (post-state in FP terms), derived from
  the current `state` + the proposed `events` being `sync`ed.

- `Tip`: Special document stored alongside the Events (in the same logical
  partition) which holds the `unfolds` associated with the current State of the
  _stream_.

- `sync`:
  [Stored procedure we use to manage consistent update of the Tip alongside insertion of Event Batch Documents](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#sync-stored-procedure)
  (contingent on the Tip's `_etag` not having changed)

- `unfold`: JSON objects maintained in the `Tip`, which represent Snapshots
  taken at a given point in the event timeline for this stream.
  - the `fold`/`evolve` function is presented Snapshots as if it was
    yet-another-Event
  - the only differences are
    1. they are not stored as in (immutable) Event documents as other Events are
    2. every write replaces all the `unfold`s in `Tip` with the result of the
      `toSnapshot`(s) function as defined in the given Access Strategy.
- `isOrigin`: A predicate function supplied to an Access Strategy that defines
  the starting point from which we'll build a `state`.
  - Must yield `true` for relevant Snapshots or Reset Events.
- `initial`: The (Application-defined) _state_ value all loaded events `fold`
  into, if an `isOrigin` event is not encountered while walking back through th
  `unfolds` and Events and instead hit the start of the stream.
- Snapshot: a single serializable representation of the `state'`
  - Facilitates optimal retrieval patterns when a stream contains a significant
    number of events
  - NOTE: Snapshots should not ever yield an observable difference in the
    `state` when compared to building it from the timeline of events; it should
    be solely a behavior-preserving optimization.
- Reset Event: an event (i.e. a permanent event, not a Snapshot) for which an
  `isOrigin` predicate yields `true`
  - e.g., for a Cart aggregate, a CartCleared event means there is no point in
    looking at _any_ preceding events in order to determine what's in the cart;
    we can start `fold`ing from that point.)
  - Multiple Reset Event Types are possible per Category, and a stream can
    often have multiple reset points (e.g., each time a Cart is `Cleared`, we
    enter a known state)
  - A _Tombstone Event_ can also be viewed as a Reset Event, e.g. if you have a
    (long running) bank account represented as a Stream per year, one might
    annually write a `TrancheCarriedForwardAndClosed` event which a) bears
    everything we care about (the final balance) b) signifies the fact that
    this tranche has now transitioned to read-only mode. Conversely, a `Closed`
    event is not by itself a _Tombstone Event_ - while you can infer the
    Open/Closed mode aspect of the Stream's State, you would still need to look
    further back through its history to be able to determine the balance that
    applied at the point the period was marked `Closed`. 

### `Cosmos` Read and Write policies

| Strategy | Reads involve | Writes involve |
| :--- | :--- | :--- |
| `Unoptimized` | Querying for, and `fold`ing all events (although the cache means it only reads events it has not seen) <br/> the `Tip` is never read, even e.g. if someone previously put a snapshot in there | 1) Insert a document with the events <br/> 2) Update `Tip` to reflect updated event count (as a transaction, as with all updates) |
| `LatestKnownEvent` | Reading the `Tip` (never the events) | 1) Inserting a document with the new event. <br/> 2) Updating the Tip to a) up count/invalidate the `_etag` b) CC the event for efficient access |
| `Snapshot` | 1) read Tip; stop if `isOrigin` accepts a snapshot from within <br/> 2) read backwards until the provided `isOrigin` function returns `true` for an Event, or we hit start of stream | 1) Produce proposed `state'` <br/> 2) write events to new document + `toSnapshot state'` result into Tip with new event count |
| `MultiSnapshot` | As per `Snapshot`, stop if `isOrigin` yields `true` for any `unfold` (then fall back to folding from base event or a reset event) | 1) Produce `state'` <br/> 2) Write events to new document + `toSnapshots state'` to `Tip` _(could be 0 or many, vs exactly one)_ |
| `RollingState` | Read `Tip` <br/> (can fall back to building from events as per `Snapshot` mode if nothing in Tip, but normally there are no events) | 1) produce `state'` <br/> 2) update `Tip` with `toSnapshot state'` <br/> 3) **no events are written** <br/> 4) Concurrency Control is based on the `_etag` of the Tip, not an expectedVersion / event count |
| `Custom` | As per `Snapshot` or `MultiSnapshot` <br/> 1) see if any `unfold`s pass the `isOrigin` test <br/> 2) Otherwise, work backward until a _Reset Event_ or start of stream | 1) produce `state'` <br/> 2) use `transmute events state` to determine a) the `unfold`s (if any) to write b) the `events` _(if any)_ to emit <br/> 3) execute the insert and/or upsert operations, contingent on the `_etag` of the opening `state` |

# Ideas

## Things that are incomplete and/or require work

This is a very loose laundry list of items that have occurred to us to do,
given infinite time. No conclusions of likelihood of starting, finishing, or
even committing to adding a feature should be inferred, but most represent
things that would be likely to be accepted into the codebase (please [read and]
raise Issues first though ;) ).

- Extend samples and templates; see
  [#57](https://github.com/jet/equinox/issues/57)

## Wouldn't it be nice - `Equinox`

- Performance tuning for non-store-specific logic; no perf tuning has been done
  to date (though some of the Store/Domain implementations do show
  perf-optimized fold implementation techniques). While in general the work is
  I/O bound, there are definitely opportunities to use `System.IO.Pipelines`
  etc, and the `MemoryStore` and `eqxtestbed` gives a good host drive this
  improvement.

## Wouldn't it be nice - `Equinox.EventStore`

EventStore, and it's Store adapter is the most proven and is pretty feature
rich relative to the need of consumers to date. Some things remain though:

- Provide a low level walking events in F# API akin to
  `Equinox.Cosmos.Core.Events`; this would allow consumers to jump from direct
  use of `EventStore.ClientAPI` -> `Equinox.EventStore.Core.Events` ->
  `Equinox.Stream` (with the potential to swap stores once one gets to using
  `Equinox.Stream`)
- Get conflict handling as efficient and predictable as for `Equinox.Cosmos`
  https://github.com/jet/equinox/issues/28
- provide for snapshots to be stored out of the stream, and loaded in a
  customizable manner in a manner analogous to
  [the proposed comparable `Equinox.Cosmos` facility](https://github.com/jet/equinox/issues/61).

## Wouldn't it be nice - `Equinox.Cosmos`

- Enable snapshots to be stored outside of the main collection in
  `Equinox.Cosmos` [#61](https://github.com/jet/equinox/issues/61)
- Multiple writers support for `u`nfolds (at present a `sync` completely
  replaces the unfolds in the Tip; this will be extended by having the stored
  proc maintain the union of the unfolds in play (both for semi-related
  services and for blue/green deploy scenarios); TBD how we decide when a union
  that's no longer in use gets removed)
  [#108](https://github.com/jet/equinox/issues/108)
- performance, efficiency and concurrency improvements based on
  [`tip-isa-batch`](https://github.com/jet/equinox/tree/tip-isa-batch) schema
  generalization [#109](https://github.com/jet/equinox/issues/109)
- performance improvements in loading logic
- Perf tuning of `JObject` vs `UTF-8` arrays and/or using a different
  serializer [#79](https://github.com/jet/equinox/issues/79)

## Wouldn't it be nice - `Equinox.DynamoDb`

- See [#76](https://github.com/jet/equinox/issues/76)
