# Documentation

Please refer to the [FAQ](README.md#FAQ), [README.md](README.md), the [Ideas](#ideas) and the [Issues](https://github.com/jet/equinox/issues) for background info on what's outstanding (aside from there being lots of room for more and better docs).

# Background reading

While there is no canonical book on doing event-sourced Domain Modelling, there are some must read books that stand alone as timeless books in general but are also specifically relevant to the considerations involved in building such systems:

_Domain Driven Design, Eric Evans, 2003_ aka 'The Blue Book'; not a leisurely read but timeless and continues to be authoritative

_Domain Modelling Made Functional, Scott Wlaschin, 2018_; extremely well edited traversal of Domain Modelling in F# which is a pleasure to read. Does not talk specifically about event sourcing, but is still very relevant.

_Implementing Domain Driven Design, Vaughn Vernon, 2013_; aka 'The Red Book'. Worked examples and a slightly different take on DDD (the appendix on Functional Event sourcing is not far from what we have around these parts; which is [not surprising given there's input from Jérémie Chassaing](https://github.com/thinkbeforecoding/FsUno.Prod))

- **Your link here** - Please add materials that helped you on your journey so far here via PRs!

# Glossary

Event Sourcing is easier _and harder_ than you think. This document is not a tutorial, and you can and will make a mess on your first forays. This glossary attempts to map terminology from established documentation outside to terms used in this documentation.

## Event-sourcing

Term | Description
-----|------------
Aggregate | Boundary within which a set of Invariants are to be preserved across a set of related Entities and Value Objects
Append | the act of adding Events reflecting a Decision to a Stream contingent on an Optimistic Concurrency Check
Command | Details representing intent to run a Decision process based on a Stream; may result in Events being Appended
CQRS | Command/Query Responsibility Segregation; Architectural principle critical to understand when building an Event Sourced System
Decision | Application logic which operates on a Command and a State. Can yield a response and/or Events to Append to the Stream in order to manifest the intent implied by the Command on Stream for this Aggregate - the rules it considers in doing so are in effect the Invariants of the Aggregate
Event | Details representing the facts of something that occurred, or a Decision that was made with regard to an Aggregate state as represented in a Stream
Eventually Consistent | A Read Model can momentarily lag a Stream's current State as events are being Projected
Fold | FP Term used to describe process of building State from the seqence of Events observed on a Stream
[Idempotent](https://en.wikipedia.org/wiki/Idempotence) | Can safely be processed >1 time without adverse effects
Invariants | Rules that an Aggregate's Fold and Decision process are together trying to uphold
Optimistic Concurrency Check | Locking/transaction mechanism used to ensure that Appends to a Stream maintain the Aggregate's Invariants, especially in the presence of multiple concurrent writers
Projection | Process whereby a Read Model tracks the State of Streams - lots of ways of skinning this cat, see Read Model, Query, Synchronous Query, Replication
Projector | Process tracking new Events, triggering Projection or Replication activities
Query | Eventually Consistent read from a Read Model
Synchronous Query | Consistent read direct from Stream State, breaking CQRS and coupling implementation to the Events used to support the Decision process
Reactions | Work carried out as a Projection which drives ripple effects arising from an Event being Appended
Read Model | Denormalized data maintained by a Projection for the purposes of providing a Read Model based on a Projection (honoring CQRS) See also Synchronous Query, Replication
Replication | Emitting Events pretty much directly as they are written (to support an Aggregate's Decision process) with a view to having a consumer traverse them to derive a Read Model or drive some form of synchronization process; aka Database Integration - building a Read Model or exposing Rich Events is preferred
Rich Events | Building a Projector that prepares a feed of events in the Bounded Context in a form that's not simply Replication (sometimes referred to a Enriching Events)
State | Information inferred from a Stream as part of a Decision process (or Synchronous Query)
Store | Holds a set of Streams
Stream | Ordered sequence of Events in a Store

## CosmosDb

Term | Description
-----|------------
Change Feed | set of query patterns allowing one to run a continuous query reading Items (documents) in a Range in order of their last update
Change Feed Processor | Library from Microsoft exposing facilities to Project from a Change Feed, maintaining Offsets per Physical Partition (Range) in the Lease Container
Container | logical space in a CosmosDb holding [loosely] related Items (aka Documents). Items bear logical Partition Keys. Formerly Collection. Can be allocated Request Units.
CosmosDb | Microsoft Azure's managed document database system
Database | Group of Containers. Can be allocated Request Units.
DocumentDb | Original offering of CosmosDb, now entitled the SQL Query Model, `Microsoft.Azure.DocumentDb.Client[.Core]`
Document id | Identifier used to load a document (Item) directly without a Query
Lease Container | Container, outside of the storage Container (to avoid feedback effects) that maintains a set of Offsets per Range, together with leases reflecting instances of the Change Feed Processors and their Range assignments (aka `aux` container)
Partition Key | Logical key identifying a Stream (maps to a Range)
Projector | Process running a [set of] Change Feed Processors across the Ranges of a Container in order to perform a global synchronization within the system across Streams
Query | Using indices to walk a set of relevant items in a Container, yielding Items (documents)
Range | Subset of the hashed key space of a collection held as a physical partition, can be split as part of scaling up the RUs allocated to a Container
Replay | The ability to re-run the processing of the Change Feed from the oldest Item (document) forward at will
Request Units | Virtual units representing max query processor capacity per second provisioned within CosmosDb to host a Range of a Container
Request Charge | Number of RUs charged for a specific action, taken from the RUs allocation for the relevant Range
Stored Procedure | JavaScript code stored in a collection that can translate an input request to a set of actions to be transacted as a group within CosmosDb. Incurs equivalent Request Charges for work performed; can chain to a continuation internally after a read or write.

## EventStore

Term | Description
-----|------------
Category | Group of Streams bearing a common `CategoryName-<id>` stream name
Event | json or blob representing an Event
EventStore | [Open source](https://eventstore.org) Event Sourcing-optimized data store server and programming model with powerful integrated projection facilities
Stream | Core abstraction presented by the API
WrongExpectedVersion | Low level exception thrown to communicate an Optimistic Concurrency Violation

## Equinox

Term | Description
-----|------------
Cache | `System.Net.MemoryCache` or equivalent holding _State_ and/or `etag` information for a Stream with a view to reducing roundtrips, latency and/or Request Charges
Rolling Snapshot | Event written to an EventStore stream in order to ensure minimal roundtrips to EventStore when there is a Cache miss
Unfolds | Snapshot information, represented as Events that are stored in an appropriate storage location (outside of a Stream's actual events) to minimize Queries and the attendant Request Charges when there is a Cache miss

# Programming Model

NB this has lots of room for improvement, having started as a placeholder in [#50](https://github.com/jet/equinox/issues/50); **improvements are absolutely welcome, as this is intended for an audience with diverse levels of familiarity with event sourcing in general, and Equinox in particular**.

In F#, independent of the Store being used, the Equinox programming model involves (largely by convention, see [FAQ](README.md#FAQ)), per aggregation of events on a given category of stream:

- `'state`: the rolling state maintained to enable Decisions or Queries to be made given a `'command` (not expected to be serializable or stored directly in a Store; can be held in a [.NET `MemoryCache`](https://docs.microsoft.com/en-us/dotnet/api/system.runtime.caching.memorycache))
- `'event`: a discriminated union representing all the possible Events from which a state can be `evolve`d (see `e`vents and `u`nfolds in the [Storage Model](#cosmos-storage-model)). Typically the mapping of the json to an `'event` `c`ase is [driven by a `UnionContractEncoder`](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/)

- `initial: 'state`: the [implied] state of an empty stream. See also [Null Object Pattern](https://en.wikipedia.org/wiki/Null_object_pattern), [Identity element](https://en.wikipedia.org/wiki/Identity_element)

- `fold : 'state -> 'event seq -> 'state`: function used to fold one or more loaded (or proposed) events (real ones and/or unfolded ones) into a given running [persistent data structure](https://en.wikipedia.org/wiki/Persistent_data_structure) of type `'state`
- `evolve: state -> 'event -> 'state` - the `folder` function from which `fold` is built, representing the application of a single delta that the `'event` implies for the model to the `state`. _Note: `evolve` is an implementation detail of a given Aggregate; `fold` is the function used in tests and used to parameterize the Category's storage configuration._. Sometimes named `apply`

- `interpret: 'state -> 'command -> event' list`: responsible for _Deciding_ (in an [idempotent](https://en.wikipedia.org/wiki/Idempotence) manner) how the intention represented by a `command` should (given the provided `state`) be reflected in terms of a) the `events` that should be written to the stream to record the decision b) any response to be returned to the invoker (NB returning a result likely represents a violation of the [CQS](https://en.wikipedia.org/wiki/Command%E2%80%93query_separation) and/or CQRS principles, [see Synchronous Query in the Glossary](#glossary))

When using a Store with support for synchronous unfolds and/or snapshots, one will typically implement two
further functions in order to avoid having every `'event` in the stream be loaded and processed in order to
build the `'state` per Decision or Query  (versus a single cheap point read from CosmosDb to read the _tip_):

- `isOrigin: 'event -> bool`: predicate indicating whether a given `'event` is sufficient as a starting point i.e., provides sufficient information for the `evolve` function to yield a correct `state` without any preceding event being supplied to the `evolve`/`fold` functions
- `unfold: 'state -> 'event seq`: function used to render events representing the `'state` which facilitate short circuiting the building of `state`, i.e., `isOrigin` should be able to yield `true` when presented with this `'event`. (in some cases, the store implementation will provide a custom `AccessStrategy` where the `unfold` function should only produce a single `event`; where this is the case, typically this is referred to as `toSnapshot : 'state -> 'event`).

## Decision Flow

When running a decision process, we have the following stages:

1. establish a known `'state` ([as at](https://www.infoq.com/news/2018/02/retroactive-future-event-sourced) a given Position in the stream of Events)
2. present the _request/command_ and the `state` to the `interpret` function in order to determine appropriate _events_ (can be many, or none) that represent the _decision_ in terms of events
3. append to the stream, _contingent on the stream still being in the same State/Position it was in step 1_

     a. if there is no conflict (nobody else decided anything since we decided what we'd do given that command and state), append the events to the stream (retaining the updated _position_ and _etag_)

     b. if there is a conflict, obtain the conflicting events [that other writers have produced] since the Position used in step 1, `fold` them into our `state`, and go back to 2 (aside: the CosmosDb stored procedure can send them back immediately at zero cost or latency, and there is [a proposal for EventStore to afford the same facility](https://github.com/EventStore/EventStore/issues/1652))

4. [if it makes sense for our scenario], hold the _state_, _position_ and _etag_ in our cache. When a Decision or Synchronous Query is needed, do a point-read of the _tip_ and jump straight to step 2 if nothing has been modified.

See [Cosmos Storage Model](#cosmos-storage-model) for a more detailed discussion of the role of the Sync Stored Procedure in step 3

## Canonical example Aggregate + Service

The following example is a minimal version of [the Favorites model](samples/Store/Domain/Favorites.fs), with shortcuts for brevity, that implements all the relevant functions above:

```fsharp
(* Event schemas *)

type Item = { id: int; name: string; added: DateTimeOffset } 
type Event =
    | Added of Item
    | Removed of itemId: int
    | Snapshotted of items: Item[]
let (|ForClientId|) (id: ClientId) = Equinox.AggregateId("Favorites", ClientId.toStringN id)

(* State types/helpers *)

type State = Item list // NB IRL don't mix Event and State types
let is id x = x.id = id

(* Folding functions to build state from events *)

let evolve state = function
    | Snapshotted items -> List.ofArray items
    | Added item -> item :: state
    | Removed id -> state |> List.filter (is id)
let fold state = Seq.fold evolve state

(* Decision Processing to translate a Command's intent to Events that would Make It So *)

type Command =
    | Add of Item // IRL do not use Event records in Command types
    | Remove of itemId: int

let interpret command state =
    let has id = state |> List.exits (is id)
    match command with
    | Add item -> if has item.id then [] else [Added item]
    | Remove id -> if has id then [Removed id] else []

(* Optional: Snapshot/Unfold-related functions to allow establish state efficiently,
   without having to read/fold all Events in a Stream *)

let toSnapshot state = [Event.Snapshotted (Array.ofList state)]

(* The Service defines operations in business terms with minimal reference to Equinox terms
   or need to talk in terms of infrastructure; typically the service is stateless and can be a Singleton *)

type Service(log, resolve, ?maxAttempts) =

    let resolve (Events.ForClientId streamId) = Equinox.Stream(log, resolve streamId, defaultArg maxAttempts 3)

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
```

<a name="api"></a>
# Equinox API Usage Guide

The most terse walkthrough of what's involved in using Equinox to do a Synchronous Query and/or Execute a Decision process is in the [Programming Model section](#programming-model). In this section we’ll walk through how one implements common usage patterns using the Equinox `Stream` API in more detail.

## ES, CQRS, Event-driven architectures etc

There are a plethora of [basics underlying Event Sourcing](https://eventstore.org/docs/event-sourcing-basics/index.html) and its cousin, the [CQRS architectural style](https://docs.microsoft.com/en-us/azure/architecture/guide/architecture-styles/cqrs). There are also myriad ways of arranging [event driven processing across a system](https://docs.microsoft.com/en-us/azure/architecture/guide/architecture-styles/event-driven).

The goal of [CQRS](https://martinfowler.com/bliki/CQRS.html) is to enable representation of the same data using multiple models. It’s [not a panacea, and most definitely not a top level architecture](https://www.youtube.com/watch?v=LDW0QWie21s&feature=youtu.be&t=448), but you should at least be [considering whether it’s relevant in your context](https://vladikk.com/2017/03/20/tackling-complexity-in-cqrs/). There are [various ways of applying the general CQRS concept as part of an architecture](https://docs.microsoft.com/en-us/azure/architecture/patterns/cqrs).

## Applying Equinox in an Event-sourced architecture

There are many trade-offs to be considered along the journey from an initial proof of concept implementation to a working system and evolving that over time to a successful and maintainable model. There is no official magical combination of CQRS and ES that is always correct, so it’s important to look at the following information as a map of all the facilities available - it’s certainly not a checklist; not all achievements must be unlocked.

At a high level we have:
- _Aggregates_ - a set of information (Entities and Value Objects) within which Invariants need to be maintained, leading us us to logically group them in order to consider them when making a Decision
- _Events_ - Events that have been accepted into the model as part of a Transaction represent the basic Facts from which State or Projections can be derived
- _Commands_ - taking intent implied by an upstream request or other impetus (e.g., automated synchronization based on an upstream data source) driving a need to sync a system’s model to reflect that implied need (while upholding the Aggregate's Invariants). The Decision process is responsible proposing Events to be appended to the Stream representing the relevant events in the timeline of that Aggregate.
- _State_ - the State at any point is inferred by _folding_ the events in order; this state typically feeds into the Decision with the goal of ensuring Idempotent handling (if its a retry and/or the desired state already pertained, typically one would expect the decision to be "no-op, no Events to emit")
- _Projections_ - while most State we'll fold from the events will be dictated by what we need (in addition to a Command's arguments) to be able to make the Decision implied by the command, the same Events that are _necessary_ for Command Handling to be able to uphold the Invariants can also be used as the basis for various summaries at the single aggregate level, Rich Events exposed as notification feeds at the boundary of the Bounded Context, and/or as denormalized representations forming a [Materialized View](https://docs.microsoft.com/en-us/azure/architecture/patterns/materialized-view).
- Queries - as part of the processing, one might wish to expose the state before or after the Decision and/or a computation based on that to the caller as a result. In its simplest form (just reading the value and emitting it without any potential Decision/Command even applying), such a _Synchronous Query_ is a gross violation of CQRS - reads should ideally be served from a Read Model_ 

## Programming Model walkthrough

### Core elements

In the code handling a given Aggregate’s Commands and Synchronous Queries, the code you write divide into:

- Events (`codec`, `encode`, `tryDecode`, `category`, `(|For|)` etc.)
- State/Fold (`evolve`, `fold`, `initial`)
- Storage Model helpers (`isOrigin`,`unfold`,`toSnapshot` etc)

while these are not omnipresent, for the purposes of this discussion we’ll treat them as that. See the [Programming Model](#programming-model) for a drilldown into these elements and their roles.

### Flows, Streams and Accumulators

Equinox’s Command Handling consists of < 200 lines including interfaces and comments in https://github.com/jet/equinox/tree/master/src/Equinox - the elements you'll touch in a normal application are:

- [`module Flow`](https://github.com/jet/equinox/blob/master/src/Equinox/Flow.fs#L34) - internal implementation of Optimistic Concurrency Control / retry loop used by `Stream`. It's recommended to at least scan this file as it defines the Transaction semantics everything is coming together in service of.
- [`type Stream`](https://github.com/jet/equinox/blob/master/src/Equinox/Equinox.fs#L11) - surface API one uses to `Transact` or `Query` against a specific stream
- [`type Target` Discriminated Union](https://github.com/jet/equinox/blob/master/src/Equinox/Equinox.fs#L42) - used to identify the Stream pertaining to the relevant Aggregate that `resolve` will use to hydrate a `Stream`
- _[`type Accumulator`](https://github.com/jet/equinox/blob/master/src/Equinox/Accumulator.fs) - optional `type` that can be used to manage application-local State in extremely complex flavors of Service__

Its recommended to read the examples in conjunction with perusing the code in order to see the relatively simple implementations that underlie the abstractions; the 3 files can tell many of the thousands of words about to follow!

#### Stream Members

```fsharp
type Equinox.Stream(stream, log, maxAttempts) =

    // Run interpret function with present state, retrying with Optimistic Concurrency
    member __.Transact(interpret : State -> Event list) : Async<unit>

    // Run decide function with present state, retrying with Optimistic Concurrency, yielding Result on exit
    member __.Transact(decide : State -> Result*Event list) : Async<Result>

    // Runs a Null Flow that simply yields a `projection` of `Context.State`
    member __.Query(projection : State -> View) : Async<View>
```

#### Accumulator

```fsharp
type Accumulator(fold, originState) =

    // Events proposed thus far
    member Accumulated : Event list

    // Effective State including `Accumulated`
    member State : State

    // Execute a normal Command-interpretation, stashing the Events in `Accumulated`
    member Transact(interpret : State -> Event list) : unit

    // Less frequently used variant that can additionally yield a result
    member Transact(decide : State -> 'result * Event list) : 'result
```

`Accumulator` is a small optional helper class that can be useful in certain scenarios where one is applying a sequence of Commands. One can use it within the body of a `decide` or `interpret` function as passed to `Stream.Transact`.

### Favorites walkthrough

In this section, we’ll use possibly the simplest toy example: an unbounded list of items a user has favorited (starred) in an e-commerce system.

See [samples/Tutorial/Favorites.fsx](samples/Tutorial/Favorites.fsx). It’s recommended to load this in Visual Studio and feed it into the F# Interactive REPL to observe it step by step. Here, we'll skip some steps and annotate some aspects with regard to trade-offs that should be considered.

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

Events are represented as an F# Discriminated Union; see the [article on the `UnionContractEncoder`](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/) for information about how that's typically applied to map to/from an Event Type/Case in an underlying Event storage system.

The `evolve` function is responsible for computing the post-State that should result from taking a given State and incorporating the effect that _single_ Event implies in that context and yielding that result _without mutating either input_.

While the `evolve` function operates on a `state` and a _single_ event, `fold` (named for the standard FP operation of that name) walks a chain of events, propagating the running state into each `evolve` invocation. It is the `fold` operation that's typically used a) in tests and b) when passing a function to an Equinox `StreamBuilder` to manage the behavior.

It should also be called out that Events represent Facts about things that have happened - an `evolve` or `fold` should not throw Exceptions or log. There should be absolutely minimal conditional logic.

In order to fulfill the _without mutating either input_ constraint, typically `fold` and `evolve` either deep clone to a new mutable structure with space for the new events, or use a [persistent data structure, such as F#'s `list`] [https://en.wikipedia.org/wiki/Persistent_data_structure,]. The reason this is necessary is that the result from `fold` can also be used for one of the following reasons:

- computing a 'proposed' state that never materializes due to a failure to save and/or an Optimistic Concurrency failure
- the store can sometimes take a `state` from the cache and `fold`ing in different `events` when the conflicting events are supplied after having been loaded for the retry in the loop
- concurrent executions against the stream may be taking place in parallel within the same process; this is permitted, Equinox makes no attempt to constrain the behavior in such a case

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

Command handling should almost invariably be implemented in an [Idempotent](https://en.wikipedia.org/wiki/Idempotence) fashion. In some cases, a blind append can argubaly be an OK way to do this, especially if one is doing simple add/removes that are not problematic if repeated or reordered. However it should be noted that:

- each write roundtrip (i.e. each `Transact`), and ripple effects resulting from all subscriptions having to process an event are not free either. As the cache can be used to validate whether an Event is actually necessary in the first instance, it's highly recommended to follow the convention as above and return an empty Event `list` in the case of a Command not needing to trigger events to move the model toward it's intended end-state
- understanding the reasons for each event typically yields a more correct model and/or test suite, which pays off in more understandable code
- under load, retries frequently bunch up, and being able to dedupicate them without hitting the store and causing a conflict can significantly reduce feedback effects that cause inordinate impacts on stability at the worst possible time

_It should also be noted that, when executing a Command, the `interpret` function is expected to behave statelessly; as with `fold`, multiple concurrent calls within the same process can occur.__

A final consideration to mention is that, even when correct idempotent handling is in place, two writers can still produce conflicting events. In this instance, the `Transact` loop's Optimistic Concurrency control will cause the 'loser' to re-`interpret` the Command with an updated `state` [incorporating the conflicting events the other writer (thread/process/machine) produced] as context

#### `Stream` usage

```fsharp
type Service(log, resolve, ?maxAttempts) =

    let streamFor (clientId: string) =
        let aggregateId = Equinox.AggregateId("Favorites", clientId)
        let stream = resolve aggregateId
        Equinox.Stream(log, stream, defaultArg maxAttempts 3)

    let execute clientId command : Async<unit> =
        let stream = streamFor clientId
        stream.Transact(interpret command)

    let read clientId : Async<string list> =
        let stream = streamFor clientId
        inner.Query id
```

The `Stream`-related functions in a given Aggregate establish the access patterns used across when Service methods access streams (see below). Typically these are relatively straightforward calls forwarding to a `Equinox.Stream` equivalent (see [`src/Equinox/Equinox.fs`](src/Equinox/Equinox.fs)), which in turn use the Optimistic Concurrency retry-loop  in [`src/Equinox/Flow.fs`](src/Equinox/Flow.fs).

`Read` above will do a roundtrip to the Store in order to fetch the most recent state (in `AllowStale` mode, the store roundtrip can be optimized out by reading through the cache). This Synchronous Read can be used to [Read-your-writes](https://en.wikipedia.org/wiki/Consistency_model#Read-your-writes_Consistency) to establish a state incorporating the effects of any Command invocation you know to have been completed.

`Execute` runs an Optimistic Concurrency Controlled `Transact` loop in order to effect the intent of the [write-only] Command. This involves:

a) establish state
b) use `interpret` to determine what (if any) Events need to be appended
c) submit the Events, if any, for appending
d) retrying b+c where there's a conflict (i.e., the version of the stream that pertained in (a) has been superseded)
e) after `maxAttempts` / `3` retries, a `MaxResyncAttemptsExceededException` is thrown, and an upstream can retry as necessary (depending on SLAs, a timeout may further constrain number of retries that can occur)

Aside from reading the various documentation regarding the concepts underlying CQRS, it's important to consider that (unless you're trying to leverage the Read-your-writes guarantee), doing reads direct from an event-sourced store is generally not considered a best practice (the opposite, in fact). Any state you surface to a caller is by definition out of date the millisecond you obtain it, so in many cases a caller might as well use an eventually-consistent version of the same state obtained via a [n eventually-consistent] Projection (see terms above).

All that said, if you're in a situation where your cache hit ratio is going to be high and/or you have reason to believe the underlying Event-Streams are not going to be long, pretty good performance can be achieved nonetheless; just consider that taking this shortcut _will_ impede scaling and, at worst, can result in you ending up with a model that's potentially both:

- overly simplistic - you're passing up the opportunity to provide a Read Model that directly models the requirement by providing a Materialized View
- unnecessarily complex - the increased complexity of the `fold` function and/or any output from `unfold` (and its storage cost) is a drag on one's ability to read, test, extend and generally maintain the Command Handling/Decision logic that can only live on the write side

#### `Service` Members

```fsharp
    member __.Favorite(clientId, sku) =
        execute clientId (Add sku)
    member __.Unfavorite(clientId, skus) =
        execute clientId (Remove skus)
    member __.List clientId : Async<string list> =
        stream.Read clientId
```

- while the Stream-related logic in the `Service` type can arguably be extracted into a `Stream` or `Handler` type in order to separate the stream logic from the business function being accomplished, its been determined in the course of writing tutorials and simply explaining what things do that the extra concept count does not present sufficient value. This can be further exacerbated by the need to hover and/or annotate in order to understand what types are flowing about.

- while the Command pattern can help clarify a high level flow, there's no subsitute for representing actual business functions as well-named methods representing specific behaviors that are meaningful in the context of the application's Ubiquitous Language, can be documented and tested.

- the `resolve` parameter affords one a sufficient [_seam_](http://www.informit.com/articles/article.aspx?p=359417) that facilitates testing independently with a mocked or stubbed Stream (without adding any references), or a `MemoryStore` (which does necessitate a reference to a separate Assembly for clarity) as desired. 

### Todo[Backend] walkthrough

See [the TodoBackend.com sample](README.md#TodoBackend) for reference info regarding this sample, and [the `.fsx` file from where this code is copied](samples/Tutorial/Todo.fsx). Note that the bulk if the design of the events stems from the nature of the TodoBackend spec; there are many aspects of the implementation that constitute less than ideal design; please note the provisos below...

#### `Event`s

```fsharp
type Todo = { id: int; order: int; title: string; completed: bool }
type Event =
    | Added     of Todo
    | Updated   of Todo
    | Deleted   of int
    | Cleared
    | Compacted of Todo[]
let (|ForClientId|) (id : string) = Equinox.AggregateId("Todos", id)
```

The fact that we have a `Cleared` Event stems from the fact that the spec defines such an operation. While one could implement this by emitting a `Deleted` event per currently existing item, there many reasons to do model this as a first class event:-
  i) Events should reflect user intent in its most direct form possible; if the user clicked Delete All, it's not the same to implement that as a set of individual deletes that happen to be united by having timestamp with a very low number of ms of each other.
  ii) Because the `Cleared` Event establishes a known State, one can have the `isOrigin` flag the event as being the furthest one needs to search backwards before starting to `fold` events to establish the state. This also prevents the fact that the stream gets long in terms of numbers of events from impacting the efficiency of the processing
  iii) While having a `Cleared` event happens to work, it also represents a technical trick in a toy domain and should not be considered some cure-all Pattern - real Todo apps don't have a 'declare bankruptcy' function. And example alternate approaches might be to represent each Todo list as it's own stream, and then have a `TodoLists` aggregate coordinating those.

The `Compacted` event is used to represent Rolling Snapshots (stored in-stream) and/or Unfolds (stored in Tip document-Item); For a real Todo list, using this facility may well make sense - the State can fit in a reasonable space, and the likely number of Events may reach an interesting enough count to justify applying such a feature
  i) it should also be noted that Caching may be a better answer - note `Compacted` is also an `isOrigin` event - there's no need to go back any further if you meet one.
  ii) we use an Array in preference to a [F#] `list`; while there are `ListConverter`s out there (notably not in [`FsCodec`](https://github.com/jet/FsCodec)), in this case an Array is better from a GC and memory-efficiency stance, and does not need any special consideration when using `Newtonsoft.Json` to serialize.

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

- for `State` we use records and `list`s as the state needs to be a Persistent data structure.
- in general the `evolve` function is straightforward idiomatic F# - while there are plenty ways to improve the efficiency (primarily by implementing `fold` using mutable state), in reality this would reduce the legibility and malleability of the code.

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

- Note `Add` does not adhere to the normal idempotency constraint, being unconditional. If the spec provided an id or token to deduplicate requests, we'd track that in the `fold` and use it to rule out duplicate requests.
- For `Update`, we can lean on structural equality in `when current <> value` to cleanly rule out redundant updates
- The current implementation is 'good enough' but there's always room to argue for adding more features. For `Clear`, we could maintain a flag about whether we've just seen a clear, or have a request identifier to deduplicate, rather than risk omitting a chance to mark the stream clear and hence leverage the `isOrigin` aspect of having the event.

#### `Service`

```fsharp
type Service(log, resolve, ?maxAttempts) =

    let resolve (ForClientId streamId) = Equinox.Stream(log, resolve streamId, defaultArg maxAttempts 3)

    let execute clientId command : Async<unit> =
        let stream = reolve clientId
        stream.Transact(interpret command)
    let handle clientId command : Async<Todo list> =
        let stream = reolve clientId
        stream.Transact(fun state ->
            let ctx = Equinox.Context(fold, state)
            ctx.Execute (interpret command)
            ctx.State.items,ctx.Accumulated) // including any events just pended
    let query clientId (projection : State -> T) : Async<T> =
        let stream = reolve clientId
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

- `handle` represents a command processing flow where we (idempotently) apply a command, but then also emit the state to the caller, as dictated by the needs of the call as specified in the TodoBackend spec. This uses the `Accumulator` helper type, which accumulates an `Event list`, and provides a way to compute the `state` incorporating the proposed events immediately.
- While we could theoretically use Projections to service queries from an eventually consistent Read Model, this is not in alignment with the Read-you-writes expectation embodied in the tests (i.e. it would not pass the tests), and, more importantly, would not work correctly as a backend for the app. Because we have more than one query required, we make a generic `query` method, even though a specific `read` method (as in the Favorite example) might make sense to expose too
- The main conclusion to be drawn from the Favorites and TodoBackend `Service` implementations's use of `Stream` Methods is that, while there can be commonality in terms of the sorts of transactions one might encapsulate in this manner, there's also It Depends factors; for instance:
  i) the design doesnt provide complete idempotency and/or follow the CQRS style
  ii) the fact that this is a toy system with lots of artificial constraints and/or simplifications when compared to aspects that might present in a more complete implementation.
- the `AggregateId` and `Stream` Active Patterns provide succinct ways to map an incoming `clientId` (which is not a `string` in the real implementation but instead an id using [`FSharp.UMX`](https://github.com/fsprojects/FSharp.UMX) in an unobtrusive manner.

# Equinox Architectural Overview

There are virtually unlimited ways to build an event-sourced model. It's critical that, for any set of components to be useful, that they are designed in a manner where one combines small elements to compose a whole, [versus trying to provide a hardwired end-to-end 'framework'](https://youtu.be/LDW0QWie21s?t=1928).

While going the library route leaves plenty seams needing to be tied together at the point of consumption (with resulting unavoidable complexity), it's unavoidable if one is to provide a system that can work in the real world.

This section outlines key concerns that the Equinox [Programming Model](#programming-model) is specifically taking a view on, and those that it is going to particular ends to leave open.

## Concerns leading to need for a programming model

F#, out of the box has a very relevant feature set for building Domain models in an event sourced fashion (DUs, persistent data structures, total matching, list comprehensions, async builders etc). However, there are still lots of ways to split the process of folding the events, encoding them, deciding events to produce etc.

In the general case, it doesnt really matter what way one opts to model the events, folding and decision processing.

However, given one has a specific store (or set of stores) in mind for the events, a number of key aspects need to be taken into consideration:

1. Coding/encoding events - Per aggregate or system, there is commonality in how one might wish to encode and/or deal with versioning of event representations. Per store, the most efficient way to bridge to that concern can vary. Per domain and encoding system, the degree to which one wants to unit or integration test this codec process will vary.

2. Caching - Per store, there are different tradeoffs/benefits for Caching. Per system, caching may or may not even make sense. For some stores, it makes sense to integrate caching into the primary storage.

3. Snapshotting - The store and/or the business need may provide a strong influence on whether or not (and how) one might employ a snapshotting mechanism.

## Store-specific concerns mapping to the programming model

This section enumerates key concerns feeding into how Stores in general, and specific concrete Stores bind to the [Programming Model](#programming-model):

### EventStore

TL;DR caching not really needed, storing snapshots has many considerations in play, projections built in 

Overview: EventStore is a mature and complete system, explicitly designed to address key aspects of building an event-sourced system. There are myriad bindings for multiple languages and various programming models. The docs present various ways to do snapshotting. The projection system provides ways in which to manage snapshotting, projections, building read models etc.

Key aspects relevant to the Equinox programming model:

- In general, EventStore provides excellent caching and performance characteristics intrinsically by virtue of its design
- Projections can be managed by either tailing streams (including the synthetic `$all` stream) or using the Projections facility - there's no obvious reason to wrap it, aside from being able to uniformly target CosmosDb (i.e. one could build an `Equinox.EventStore.Projection` library and an `eqx project stats es` with very little code).
- In general event streams should be considered append only, with no mutations or deletes
- For snapshotting, one can either maintain a separate stream with a maximum count or TTL rule, or include faux _Compaction_ events in the normal streams (to make it possible to combine reading of events and a snapshot in a single roundtrip). The latter is presently implemented in `Equinox.EventStore`
- While there is no generic querying facility, the APIs are designed in such a manner that it's generally possible to achieve any typically useful event access pattern needed in an optimal fashion (projections, catchup subscriptions, backward reads, caching)
- While EventStore allows either json or binary data, its generally accepted that json (presented as UTF-8 byte arrays) is a good default for reasons of interoperability (the projections facility also strongly implies json)

### Azure CosmosDb concerns

TL;DR caching can optimize RU consumption significantly. Due to the intrinsic ability to mutate easily, the potential to integrate rolling snapshots into core storage is clear. Providing ways to cache and snapshot matter a lot on CosmosDb, as lowest-common-denominator queries loading lots of events cost in performance and cash. The specifics of how you use the changefeed matters more than one might thing from the CosmosDb high level docs.

Overview: CosmosDb has been in production for >5 years and is a mature Document database. The initial DocumentDb offering is at this point a mere projected programming model atop a generic Document data store. Its changefeed mechanism affords a base upon which one can manage projections, but there is no directly provided mechanism that lends itself to building Projections that map directly to EventStore's facilities in this regard (i.e., there is nowhere to maintain consumer offsets in the store itself).

Key aspects relevant to the Equinox programming model:

- CosmosDb has pervasive optimization feedback per call in the form of a Request Charge attached to each and every action. Working to optimize one's request charges per scenario is critical both in terms of the effect it has on the amount of Request Units/s one you need to pre-provision (which translates directly to costs on your bill), and then live predictably within if one is not to be throttled with 429 responses. In general, the request charging structure can be considered a very strong mechanical sympathy feedback signal
- Point reads of single documents based on their identifier are charged as 1 RU plus a price per KB and are optimal. Queries, even ones returning that same single document, have significant overhead and hence are to be avoided
- One key mechanism CosmosDb provides to allow one to work efficiently is that any point-read request where one supplies a valid `etag` is charged at 1 RU, regardless of the size one would be transferring in the case of a cache miss (the other key benefit of using this is that it avoids unnecessarily clogging of the bandwidth, and optimal latencies due to no unnecessary data transfers)
- Indexing things surfaces in terms of increased request charges; at scale, each indexing hence needs to be justified
- Similarly to EventStore, the default ARS encoding CosmosDb provides, together with interoperability concerns, means that straight json makes sense as an encoding form for events (UTF-8 arrays)
- Collectively, the above implies (arguably counter-intuitively) that using the powerful generic querying facility that CosmosDb provides should actually be a last resort.
- See [Cosmos Storage Model](#cosmos-storage-model) for further information on the specific encoding used, informed by these concerns.
- Because reads, writes _and updates_ of items in the Tip document are charged based on the size of the item in units of 1KB, it's worth compressing and/or storing snapshots outside of the Tip-document (while those factors are also a concern with EventStore, the key difference is their direct effect of charges in this case).

The implications of how the changefeed mechanism works also have implications for how events and snapshots should be encoded and/or stored:

- Each write results in a potential cost per changefeed consumer, hence one should minimize changefeed consumers count
- Each update of a document can have the same effect in terms of Request Charges incurred in tracking the changefeed (each write results in a document "moving to the tail" in the consumption order - if multiple writes occur within a polling period, you'll only see the last one)
- The ChangeFeedProcessor presents a programming model which needs to maintain a position. Typically one should store that in an auxiliary collection in order to avoid feedback and/or interaction between the changefeed and those writes

It can be useful to consider keeping snapshots in the auxiliary collection employed by the changefeed in order to optimize the interrelated concerns of not reading data redundantly, and not feeding back into the oneself (although having separate roundtrips obviously has implications).
 
# Cosmos Storage Model

This article provides a walkthrough of how `Equinox.Cosmos` encodes, writes and reads records from a stream under its control.

The code (see [source](src/Equinox.Cosmos/Cosmos.fs#L6)) contains lots of comments and is intended to be read - this just provides some background.

## Batches

Events are stored in immutable batches consisting of:

- `p`artitionKey: `string` // stream identifier, e.g. "Cart-{guid}"
- `i`ndex: `int64` // base index position of this batch (`0` for first event in a stream)
- `n`extIndex: `int64` // base index ('i') position value of the next record in the stream - NB this _always_ corresponds to `i`+`e.length` (in the case of the `Tip` record, there won't actually be such a record yet)
- `id`: `string` // same as `i` (CosmosDb forces every item (document) to have one[, and it must be a `string`])
- `e`vents: `Event[]` // (see next section) typically there is one item in the array (can be many if events are small, for RU and performance/efficiency reasons; RU charges are per 1024 byte block)
- `ts` // CosmosDb-intrinsic last updated date for this record (changes when replicated etc, hence see `t` below)

## Events

Per `Event`, we have the following:

- `c`ase - the case of this union in the Discriminated Union of Events this stream bears (aka Event Type)
- `d`ata - json data (CosmosDb maintains it as actual json; you are free to index it and/or query based on that if desired)
- `m`etadata - carries ancillary information for an event; also json
- `t` - creation timestamp 

## Tip [Batch]

The _tip_ is always readable via a point-read, as the `id` has a fixed, well-known value: `"-1"`). It uses the same base layout as the aforementioned Batch (`Tip` *isa* `Batch`), adding the following:

- `id`: always `-1` so one can reference it in a point-read GET request and not pay the cost and latency associated with a full indexed query
- `_etag`: CosmosDb-managed field updated per-touch (facilitates `NotModified` result, see below)
- `u`: Array of _unfold_ed events based on a point-in-time _state_ (see _State, Snapshots, Events and Unfolds_, _Unfolded Events_  and `unfold` in the programming model section). Not indexed. While the data is json, the actual `d`ata and `m`etadata fields are compressed and encoded as base64 (and hence can not be queried in any reasonable manner).

# State, Snapshots, Events and Unfolds

In an Event Sourced system, we typically distinguish between the following basic elements

- _Events_ - Domain Events representing real world events that have occurred (always past-tense; it's happened and is not up for debate), reflecting the domain as understood by domain experts - see [Event Storming](https://en.wikipedia.org/wiki/Event_storming). Examples: _The customer favorited the item_, _the customer add SKU Y to their saved for later list_, _A charge of $200 was submitted successfully with transaction id X_.

- _State_ - derived representations established from Events. A given set of code in an environment will, in service of some decision making process, interpret the Events as implying particular state changes in a model. If we change the code slightly or add a field, you wouldn't necessarily expect a version of your code from a year ago to generate you equivalent state that you can simply blast into your object model and go. (But you can easily and safely hold a copy in memory as long as your process runs as this presents no such interoperability or versioning concerns). State is not necessarily always serializable, nor should it be.

- _Snapshots_ - A snapshot is an intentionally roundtrippable version of a State, that can be saved and restored. Typically one would do this to save the (latency, roundtrips, RUs, deserialization and folding) cost of loading all the Events in a long running sequence of Events to re-establish the State. The [EventStore folks have a great walkthrough on Rolling Snapshots](https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html).

- Projections - the term projection is *heavily* overloaded, meaning anything from the proceeds of a SELECT statement, the result of a `map` operation, an EventStore projection to an event being propagated via Kafka (no, further examples are not required!).

.... and:

- Unfolds - the term `unfold` is based on the well known 'standard' FP function of that name, bearing the signature `'state -> 'event seq`. **=> For `Equinox.Cosmos`, one might say `unfold` yields _projection_ s as _event_ s to _snapshot_ the _state_ as at that _position_ in the _stream_**.

## Generating and saving `unfold`ed events

Periodically, along with writing the _events_ that a _decision function_ yields to represent the implications of a _command_ given the present _state_, we also `unfold` the resulting `state'` and supply those to the `Sync` function too. The `unfold` function takes the `state` and projects one or more snapshot-events that can be used to re-establish a state equivalent to that we have thus far derived from watching the events on the stream. Unlike normal events, `unfold`ed events do not get replicated to other systems, and can also be jetisonned at will (we also compress them rather than storing them as fully expanded json).

_NB, in the present implementation, `u`nfolds are generated, transmitted and updated upon every write; this makes no difference from a Request Charge perspective, but is clearly suboptimal due to the extra computational effort and network bandwidth consumption. This will likely be optimized by exposing controls on the frequency at which `unfold`s are triggered_

# Reading from the Storage Model

The dominant pattern is that reads request _Tip_  with an `IfNoneMatch` precondition citing the _etag_ it bore when we last saw it. That, when combined with a cache means one of the following happens when a reader is trying to establish the _state_ of a _stream_ prior to processing a _Command_:

- `NotModified` (depending on workload, can be the dominant case) - for `1` RU, minimal latency and close-to-`0` network bandwidth, we know the present state
- `NotFound` (there's nothing in the stream) - for equivalently low cost (`1` RU), we know the _state_ is `initial`
- `Found` - (if there are multiple writers and/or we don't have a cached version) - for the minimal possible cost (a point read, not a query), we have all we need to establish the state:-
    `i`: a version number
    `e`: events since that version number
    `u`: unfolded (auxiliary) events computed at the same time as the batch of events was sent (aka informally as snapshots) - (these enable us to establish the `state` without further queries or roundtrips to load and fold all preceding events)

## Building a state from the Storage Model and/or the Cache

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

If we have `state4` based on the events up to `{i:3, c:c1, d: d4}` and the Tip Item-document, we can produce the `state` by folding in a variety of ways:

- `fold initial [ C1 d1; C2 d2; C3 d3; C1 d4; C3 d5 ]` (but would need a query to load the first 2 batches, with associated RUs and roundtrips)
- `fold state4 [ C3 d5 ]` (only need to pay to transport the _tip_ document as a point read)
- (if `isOrigin (S1 s5)` = `true`): `fold initial [S1 s5]` (point read + transport + decompress `s5`)
- (if `isOrigin (S2 s4)` = `true`): `fold initial [S2 s4; C3 d5]` (only need to pay to transport the _tip_ document as a point read and decompress `s4` and `s5`)

If we have `state3` based on the events up to `{i:3, c:c1, d: d4}`, we can produce the `state` by folding in a variety of ways:

- `fold initial [ C1 d1; C2 d2; C3 d3; C1 d4; C3 d5 ]` (but query, round-trips)
- `fold state3 [C1 d4 C3 d5]` (only pay for point read+transport)
- `fold initial [S2 s4; C3 d5]` (only pay for point read+transport)
- (if `isOrigin (S1 s5)` = `true`): `fold initial [S1 s5]` (point read + transport + decompress `s5`)
- (if `isOrigin (S2 s4)` = `true`): `fold initial [S2 s4; C3 d5]` (only need to pay to transport the _tip_ document as a point read and decompress `s4` and `s5`)

If we have `state5` based on the events up to `C3 d5`, and (being the writer, or a recent reader), have the etag: `etagXYZ`, we can do a `HTTP GET` with `etag: IfNoneMatch etagXYZ`, which will return `302 Not Modified` with < 1K of data, and a charge of `1.00` RU allowing us to derive the state as:

- `state5`

See [Programming Model](#programing-model) for what happens in the application based on the events presented.

# Sync stored procedure

This covers what the most complete possible implementation of the JS Stored Procedure (see [source](https://github.com/jet/equinox/blob/tip-isa-batch/src/Equinox.Cosmos/Cosmos.fs#L302)) does when presented with a batch to be written. (NB The present implementation is slightly simplified; see [source](src/Equinox.Cosmos/Cosmos.fs#L303).

The `sync` stored procedure takes as input, a document that is almost identical to the format of the _`Tip`_ batch (in fact, if the stream is found to be empty, it is pretty much the template for the first document created in the stream). The request includes the following elements:

- `expectedVersion`: the position the requester has based their [proposed] events on (no, [providing an `etag` to save on Request Charges is not possible in the Stored Proc](https://stackoverflow.com/questions/53355886/azure-cosmosdb-stored-procedure-ifmatch-predicate))
- `e`: array of Events (see Event, above) to append iff the expectedVersion check is fulfilled
- `u`: array of `unfold`ed events (aka snapshots) that supersede items with equivalent `c`ase values  
- `maxEvents`: the maximum number of events in an individual batch prior to starting a new one. For example:

  - if `e` contains 2 events, the _tip_ document's `e` has 2 events and the `maxEvents` is `5`, the events get appended onto the tip
  - if the total length including the new `e`vents would exceed `maxEvents`, the Tip is 'renamed' (gets its `id` set to `i.toString()`) to become a batch, and the new events go into the new Tip-Batch, the _tip_ gets frozen as a `Batch`, and the new request becomes the _tip_ (as an atomic transaction on the server side)

- (PROPOSAL/FUTURE) `thirdPartyUnfoldRetention`: how many events to keep before the base (`i`) of the batch if required by lagging `u`nfolds which would otherwise fall out of scope as a result of the appends in this batch (this will default to `0`, so for example if a writer says maxEvents `10` and there is an `u`nfold based on an event more than `10` old it will be removed as part of the appending process)
- (PROPOSAL/FUTURE): adding an `expectedEtag` would enable competing writers to maintain and update `u`nfold data in a consistent fashion (backing off and retrying in the case of conflict, _without any events being written per state change_)

# Equinox.Cosmos.Core.Events

The `Equinox.Cosmos.Core` namespace provides a lower level API that can be used to manipulate events stored within a Azure CosmosDb using optimized native access patterns.

The higher level APIs (i.e. not `Core`), as demonstrated by the `dotnet new` templates are recommended to be used in the general case, as they provide the following key benefits:

- Domain logic is store-agnostic, leaving it easy to:
  - Unit Test in isolation (verifying decisions produce correct events)
  - Integration test using the `MemoryStore`, where relevant

- Decouples encoding/decoding of events from the decision process of what events to write (means your Domain layer does not couple to a specific storage layer or encoding mechanism)
- Enables efficient caching and/or snapshotting (providing Equinox with `fold`, `initial`, `isOrigin`, `unfold` and a codec allows it to manage this efficiently)
- Provides Optimistic Concurrency Control with retries in the case of conflicting events

## Example Code

```fsharp

open Equinox.Cosmos.Core
// open MyCodecs.Json // example of using specific codec which can yield UTF-8 byte arrays from a type using `Json.toBytes` via Fleece or similar

type EventData with
    static member FromT eventType value = EventData.FromUtf8Bytes(eventType, Json.toBytes value)

// Load connection sring from your Key Vault (example here is the CosmosDb simulator's well known key)
let connectionString: string = "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;"

// Forward to Log (you can use `Log.Logger` and/or `Log.ForContext` if your app uses Serilog already)
let outputLog = LoggerConfiguration().WriteTo.NLog().CreateLogger()
// Serilog has a `ForContext<T>()`, but if you are using a `module` for the wiring, you might create a tagged logger like this:
let gatewayLog = outputLog.ForContext(Serilog.Core.Constants.SourceContextPropertyName, "Equinox")

// When starting the app, we connect (once)
let connector : Equinox.Cosmos.Connector =
    Connector(
        requestTimeout = TimeSpan.FromSeconds 5.,
        maxRetryAttemptsOnThrottledRequests = 1,
        maxRetryWaitTimeInSeconds = 3,
        log = gatewayLog)
let cnx = connector.Connect("Application.CommandProcessor", Discovery.FromConnectionString connectionString) |> Async.RunSynchronously

// If storing in a single collection, one specifies the db and collection
// alternately use the overload that defers the mapping until the stream one is writing to becomes clear
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

# Ideas

# Things that are incomplete and/or require work

This is a very loose laundry list of items that have occurred to us to do, given infinite time. No conclusions of likelihood of starting, finishing, or even committing to adding a feature should be inferred, but most represent things that would be likely to be accepted into the codebase (please [read and] raise Issues first though ;) ).

- Extend samples and templates; see [#57](https://github.com/jet/equinox/issues/57)

## Wouldn't it be nice - `Equinox`

- Performance tuning for non-store-specific logic; no perf tuning has been done to date (though some of the Store/Domain implementations do show perf-optimized fold implementation techniques). While in general the work is I/O bound, there are definitely opportunities to use `System.IO.Pipelines` etc, and the `MemoryStore` and `eqxtestbed` gives a good host drive this improvement.

## Wouldn't it be nice - `Equinox.EventStore`

EventStore, and it's Store adapter is the most proven and is pretty feature rich relative to the need of consumers to date. Some things remain though:

- Provide a low level walking events in F# API akin to `Equinox.Cosmos.Core.Events`; this would allow consumers to jump from direct use of `EventStore.ClientAPI` -> `Equinox.EventStore.Core.Events` -> `Equinox.Stream` (with the potential to swap stores once one gets to using `Equinox.Stream`)
- Get conflict handling as efficient and predictable as for `Equinox.Cosmos` https://github.com/jet/equinox/issues/28
- provide for snapshots to be stored out of the stream, and loaded in a customizable manner in a manner analogous to [the proposed comparable `Equinox.Cosmos` facility](https://github.com/jet/equinox/issues/61).

## Wouldn't it be nice - `Equinox.Cosmos`

- Enable snapshots to be stored outside of the main collection in `Equinox.Cosmos` [#61](https://github.com/jet/equinox/issues/61)
- Multiple writers support for `u`nfolds (at present a `sync` completely replaces the unfolds in the Tip; this will be extended by having the stored proc maintain the union of the unfolds in play (both for semi-related services and for blue/green deploy scenarios); TBD how we decide when a union that's no longer in use gets removed) [#108](https://github.com/jet/equinox/issues/108)
- performance, efficiency and concurrency improvements based on [`tip-isa-batch`](https://github.com/jet/equinox/tree/tip-isa-batch) schema generalization [#109](https://github.com/jet/equinox/issues/109)
- performance improvements in loading logic
- Perf tuning of `JObject` vs `UTF-8` arrays and/or using a different serializer [#79](https://github.com/jet/equinox/issues/79)

## Wouldn't it be nice - `Equinox.DynamoDb`

- See [#76](https://github.com/jet/equinox/issues/76)