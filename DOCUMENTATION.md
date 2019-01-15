Jet 😍 F# and Event Sourcing; open sourcing Equinox has been a long journey; we're _nearly_ there!

Maxim: They say you should always release while you're still embarrassed to do so; **we are**.
Solution: claim we've not yet released ;)

Please refer to the [FAQ](#FAQ) and [README.md](README.md) for background info and (**especially while we remain pr-release**), the [Roadmap](#roadmap) and excuses below.

----

TL;DR excuses, excuses, WIP, :under-construction:

Seriously: this is not released, it's a soft-launched open source repo at version 1.0; the 1.0 reflects (only) the fact its in production usage. There is absolutely an intention to make this be a proper open-source project; we're absolutely not making that claim right now. _It is **not** a proper open source project yet, unfortunately_.

- While [`dotnet new equinoxweb`](https://github.com/jet/dotnet-templates) does provide the option to include a full-featured [TodoBackend](https://todobackend.com) per the spec, a more complete sample application is needed; see [#57](https://github.com/jet/equinox/issues/57)
- there is no proper step by step tutorial showing code and stored representations interleaved (there is a low level spec-example in [the docs](DOCUMENTATION.md#Programming-Model) in the interim)
- There is a placeholder [Roadmap](#roadmap) for now, which is really an unordered backlog.
- As noted in the contributing section, we're simply not ready yet (we have governance models in place; this is purely a matter of conserving bandwidth, prioritising getting the system serviceable in terms of samples and documentation in advance of inviting people to evaluate)...

# FAQ

## What _is_ Equinox?

OK, I've read the README and the tagline. I still don't know what it does! Really, what's the TL;DR ?

- it supports storing events in [EventStore](https://eventstore.org), including working with existing data you may have (that's where it got its start)
- it includes a proprietary optimized Store implementation that only needs an empty Azure CosmosDb collection to get going
- it provides all the necessary infrastructure to build idempotent synchronous command processing against all of the stores; your Domain code intentionally doesn't need to reference *any* Equinox modules whatsoever (although for smaller systems, you'll often group `Events`+`Folds`+`Commands`+`Handler`+`Service` in a single `module`, which implies a reference to [the core `Equinox` package](https://github.com/jet/equinox/tree/master/src/Equinox)).
- following on from the previous point, you just write the unit tests without any Equinox-specific hoops to jump through (this really works very well indeed, assuming you're writing the domain code and the tests in F#. If you're working in a more verbose language, you may end up building some test helpers. We don't envisage Equinox mandating a specific pattern on the unit testing side (consistent naming such as `Events.Event`+`evolve`+`fold`+`Command`+`interpret` can help though).)
- it helps with integration testing decision processes by
  a) staying out of your way as much as possible
  b) providing an in-memory store that implements the same interface as the EventStore and CosmosDb stores do
- it does not [presently] run non-synchronous projections (see later in this guide for more on this)

## Should I use Equinox to learn event sourcing ?

You _could_. However the Equinox codebase here is not designed to be a tutorial; it's also extracted from systems with no pedagogical mission whatsoever. [FsUno.Prod](https://github.com/thinkbeforecoding/FsUno.Prod) on the other hand has this specific intention, walking though it is highly recommended. Also [EventStore](https://eventstore.org/), being a widely implemented and well-respected open source system has some excellent learning materials and documentation with a large usage community (search for `DDD-CQRS-ES` mailing list and slack).

Having said that, we'd love to see a set of tutorials written by people looking from different angles, and over time will likely do one too ... there's no reason why the answer to this question can't become "**of course!**"

## Can I use it for really big projects?

You can. Folks in Jet do; we also have systems where we have no plans to use it, or anything like it. That's OK; there are systems where having precise control over one's data access is critical. And (shush, don't tell anyone!) some find writing this sort of infrastructure to be a very fun design challenge that beats doing domain modelling any day ...

## Can I use it for really small projects and tiny microservices?

You can. Folks in Jet do; but we also have systems where we have no plans to use it, or anything like it as it would be overkill even for people familiar with Equinox.

## OK, but _should_ I use Equinox for a small project ?

You'll learn a lot from building your own equivalent wrapping layer. Given the array of concerns Equinox is trying to address, there's no doubt that a simpler solution is always possible if you constrain the requirements to specifics of your context with regard to a) scale b) complexity of domain c) degree to which you use or are likely to use >1 data store. You can and should feel free to grab slabs of Equinox's implementation and whack it into an `Infrastructure.fs` in your project too (note you should adhere to the rules of the [Apache 2 license](LICENSE)). If you find there's a particular piece you'd really like isolated or callable as a component and it's causing you pain as [you're using it over and over in ~ >= 3 projects](https://en.wikipedia.org/wiki/Rule_of_three_(computer_programming)), please raise an Issue though ! 

Having said that, getting good logging, some integration tests and getting lots of off-by-one errors off your plate is nice; the point of [DDD-CQRS-ES](https://ddd-cqrs-es.slack.com/) is to get beyond toy examples to the good stuff - Domain Modelling on your actual domain.

## What is the scope of Equinox; what features have been consciously omitted?

While it's not intended to be complete, try the [Roadmap](#roadmap). TL;DR The current codebase does not have anything to say about projections, aside from the [`unfold`s feature](DOCUMENTATION.md#Cosmos-Storage-Model), which provides _synchronous_ projections/snapshots on CosmosDb (and the integrated in-stream Rolling Snapshots on EventStore, although if you're using EventStore heavily you won't want to ignore its strong capabilities in this space).

## What will Equinox _never_ do?

Hard to say; try us, raise an Issue.

## What client languages are supported ?

The main language in mind for consumption is of course F# - many would say that F# and event sourcing are a dream pairing; little direct effort has been expended polishing it to be comfortable to consume from other .NET languages - but it's definitely not ruled out, and a small bit of polish and some samples should be able to make it just as good to use from other .NET languages.

## You say I can use volatile memory for integration tests, could this also be used for learning how to get started building event sourcing programs with equinox? 

The `MemoryStore` backend is intended to implement the complete semantics of a durable store [aside from caching, which would be a pyrrhic victory if implemented like in the other Stores, though arguably it may make sense should the caching layer ever get pushed out of the Stores themselves]. The main benefit of using it is that any tests using it have zero environment dependencies. In some cases this can be very useful for demo apps or generators (rather than assuming a specific store at a specific endpoint and/or credentials, there is something to point at which does not require configuration or assumptions.). The problem of course is that it's all in-process; the minute you stop the host, your TODO list has been forgotten. In general, EventStore is a very attractive option for prototyping; the open source edition is trivial to install and has a nice UI that lets you navigate events being produced etc.

## OK, so it supports CosmosDb, EventStore and might even support more in the future. I really don't intend to shift datastores. Period. Why would I take on this complexity only to get the lowest common denominator ?

Yes, you have decisions to make; Equinox is not a panacea - there is no one size fits all. While the philosophy of Equinox is a) provide an opinionated store-neutral [Programming Model](DOCUMENTATION.md#Programming-Model) with a good pull toward a big [pit of success](https://blog.codinghorror.com/falling-into-the-pit-of-success/), while not closing the door to using store-specific features where relevant, having a dedicated interaction is always going to afford you more power and control.

## Why do I need two caches if I have two stores?

- in general, individual apps will not typically be mixing data stores in the first instance
- see [The Rule of Three](https://en.wikipedia.org/wiki/Rule_of_three_(computer_programming)); the commonality may reveal itself better at a later point, but the cut and paste (with a cut and paste of the the associated acceptance tests) actually keeps the cache integration clearer at the individual store level for now. No, it's not set in stone ;)

## Is there a guide to building the simplest possible hello world "counter" sample, that simply counts with an add and a subtract event? 

There's a skeleton one in [#56](https://github.com/jet/equinox/issues/56), but your best choices are probably to look at the `Aggregate.fs` and `TodoService.fs` files emitted by [`dotnet new equinoxweb`](https://github.com/jet/dotnet-templates)

## OK, but you didn't answer my question, you just talked about stuff you wanted to talk about!

😲Please raise a question-Issue, and we'll be delighted to either answer directly, or incorporate the question and answer here

# Roadmap

# Very likely to happen and/or people looking at it:

- Extend samples and templates; see [#57](https://github.com/jet/equinox/issues/57)
- Enable snapshots to be stored outside of the main collection in `Equinox.Cosmos` see [#61](https://github.com/jet/equinox/issues/61)
- `Equinox.Cosmos.Projector` - helpers to consume events from the Azure DocumentDb Changefeed (no blueprints as yet, but [skeleton code in a branch](https://github.com/jet/equinox/tree/projection))
    - Maybe fully wired Kafka feeding example?

# Things that are incomplete and/or require work

This is a very loose laundry list of items that have occurred to us to do, given infinite time. No conclusions of likelihood of starting, finishing, or even committing to adding a feature should be inferred, but most represent things that would be likely to be accepted into the codebase (please raise Issues first though ;) ).

## Wouldn't it be nice - `Equinox.EventStore`:

EventStore, and it's Store adapter is the most proven and is pretty feature rich relative to the need of consumers to date. Some things remain though:

- Provide a low level walking events in F# API akin to `Equinox.Cosmos.Core.Events`; this would allow consumers to jump from direct use of `EventStore.ClientAPI` -> `Equinox.EventStore.Core.Events` -> `Equinox.Handler` (with the potential to be able to use alternate stores once one gets to using `Equinox.Handler` (which is store Agnostic))
- Get conflict handling as efficient and predictable as for `Equinox.Cosmos` https://github.com/jet/equinox/issues/28
- provide for snapshots to be stored out of the stream, and loaded in a customizable manner in a manner analogous to [the proposed comparable `Equinox.Cosmos` facility](https://github.com/jet/equinox/issues/61).

## Wouldn't it be nice - `Equinox`:

- While the plan is to support linux and MacOS, there are skipped tests wrt Mono etc. In terms of non-Windows developer experience, there's plenty scope for improvement.
- Performance tuning for non-store-specific logic; no perf tuning has been done to date (though some of the Store/Domain implementations do show perf-optimized fold implementation techniques). While in general the work is I/O bound, there are definitely opportunities to use `System.IO.Pipelines` etc, and the `MemoryStore` and CLI gives a good testbed to drive this improvement.

## Wouldn't it be nice - `Equinox.Cosmos`:

- Multiple writers support for `u`nfolds (at present a `sync` completely replaces the unfolds in the Tip; this will be extended by having the stored proc maintain the union of the unfolds in play (both for semi-related services and for blue/green deploy scenarios); TBD how we decide when a union that's no longer in use gets removed)
- performance improvements in loading logic
- `_etag`-based consistency checks?
- Perf tuning of `JObject` vs `UTF-8` arrays

## Wouldn't it be nice - `Equinox.Projection`, `Equinox.Projector`, `Equinox.Projection.*`:

- pre-built projector providing a consistent featureset, adapted to each individual Store

## Wouldn't it be nice - `Equinox.SqlStreamStore`: See [#62](https://github.com/jet/equinox/issues/62)

# Architectural Overview

There are virtually unlimited ways to build an event-sourced model. It's critical that, for any set of components to be useful, that they are designed in a manner where one combines small elements to compose a whole, [versus trying to provide a hardwired end-to-end 'framework'](https://youtu.be/LDW0QWie21s?t=1928).

While going the library route leaves plenty seams needing to be tied together at the point of consumption (with resulting unavoidable complexity), it's unavoidable if one is to provide a system that can work in the real world.

This section outlines key concerns that the Equinox [Programming Model](#programming-model) is specifically taking a view on, and those that it is going to particular ends to leave open.

## Concerns leading to need for a programming model

F#, out of the box has a very relevant featureset for building Domain models in an event sourced fashion (DUs, persistent data structures, total matching, list comprehensions, async builders etc). However, there are still lots of ways to split the process of folding the events, encoding them, deciding events to produce etc.

In the general case, it doesnt really matter what way one opts to model the events, folding and decision processing.

However, given one has a specific store (or set of stores) in mind for the events, a number of key aspects need to be taken into consideration:

1. Coding/encoding events - Per aggregate or system, there is commonality in how one might wish to encode and/or deal with versioning of event representations. Per store, the most efficient way to bridge to that concern can vary. Per domain and encoding system, the degree to which one wants to unit or integration test this codec process will vary.

2. Caching - Per store, there are different tradeoffs/benefits for Caching./ Per system, caching may or may not even make sense. For some stores, it makes sense to integrate caching into the primary storage.

3. Snapshotting - The store, and/or the business need may provide a strong influence on whether or not (and how) one might employ a snapshotting mechanism. 

## Store-specific concerns mapping to the programming model

This sections enumerates key concerns feeding into how the Store binds to the [Programming Model](#programming-model):

#### EventStore

TL;DR caching not really needed, storing snapshots has many considerations in play, projections built in 

Overview: EventStore is a mature and complete system, explicitly designed to address key aspects of building an event-sourced system. There are myriad bindings for multiple languages and various programming models. The docs present various ways to do snapshotting. The projection system provides ways in which to manage snapshotting. 

Key aspects relevant to the Equinox programming model:
- In general, EventStore provides excellent caching and performance characteristics intrinsically by virtue of it's design
- Projections can be managed by either tailing streams (including the syntethetic `$all` stream) or using the Projections facility - there's no obvious reason to wrap it for any particular reason
- In general event streams should be considered append only, with no mutations or deletes
- For snapshotting, one can either maintain a separate stream with a maximum count or TTL rule, or include faux _Compaction_ events in the normal streams (to make it possible to combine reading of events and a snapshot in a single roundtrip)
- While there is no generic querying facilitit, the APIs are designed in such a manner that it's generally possible to achieve any typically useful event access pattern needed in an optimal fashion (rendeirng that moot)
- While EventStore allows either json or binary data, its generally accepted that json (presented as UTF-8 byte arrays) is a good default for reasons of interoperability (the projections facility also strongly implies json)

#### Azure CosmosDb

TL;DR caching can optimize RU consumption significantly. Due to ability to mutate easily, potential to integrate rolling snapshots into core storage is provided. Providing ways to cache and snapshot matter a lot on CosmosDb, as lowest-common-demominator queries loading lots of events cost in performance and cash. The specifics of how you use the changefeed matters more than one might thing from the CosmosDb high level docs.

Overview: CosmosDb has been in production for >5 years and is a mature Document database. The initial DocumentDb offering is at this point a mere projected programming model atop a generic Document data store. Its changefeed mechanism affords one a base upon which one can manage projections, but there is no directly provided mechanism which lends itself to building projections that map directly to EventStore's facilties in this regard.

Key aspects relevant to the Equinox programming model:
- CosmosDb has pervasive optimization feedback per call in the form of a Request Charge attached to each and every action. Working to optimize one's request charges per scenario is critical both in terms of the effect it has on the amount of Request Units/s one you need to preprovision (which translates directly to costs on your bill), and then live predictably within if one is not to be throttled with 429 responses. In general, the request charging structure can be considered a very strong mechanical sympathy feedback signal.
- Point reads of single documents based on their identifier are charged as 1 RU plus a price per KB and are optimal. Queries, even ones returning that same single document, have significant overhead and hence are to be avoided
- One key mechanism CosmosDb provides to allow one to work efficiently is that any point-read request where one supplies a valid `etag` is charged at 1 RU, regardless of the size one would be transferring in the case of a cache miss (the other key benefit of using this is that it avoids unecessarly clogging of the bandwidth, and optimal latencies due to no unnecessary data transfers)
- Indexing things surfaces in terms of increased request charges; at scale, each indexing hence needs to be justified
- Similarly to EventStore, the default ARS encoding CosmosDb provides, together with interoperability concerns, means that straight json makes sense as an encoding form for events (UTF-8 arrays)
- Collectively, the above implies that, counterintuitively, using the powerful generic querying facility that CosmosDb provides should actually be a last resort.
- See [Cosmos Storage Model](#cosmos-storage-model) for further information on the specific encoding used, informed by these concerns.
- Because reads, writes _and updates_ of items in the Tip document are charged based on the size of the document in units of 1KB, it's worth compressing and/or storing snapshots ouside of the Tip-document (while those factors are also a concern with EventStore, the key difference is their direct effect of charges in this case).

The implications of how the changefeed mechanism works also have implications for how events and snapshots should be encoded and/or stored.
- Each write results in a potential cost per changefeed consumer, hence one should minimize changefeed consumers count
- Each update of a document can have the same effect in terms of Request Charges incurred in tracking the changefeed (each write results in a document "moving to the tail" in the consumption order - if multiple writes occur within a polling period, you'll only see the last one)
- The changefeed presents a programming model which stores a position. Typically one should store that in an auxilliary collection in order to avoid feedback and/or interaction between the changefeed and those writes

It can be useful to consider keeping snapshots in the auxilliary collection employed by the changefeed in order to optimize the interrelated concerns of not reading data redundantly, and not feeding back into the oneself (although having separate roundtrips obviously implications).
 
# Programming Model

NB this is long and needs _lots_ of editing, having started as a placeholder in [#50](https://github.com/jet/equinox/issues/50). The original article mixed two concerns: the programming model, and the `Equinox.Cosmos` implementation of the store; Some aspects have been been moved into [Cosmos Storage Model](#cosmos-storage-model), but both sides are still far from complete; don't look for a discernible story arc ;) **edits are absolutely welcome, as this is intended for an audience with diverse levels fo familiarity with event sourcing in general, and Equinox in particular**.

In F#, the Equinox programming model involves (largely by convention, see [FAQ](#FAQ)), per aggregation of events on a given category of stream:

* `'state`: the state required to support the Decision or Query being supported (not expected to be serializable or stored directly in a Store; can be held in a [.NET `MemoryCache`](https://docs.microsoft.com/en-us/dotnet/api/system.runtime.caching.memorycache))
* `initial: 'state`: the [implied] state of an empty stream
* `'event`: a discriminated union representing all the possible Events from which a state be `evolve`d (see `e`vents and `u`nfolds in the data model). Typically the mapping of the json to an `'event` `c`ase is [driven by a `UnionContractEncoder`](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/)
* `fold : 'state -> 'event seq -> 'state`: function used to fold one or more loaded (or proposed) events (real ones and/or unfolded ones) into a given running [persistent data structure](https://en.wikipedia.org/wiki/Persistent_data_structure) of type `'state`
* `evolve: state -> 'event -> 'state` - the `folder` function from which `fold` is built, representing the application of the delta the `'event` implies for the model to the `state`
* `interpret: 'state -> 'command -> event' list`: responsible for _deciding_ (in an [idempotent](https://en.wikipedia.org/wiki/Idempotence) manner) how the intention represented by a `command` should (given the provided `state`) be interpret in terms of a) the `events` that should be written to the stream to record the decision b) any response to be returned to the invoker (NB returning a result likely represents a violation of the [CQS](https://en.wikipedia.org/wiki/Command%E2%80%93query_separation) and/or CQRS principles)

When using a Store with support for synchronous unfolds and/or snapshots, one will typically implement two further functions in order to avoid having to have every `'event` in the stream having to be loaded and processed in order to build the `'state` (versus a single cheap point read from CosmosDb to read the _tip_):

* `isOrigin: 'event -> bool`: predicate indicating whether a given `'event` is sufficient as a starting point i.e., provides sufficient information for the `evolve` function to yield a correct `state` without any preceding event being supplied to the `evolve`/`fold` functions
* `unfold: 'state -> 'event seq`: function used to render events representing the state which facilitate short circuiting the building of `state`, i.e., some `isOrigin` should be able to yield `true` when presented with this `'event`. Note, in some cases, the store implementation will provide a custom AccessStrategy where the `unfold` function should only produce a single `event`; where this is the case, typically this is referred to as `compact : 'state -> 'event`.

## High level Command Processing flow

When running a decision process, we have the following stages:

1. establish a known `'state` ([as at](https://www.infoq.com/news/2018/02/retroactive-future-event-sourced) a given Position in the stream of Events)
2. present the _request/command_ and the `state` to the `interpret` function in order to determine appropriate _events_ (can be many, or none) that represent the _decision_ in terms of events
3. append to the stream, _contingent on the stream still being in the same State/Position it was in step 1
3a. if there is no conflict (nobody else decided anything since we decided what we'd do given that command and state), append the events to the stream (retaining the updated _position_ and _etag_)
3b. if there is a conflict, obtain the conflicting events [that other writers have produced] since the Position used in step 1, `fold` them into our `state`, and go back to 2 (aside: the CosmosDb stored procedure can send them back immediately at zero cost or latency, and there is [a proposal for EventStore to afford the same facility](https://github.com/EventStore/EventStore/issues/1652))
4. [if it makes sense for our scenario], hold the _state_, _position_ and _etag_ in our cache. When a reader comes along, do a point-read of the _tip_ and jump straight to step 2 if nothing has been modified.

See [Cosmos Storage Model](#cosmos-storage-model) for a more detailed discussion of the role of the `sync` Stored Procedure in step 3

## Example

The following example is a trimmed version of [the Favorites model](samples/Store/Domain/Favorites.fs), with shortcuts for brevity (yes, and imperfect performance characteristics), which implements all the relevant functions above:
```fsharp
(* Event schemas *)

type Item = { id: int; name: string; added: DateTimeOffset } 
type Event =
    | Added of Item
    | Removed of itemId
    | Compacted of items: Item[]

(* State types *)

type State = Item list

let contains state id = state |> List.exists (fun x -> x.id=id)

(* Folding functions to build state from events *)

let evolve state event =
    match event with
    | Compacted items -> List.ofArray items
    | Added item -> item :: state
    | Removed id -> List.filter (not (contains state id)) 
let fold state events = Seq.fold evolve state events 

(* Decision Processing *)

type Command =
    | Add item
    | Remove itemId: int

let interpret command state =
    match command with
    | Add (id, name, date) ->
        if contains id then [] else [Added {id=id; name=name; date=date}]
    | Remove id -> 
        if contains id then [Removed id] else []

(* Unfold Functions to allow loading efficiently without having to read all the events in a stream *)

let unfold state =
    [Event.Compacted state]
let isOrigin = function
    | Compacted _ -> true
    | _ -> false
```

# Cosmos Storage Model

This article provides a walkthrough of how `Equinox.Cosmos` encodes, writes and reads records from a stream under its control.

The code (see [source](src/Equinox.Cosmos/Cosmos.fs#L6)) contains lots of comments and is intended to be read - this just provides some background.

## Batches

Events are stored in immutable batches consisting of:

* `p`artitionKey: `string` // stream identifier, e.g. "Cart-{guid}"
* `i`ndex: `int64` // base index position of this batch (`0` for first event in a stream)
* `n`extIndex: `int64` // base index ('i') position value of the next record in the stream - NB this _always_ corresponds to `i`+`e.length` (in the case of the `Tip` record, there won't actually be such a record yet)
* `id`: `string` // same as `i` (CosmosDb forces every doc to have one[, and it must be a `string`])
* `e`vents: `Event[]` // (see next section) typically there is one item in the array (can be many if events are small, for RU and performance/efficiency reasons; RU charges are per 1024 byte block)
* `ts` // CosmosDb-intrinsic last updated date for this record (changes when replicated etc, hence see `t` below)

## Events

Per `Event`, we have the following:

* `c`ase - the case of this union in the Discriminated Union of Events this stream bears (aka Event Type)
* `d`ata - json data (CosmosDb maintains it as actual json; you are free to index it and/or query based on that if desired)
* `m`etadata - carries ancillary information for an event; also json
* `t` - creation timestamp 

## Tip [Batch]

The _tip_ is always readable via a point-read, as the `id` has a fixed, well-known value: `"-1"`). It uses the same base layout as the aforementioned Batch (`Tip` *isa* `Batch`), adding the following:

* `id`: always `-1` so one can reference it in a point-read GET request and not pay the cost and latency associated with a full indexed query
* `_etag`: CosmosDb-managed field updated per-touch (facilitates `NotModified` result, see below)
* `u`: Array of _unfold_ed events based on a point-in-time _state_ (see _State, Snapshots, Events and Unfolds_, _Unfolded Events_  and `unfold` in the programming model section). Not indexed. While the data is json, the actual `d`ata and `m`etadata fields are compressed and encoded as base64 (and hence can not be queried in any reasonable manner).

# State, Snapshots, Events and Unfolds

In an Event Sourced system, we typically distinguish between the following basic elements

- _Events_ - Domain Events representing real world events that have occurred (always past-tense; it's happened and is not up for debate), reflecting the domain as understood by domain experts - see [Event Storming](https://en.wikipedia.org/wiki/Event_storming). Examples: _The customer favorited the item_, +the customer add SKU Y to their saved for later list_, _A charge of $200 was submitted successfully with transaction id X_.

- _State_ - derived representations established from Events. A given set of code in an environment will, in service of some decision making process, interpret the Events as implying particular state changes in a model. If we change the code slightly or add a field, you wouldn't necessarily expect a version of your code from a year ago to generate you equivalent state that you can simply blast into your object model and go. (But you can easily and safely hold a copy in memory as long as your process runs as this presents no such interoperability or versioning concerns). State is not necessarily always serializable, nor should it be.

- _Snapshots_ - A snapshot is an intentionally roundtrippable version of a State, that can be saved and restored. Typically one would do this to save the (latency, roundtrips, RUs, deserialization and folding) cost of loading all the Events in a long running sequence of Events to re-establish the State. The [EventStore folks have a great walkthrough on Rolling Snapshots](https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html).

- Projections - the term projection is *heavily* overloaded, meaning anything from the proceeds of a SELECT statement, the result of a `map` operation, an EventStore projection, an event being propagated via Kafka (no, further examples are not required!).

.... and:

- Unfolds - the term `unfold` is based on the well known 'standard' FP function of that name, bearing the signature `'state -> 'event seq`. **=> For `Equinox.Cosmos`, one might say `unfold` yields _projections_s as _event_s to _snapshot_ the _state_ as at that _position_ in the _stream_**.

## Generating and saving `unfold`ed events

Periodically, along with writing the _events_ that a _decision function_ yields to represent the implications of a _command_ given the present _state_, we also `unfold` the resulting `state'` and supply those to the `sync` function too. The `unfold` function takes the `state` and projects one or more snapshot-events that can be used to re-establish a state equivalent to that we have thus far derived from watching the events on the stream. Unlike normal events, `unfold`ed events do not get replicated to other systems, and can also be jetisonned at will (we also compress them rather than storing them as fully expanded json).

_NB, in the present implementation, `u`nfolds are generated, transmitted and updated upon every write; this makes no difference from a Request Charge perspective, but is clearly suboptimal due to the extra computational effort and network bandwidth consumption. This will likely be optimized by exposing controls on the frequency at which `unfold`s are triggered_

# Reading from the Storage Model

The dominant pattern is that reads request _Tip_  with an`IfNoneMatch` precondition citing the `_etag_ it bore when we last saw it. That, when combined with a cache means one of the following happens when a reader is trying to establish the _state_ of a _stream_ prior to processing a _Command_:
- `NotModified` (depending on workload, can be the dominant case) - for `1` RU, minimal latency and close-to-`0` network bandwidth, we know the present state
- `NotFound` (there's nothing in the stream) - for equivalently low cost (`1` RU), we know the _state_ is `initial`
- `Found` - (if there are multiple writers and/or we don't have a cached version) - for the minimal possible cost (a point read, not a query), we have all we need to establish the state:-
    `i`: a version number
    `e`: events since that version number
    `u`: unfolded (auxiliary) events computed at the same time as the batch of events was sent (aka projections/snapshots) - (these enable us to establish the `state` without further queries or roundtrips to load and fold all preceding events)

## Building a state from the Storage Model and/or the Cache

Given a stream with:

```
{ id:0, i:0, e: [{c:c1, d:d1}]},
{ id:1, i:1, e: [{c:c2, d:d2}]}, 
{ id:2, i:2, e: [{c:c2, d:d3}]}, 
{ id:3, i:3, e: [{c:c1, d:d4}]}, 
{ id:-1,
  i:4,
  e: [{i:4, c:c3, d:d5}],
  u: [{i:4, c:s1, d:s5Compressed}, {i:3, c:s2, d:s4Compressed}],
  _etag: "etagXYZ"
}  
```

If we have `state4` based on the events up to `{i:3, c:c1, d: d4}` and the index document, we can produce the `state` by folding in a variety of ways:

- `fold initial [ C1 d1; C2 d2; C3 d3; C1 d4; C3 d5 ]` (but would need a query to load the first 2 batches, with associated RUs and roundtrips)
- `fold state4 [ C3 d5 ]` (only need to pay to transport the _tip_ document as a point read)
- (if `isStart (S1 s5)` = `true`): `fold initial [S1 s5]` (point read + transport + decompress` s5`)
- (if `isStart (S2 s4)` = `true`): `fold initial [S2 s4; C3 d5]` (only need to pay to transport the _tip_ document as a point read and decompress `s4` and `s5`)

If we have `state3` based on the events up to `{i:3, c:c1, d: d4}`, we can produce the `state` by folding in a variety of ways:
- `fold initial [ C1 d1; C2 d2; C3 d3; C1 d4; C3 d5 ]` (but query, roundtrips)
- `fold state3 [C1 d4 C3 d5]` (only pay for point read+transport)
- `fold initial [S2 s4; C3 d5]` (only pay for point read+transport)
- (if `isStart (S1 s5)` = `true`): `fold initial [S1 s5]` (point read + transport + decompress` s5`)
- (if `isStart (S2 s4)` = `true`): `fold initial [S2 s4; C3 d5]` (only need to pay to transport the _tip_ document as a point read and decompress `s4` and `s5`)

If we have `state5` based on the events up to `C3 d5`, and (being the writer, or a recent reader), have the etag: `etagXYZ`, we can do a `HTTP GET` with `etag: IfNoneMatch etagXYZ`, which will return `302 Not Modified` with < 1K of data, and a charge of `1.00` RU allowing us to derive the state as:
- `state5`

See [Programming Model](#programing-model) for what happens in the application based on the events presented.

# Sync stored procedure high level flow

This covers what the most complete possible implementation of the JS Stored Procedure (see [source](https://github.com/jet/equinox/blob/tip-isa-batch/src/Equinox.Cosmos/Cosmos.fs#L302)) does when presented with a batch to be written.

(NB The present implementation is slightly simplified; see [source](src/Equinox.Cosmos/Cosmos.fs#L303).

The `sync` stored procedure takes a document as input which is almost identical to the format of the _`Tip`_ batch (in fact, if the stream is found to be empty, it is pretty much the template for the first document created in the stream). The request includes the following elements:

- `expectedVersion`: the position the requestor has based their [proposed] events on (no, [providing an `etag` to save on Request Charges is not possible in the Stored Proc](https://stackoverflow.com/questions/53355886/azure-cosmosdb-stored-procedure-ifmatch-predicate))
- `e`: array of Events (see Event, above) to append iff the expectedVersion check is fulfilled
- `u`: array of `unfold`ed events (aka snapshots, projections) that supersede items with equivalent `c`ase values  
- `maxEvents`: the maximum number of events in an individual batch prior to starting a new one. For example:
    - if `e` contains 2 events, the _tip_ document's `e` has 2 documents and the `maxEvents` is `5`, the events get appended onto the tip
    - if the total length including the new `e`vents would exceed `maxEvents`, the Tip is 'renamed' (gets its `id` set to `i.toString()`) to become a batch, and the new events go into the new Tip-Batch, the _tip_ gets frozen as a `Batch`, and the new request becomes the _tip_ (as an atomic transaction on the server side)
- (PROPOSAL/FUTURE) `thirdPartyUnfoldRetention`: how many events to keep before the base (`i`) of the batch if required by lagging `u`nfolds which would otherwise fall out of scope as a result of the appends in this batch (this will default to `0`, so for example if a writer says maxEvents `10` and there is an `u`nfold based on an event more than `10` old it will be removed as part of the appending process)
- (PROPOSAL/FUTURE): adding an `expectedEtag` would enable competing writers to maintain and update `u`nfold data in a consistent fashion (backign off and retrying in the case of conflict, _without any events being written per state change_)

# Equinox.Cosmos.Core.Events

The `Equinox.Cosmos.Core` namespace provides a lower level API which can be used to manipulate events stored within a Azure CosmosDb using optimized native access patterns.

The higher level APIs (i.e. not `Core`), as demonstrated by the `dotnet new templates` are recommended to be used in the general case, as they provide the following key benefits:

- Domain logic is store-agnostic, leaving it easy to:
  a) Unit Test in isolation (verifying decisions produce correct events)
  b) Integration test using the `MemoryStore`, where relevant
- Decouples encoding/decoding of events from the decision process of what events to write (means your Domain layer does not couple to a specific storage layer or encoding mechanism)
- Enables efficient caching and/or snapshotting (providing Equinox with `fold`, `initial`, `isOrigin`, `unfold` and a codec allows it to manage this efficiently)
- Provides Optimistic Concurrency Control with retries in the case of conflicting events

## Example Code:

```fsharp

open Equinox.Cosmos.Core.Events
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
let connector : Equinox.Cosmos.EqxConnector =
    EqxConnector(
        requestTimeout = TimeSpan.FromSeconds 5.,
        maxRetryAttemptsOnThrottledRequests = 1,
        maxRetryWaitTimeInSeconds = 3,
        log=gatewayLog)
let cnx = connector.Connect("Application.CommandProcessor", Discovery.FromConnectionString connectionString) |> Async.RunSynchronously

// If storing in a single collection, one specifies the db and collection
// alternately use the overload which defers the mapping until the stream one is writing to becomes clear
let coll = EqxCollections("databaseName","collectionName")
let eqxCtx = EqxContext(cnx, coll, gatewayLog)

//
// Write an event
//

let expectedSequenceNumber = 0 // new stream
let streamName, eventType, eventJson = "stream-1", "myEvent", Request.ToJson event
let eventData = EventData.fromT(eventType, eventJson) |> Array.singleton

let! res =
    Events.append
        eqxCtx
        streamName 
        expectedSequenceNumber 
        eventData
match res with
| AppendResult.Ok -> ()
| c -> failwithf "conflict %A" c
```