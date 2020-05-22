# Equinox [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.equinox?branchName=master)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=4?branchName=master) [![release](https://img.shields.io/github/release/jet/equinox.svg)](https://github.com/jet/equinox/releases) [![NuGet](https://img.shields.io/nuget/vpre/Equinox.svg?logo=nuget)](https://www.nuget.org/packages/Equinox/) [![license](https://img.shields.io/github/license/jet/Equinox.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/equinox.svg) [![docs status](https://img.shields.io/badge/DOCUMENTATION-WIP-important.svg?style=popout)](DOCUMENTATION.md) [<img src="https://img.shields.io/badge/slack-DDD--CQRS--ES%20%23equinox-yellow.svg?logo=slack">](https://j.mp/ddd-es-cqrs)

A unified programming model for event-sourced processing against stream-based stores including snapshots, caching and other state management/retrieval optimizations. Does not and will not handle projections, subscriptions etc. (see [Propulsion](https://github.com/jet/propulsion) for that).

Focused on remaining a set of low dependency _libraries_ that _you_ compose into an architecture that fits your apps needs; not a final Best Practice Architecture/object model/processing pipeline foisting a one-size-fits-all model on you. You decide what facilities make sense for your context; Equinox covers just the selected infrastructural aspects. (That's not to say we don't have plenty opinions and well polished patterns; we just try to confine the impact of that to [`samples`](/samples) or [templates](https://github.com/jet/dotnet-templates), leaving judgement calls open for you to adjust as your app evolves). This core repo will never feature tight integration with any specific web or services framework.

The design is informed by discussions, talks and countless hours of hard and thoughtful work invested into many previous systems, [frameworks](https://github.com/NEventStore), [samples](https://github.com/thinkbeforecoding/FsUno.Prod), [forks of samples](https://github.com/bartelink/FunDomain), the outstanding continuous work of the [EventStore](https://github.com/eventstore) founders and team and the wider [DDD-CQRS-ES](https://groups.google.com/forum/#!forum/dddcqrs) community. It would be unfair to single out even a small number of people despite the immense credit that is due.

_If you're looking to learn more about and/or discuss Event Sourcing and it's myriad benefits, tradeoffs and pitfalls as you apply it to your Domain, look no further than the thriving 2000+ member community on the [DDD-CQRS-ES Slack](https://github.com/ddd-cqrs-es/slack-community); you'll get patient and impartial world class advice 24x7 (psst there are [#equinox](https://ddd-cqrs-es.slack.com/messages/CF5J67H6Z) and [#sql-stream-store](https://app.slack.com/client/T0HCLN01Y) channels where you can ask questions or offer feedback)._ ([invite link](https://j.mp/ddd-es-cqrs))

Some aspects of the implementation are distilled from [`Jet.com` systems dating all the way back to 2013](http://gorodinski.com/blog/2013/02/17/domain-driven-design-with-fsharp-and-eventstore/); current supported backends are:

- [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db) - contains code dating back to 2016, however [the storage model](DOCUMENTATION.md#Cosmos-Storage-Model) was arrived at based on intensive benchmarking (squash-merged in [#42](https://github.com/jet/equinox/pull/42)).
- [EventStore](https://eventstore.org/) - this codebase itself has been in production since 2017 (see commit history), with key elements dating back to approx 2016.
- `MemoryStore`: In-memory store (volatile, for unit or integration test purposes). Fulfils the full contract Equinox imposes on a store, but without I/O costs [(it's ~100 LOC wrapping a `ConcurrentDictionary`)](https://github.com/jet/equinox/blob/master/src/Equinox.MemoryStore/MemoryStore.fs), and the ability to [take serialization/deserialization cost out of the picture](https://github.com/jet/FsCodec#boxcodec).
- [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore): Bindings for the powerful and widely used SQL-backed Event Storage system. [See SqlStreamStore docs](https://sqlstreamstore.readthedocs.io/en/latest/#introduction). :pray: [@rajivhost](https://github.com/rajivhost)

<a name="tldr"></a>
# TL;DR Can we cut to the chase? :fast_forward:

- I'm a dev that wants to a flavour via _code_, any code, now! (with light commentary) :point_right: [There's a simple `Counter` example using `Equinox.MemoryStore` for you](https://github.com/jet/equinox/blob/master/samples/Tutorial/Counter.fsx#L16)
- I'm a dev that knows about event sourcing, CosmosDB and F#; Throw me a terse slab of code instead of a wall of text and let me work from that! :point_right: [~100 LOC end-to-end using CosmosDB](https://github.com/jet/equinox/blob/master/samples/Tutorial/Cosmos.fsx#L36) 
- I understand CosmosDB backwards, show me what Equinox is going to enable that something simple I write in a few hours, or [CosmoStore](https://github.com/Dzoukr/CosmoStore) doesn't already do? :point_right: [Access Strategies guide](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#access-strategies)

# Features

- Designed not to invade application code; Domain tests can be written directly against your models without any need to involve or understand Equinox assemblies or constructs as part of writing those tests.
- Extracted from working software; currently used for all data storage within Jet's API gateway and Cart processing.
- Significant test coverage for core facilities, and with baseline and specific tests per Storage system and a comprehensive test and benchmarking story
- Event serialization is fully pluggable; all encoding is specified in terms of the [`FsCodec.IEventCodec` contract](https://github.com/jet/FsCodec#IEventCodec). [FsCodec](https://github.com/jet/FsCodec) provides for pluggable encoding of events based on either:
  - `NewtonsoftJson.Codec`: a [versionable convention-based approach](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/) (using `Typeshape`'s `UnionContractEncoder` under the covers), providing for serializer-agnostic schema evolution with minimal boilerplate
  - `Box.Codec`: lightweight [non-serializing substitute equivalent to `NewtonsoftJson.Codec` for use in unit and integration tests](https://github.com/jet/FsCodec#boxcodec)
  - `Codec`: an explicitly coded pair of `encode` and `tryDecode` functions for when you need to customize
- Independent of the store used, Equinox provides for caching using the .NET `MemoryCache` to minimize roundtrips (pluggable via [`ICache`](https://github.com/jet/equinox/blob/master/src/Equinox.Core/Cache.fs#L22) thanks to [@DSilence](https://github.com/jet/equinox/pull/161), latency and bandwidth / Request Charges by maintaining the folded state, without necessitating making the Domain Model folded state serializable
- Logging is mature and comprehensive (using [Serilog](https://github.com/serilog/serilog) internally), with optimal performance and pluggable integration with your apps hosting context (we ourselves typically feed log info to Splunk and the metrics embedded in the `Serilog.Events.LogEvent` Properties to Prometheus; see relevant tests for examples)
- **`Equinox.EventStore` In-stream Rolling Snapshots**: Command processing can be optimized by means of 'compaction' events, meeting the following ends:
  - no additional roundtrips to the store needed at either the Load or Sync points in the flow
  - support, (via the [`FsCodec.IEventCodec`](https://github.com/jet/FsCodec#IEventCodec)) for the maintenance of multiple co-existing compaction schemas for a given stream (A 'compaction' event/snapshot isa Event)
  - compaction events typically do not get deleted (consistent with how EventStore works), although it is safe to do so in concept
  - NB while this works well, and can deliver excellent performance (especially when allied with the Cache), [it's not a panacea, as noted in this excellent EventStore.org article on the topic](https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html)
- **`Equinox.Cosmos` 'Tip with Unfolds' schema**: (In contrast to `Equinox.EventStore`'s `AccessStrategy.RollingSnapshots`,) when using `Equinox.Cosmos`, optimized command processing is managed via the `Tip`; a document per stream with a well-known identity enabling Syncing the r/w Position via a single point-read by virtue of the fact that the document maintains:
  a) the present Position of the stream - i.e. the index at which the next events will be appended for a given stream (events and the Tip share a common logical partition key)
  b) ephemeral (`deflate+base64` compressed) [_unfolds_](DOCUMENTATION.md#Cosmos-Storage-Model)
  c) (optionally) a holding buffer for events since those unfolded events ([presently removed](https://github.com/jet/equinox/pull/58), but [should return](DOCUMENTATION.md#Roadmap), see [#109](https://github.com/jet/equinox/pull/109))
  
  This yields many of the benefits of the in-stream Rolling Snapshots approach while reducing latency and RU provisioning requirements due to meticulously tuned Request Charge costs:-
  - Writes never need to do queries or touch event documents in any way
  - when coupled with the cache, a typical read is a point read [with `IfNoneMatch` on an etag], costing 1.0 RU if in-date [to get the `302 Not Found` response] (when the stream is empty, a `404 NotFound` response, also costing 1.0 RU)
  - no additional roundtrips to the store needed at either the Load or Sync points in the flow

  It should be noted that from a querying perspective, the `Tip` shares the same structure as `Batch` documents (a potential future extension would be to carry some events in the `Tip` as [some interim versions of the implementation once did](https://github.com/jet/equinox/pull/58), see also [#109](https://github.com/jet/equinox/pull/109).
- **`Equinox.Cosmos` `RollingState` and `Custom` 'non-event-sourced' modes**: Uses 'Tip with Unfolds' encoding to avoid having to write event documents at all - this enables one to build, reason about and test your aggregates in the normal manner, but inhibit event documents from being generated. This enables one to benefit from the caching and consistency management mechanisms without having to bear the cost of writing and storing the events themselves (and/or dealing with an ever-growing store size). Search for `transmute` or `RollingState` in the `samples` and/or see [the `Checkpoint` Aggregate in Propulsion](https://github.com/jet/propulsion/blob/master/src/Propulsion.EventStore/Checkpoint.fs). One chief use of this mechanism is for tracking Summary Event feeds in [the `dotnet-templates` `summaryConsumer` template](https://github.com/jet/dotnet-templates/tree/master/propulsion-summary-consumer).

## Components

The components within this repository are delivered as multi-targeted Nuget packages supporting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles; each of the constituent elements is designed to be easily swappable as dictated by the task at hand. Each of the components can be inlined or customized easily:-

### Core library

- `Equinox` [![NuGet](https://img.shields.io/nuget/v/Equinox.svg)](https://www.nuget.org/packages/Equinox/): Store-agnostic decision flow runner that manages the optimistic concurrency protocol. ([depends](https://www.fuget.org/packages/Equinox) on `FsCodec` (for the `StreamName` type-contract), `Serilog` (but no specific Serilog sinks, i.e. you configure to emit to `NLog` etc))

### Serialization support

- `FsCodec` [![Codec NuGet](https://img.shields.io/nuget/v/FsCodec.svg)](https://www.nuget.org/packages/FsCodec/): Defines minimal `IEventData`, `ITimelineEvent` and `IEventCodec` contracts, which are the sole aspects the Stores bind to. No dependencies.
  - [`FsCodec.IEventCodec`](https://github.com/jet/FsCodec/blob/master/src/FsCodec/FsCodec.fs#L31): defines a base interface for a serializer/deserializer.
  - `FsCodec.Codec`: enables plugging in a serializer and/or Union Encoder of your choice (typically this is used to supply a pair of functions:- `encode` and `tryDecode`)
  ([depends](https://www.fuget.org/packages/FsCodec) on nothing 
- `FsCodec.NewtonsoftJson` [![Newtonsoft.Json Codec NuGet](https://img.shields.io/nuget/v/FsCodec.NewtonsoftJson.svg)](https://www.nuget.org/packages/FsCodec.NewtonsoftJson/)
  - As described in [a scheme for the serializing Events modelled as an F# Discriminated Union](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/), allows tagging of F# Discriminated Union cases in a versionable manner with low-dependencies using[TypeShape](https://github.com/eiriktsarpalis/TypeShape)'s [`UnionContractEncoder`](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores)
  - uses Json.net to serialize the event bodies.
  - `FsCodec.Box.Codec`: Testing substitute for `FsCodec.NewtonsoftJson.Codec`, included in same package.
  - ([depends](https://www.fuget.org/packages/FsCodec.NewtonsoftJson) on `FsCodec`, `Newtonsoft.Json >= 11.0.2`, `TypeShape`, see [FsCodec repo](https://github.com/jet/FsCodec) for details)
- (planned) `FsCodec.SystemTextJson`: drop in replacement that allows one to target the .NET `System.Text.Json` serializer solely by changing the referenced namespace.

### Store libraries

- `Equinox.Core` [![NuGet](https://img.shields.io/nuget/v/Equinox.Core.svg)](https://www.nuget.org/packages/Equinox.Core/): Interfaces and helpers used in realizing the concrete Store implementations, together with the default [`System.Runtime.Caching.Cache`-based] `Cache` implementation . ([depends](https://www.fuget.org/packages/Equinox.Core) on `Equinox`, `System.Runtime.Caching`)
- `Equinox.MemoryStore` [![MemoryStore NuGet](https://img.shields.io/nuget/v/Equinox.MemoryStore.svg)](https://www.nuget.org/packages/Equinox.MemoryStore/): In-memory store for integration testing/performance baselining/providing out-of-the-box zero dependency storage for examples. ([depends](https://www.fuget.org/packages/Equinox.MemoryStore) on `Equinox.Core`, `FsCodec`)
- `Equinox.EventStore` [![EventStore NuGet](https://img.shields.io/nuget/v/Equinox.EventStore.svg)](https://www.nuget.org/packages/Equinox.EventStore/): Production-strength [EventStore](https://eventstore.org/) Adapter instrumented to the degree necessitated by Jet's production monitoring requirements. ([depends](https://www.fuget.org/packages/Equinox.EventStore) on `Equinox.Core`, `EventStore.Client[Api.NetCore] >= 5.0.1`, `FSharp.Control.AsyncSeq`)
- `Equinox.Cosmos` [![Cosmos NuGet](https://img.shields.io/nuget/v/Equinox.Cosmos.svg)](https://www.nuget.org/packages/Equinox.Cosmos/): Production-strength Azure CosmosDb Adapter with integrated 'unfolds' feature, facilitating optimal read performance in terms of latency and RU costs, instrumented to the degree necessitated by Jet's production monitoring requirements. ([depends](https://www.fuget.org/packages/Equinox.Cosmos) on `Equinox.Core`, `Microsoft.Azure.DocumentDb[.Core] >= 2.2`, `FsCodec.NewtonsoftJson`, `FSharp.Control.AsyncSeq`)
- `Equinox.SqlStreamStore` [![SqlStreamStore NuGet](https://img.shields.io/nuget/v/Equinox.SqlStreamStore.svg)](https://www.nuget.org/packages/Equinox.SqlStreamStore/): Production-strength [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore) Adapter derived from `Equinox.EventStore` - provides core facilities (but does not connect to a specific database; see sibling `SqlStreamStore`.* packages). ([depends](https://www.fuget.org/packages/Equinox.SqlStreamStore) on `Equinox.Core`, `FsCodec`, `SqlStreamStore >= 1.2.0-beta.8`, `FSharp.Control.AsyncSeq`)
- `Equinox.SqlStreamStore.MsSql` [![MsSql NuGet](https://img.shields.io/nuget/v/Equinox.SqlStreamStore.MsSql.svg)](https://www.nuget.org/packages/Equinox.SqlStreamStore.MsSql/): [SqlStreamStore.MsSql](https://sqlstreamstore.readthedocs.io/en/latest/sqlserver) Sql Server `Connector` implementation for `Equinox.SqlStreamStore` package). ([depends](https://www.fuget.org/packages/Equinox.SqlStreamStore.MsSql) on `Equinox.SqlStreamStore`, `SqlStreamStore.MsSql >= 1.2.0-beta.8`)
- `Equinox.SqlStreamStore.MySql` [![MySql NuGet](https://img.shields.io/nuget/v/Equinox.SqlStreamStore.MySql.svg)](https://www.nuget.org/packages/Equinox.SqlStreamStore.MySql/): `SqlStreamStore.MySql` MySQL Í`Connector` implementation for `Equinox.SqlStreamStore` package). ([depends](https://www.fuget.org/packages/Equinox.SqlStreamStore.MySql) on `Equinox.SqlStreamStore`, `SqlStreamStore.MySql >= 1.2.0-beta.8`)
- `Equinox.SqlStreamStore.Postgres` [![Postgres NuGet](https://img.shields.io/nuget/v/Equinox.SqlStreamStore.Postgres.svg)](https://www.nuget.org/packages/Equinox.SqlStreamStore.Postgres/): [SqlStreamStore.Postgres](https://sqlstreamstore.readthedocs.io/en/latest/postgres) PostgreSQL `Connector` implementation for `Equinox.SqlStreamStore` package). ([depends](https://www.fuget.org/packages/Equinox.SqlStreamStore.Postgres) on `Equinox.SqlStreamStore`, `SqlStreamStore.Postgres >= 1.2.0-beta.8`)

### Projection libraries

Equinox does not focus on projection logic or wrapping thereof - each store brings its own strengths, needs, opportunities and idiosyncrasies. Here's a list of some relevant libraries from sibling projects that get used with regard to this:

- `FsKafka` [![FsKafka NuGet](https://img.shields.io/nuget/v/FsKafka.svg)](https://www.nuget.org/packages/FsKafka/): Wraps `Confluent.Kafka` to provide efficient batched Kafka Producer and Consumer configurations, with basic logging instrumentation. Used in the [`propulsion project kafka`](https://github.com/jet/propulsion#dotnet-tool-provisioning--projections-test-tool) tool command; see [`dotnet new proProjector -k; dotnet new proConsumer` to generate a sample app](https://github.com/jet/dotnet-templates#propulsion-related) using it (see the `BatchedAsync` and `BatchedSync` modules in `Examples.fs`).
- `Propulsion` [![Propulsion NuGet](https://img.shields.io/nuget/v/Propulsion.svg)](https://www.nuget.org/packages/Propulsion/): defines a canonical `Propulsion.Streams.StreamEvent` used to interop with `Propulsion.*` in processing pipelines for the `proProjector` and `proSync` templates in the [templates repo](https://github.com/jet/dotnet-templates), together with the `Ingestion`, `Streams`, `Progress` and `Parallel` modules that get composed into those processing pipelines. ([depends](https://www.fuget.org/packages/Propulsion) on `Serilog`)
- `Propulsion.Cosmos` [![Propulsion.Cosmos NuGet](https://img.shields.io/nuget/v/Propulsion.Cosmos.svg)](https://www.nuget.org/packages/Propulsion.Cosmos/): Wraps the [Microsoft .NET `ChangeFeedProcessor` library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet) providing a [processor loop](DOCUMENTATION.md#change-feed-processors) that maintains a continuous query loop per CosmosDb Physical Partition (Range) yielding new or updated documents (optionally unrolling events written by `Equinox.Cosmos` for processing or forwarding). Used in the [`propulsion project stats cosmos`](dotnet-tool-provisioning--benchmarking-tool) tool command; see [`dotnet new proProjector` to generate a sample app](#quickstart) using it. ([depends](https://www.fuget.org/packages/Propulsion.Cosmos) on `Equinox.Cosmos`, `Microsoft.Azure.DocumentDb.ChangeFeedProcessor >= 2.2.5`)
- `Propulsion.EventStore` [![Propulsion.EventStore NuGet](https://img.shields.io/nuget/v/Propulsion.EventStore.svg)](https://www.nuget.org/packages/Propulsion.EventStore/) Used in the [`propulsion project es`](dotnet-tool-provisioning--benchmarking-tool) tool command; see [`dotnet new proSync` to generate a sample app](#quickstart) using it. ([depends](https://www.fuget.org/packages/Propulsion.EventStore) on `Equinox.EventStore`)
- `Propulsion.Kafka` [![Propulsion.Kafka NuGet](https://img.shields.io/nuget/v/Propulsion.Kafka.svg)](https://www.nuget.org/packages/Propulsion.Kafka/): Provides a canonical `RenderedSpan` that can be used as a default format when projecting events via e.g. the Producer/Consumer pair in `dotnet new proProjector -k; dotnet new proConsumer`. ([depends](https://www.fuget.org/packages/Propulsion.Kafka) on `Newtonsoft.Json >= 11.0.2`, `Propulsion`, `FsKafka`)

### `dotnet tool` provisioning / benchmarking tool

- `Equinox.Tool` [![Tool NuGet](https://img.shields.io/nuget/v/Equinox.Tool.svg)](https://www.nuget.org/packages/Equinox.Tool/): Tool incorporating a benchmark scenario runner, facilitating running representative load tests composed of transactions in `samples/Store` and `samples/TodoBackend` against any nominated store; this allows perf tuning and measurement in terms of both latency and transaction charge aspects. (Install via: `dotnet tool install Equinox.Tool -g`)

### `dotnet new` starter project templates and sample applications

- `Equinox.Templates` [![Templates NuGet](https://img.shields.io/nuget/v/Equinox.Templates.svg)](https://www.nuget.org/packages/Equinox.Templates/): [The templates repo](https://github.com/jet/dotnet-templates) has C# and F# sample apps. (Install via `dotnet new -i Equinox.Templates && dotnet new eqx --list`). See [the quickstart](#quickstart) for examples of how to use it.
- [`samples/Store` (in this repo)](/samples/Store): Example domain types reflecting examples of how one applies Equinox to a diverse set of stream-based models
- [`samples/TodoBackend` (in this repo)](/samples/TodoBackend): Standard https://todobackend.com compliant backend
- [`samples/Tutorial` (in this repo)](/samples/Tutorial): Annotated `.fsx` files with sample Aggregate impls

# Overview

## The Propulsion Perspective

Equinox and Propulsion have a [Yin and yang](https://en.wikipedia.org/wiki/Yin_and_yang) relationship; the use cases for both naturally interlock and overlap. It can be relevant to peruse [the Propulsion Documentation's Overview Diagrams](https://github.com/jet/propulsion/blob/master/DOCUMENTATION.md#overview) for the complementary perspective (TL;DR its largely the same topology, with elements that are central here de-emphasized over there, and vice versa)

## [C4](https://c4model.com) Context diagram

Equinox focuses on the **Consistent Processing** element of building an event-sourced system, offering tailored components that interact with a specific **Consistent Event Store**, as laid out here in this [C4](https://c4model.com) System Context Diagram:

![Equinox c4model.com Context Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/context.puml&fmt=svg)

:point_up: Propulsion elements (which we consider External to Equinox) support the building of complementary facilities as part of an overall Application:

- **Ingesters**: read stuff from outside the Bounded Context of the System. This kind of service covers aspects such as feeding reference data into **Read Models**, ingesting changes into a consistent model via **Consistent Processing**. _These services are not acting in reaction to events emanating from the **Consistent Event Store**, as opposed to..._
- **Publishers**: react to events as they are arrive from the **Consistent Event Store** by filtering, rendering and producing to feeds for downstreams (here we label that _Publish Simple Notifications_). _While these services may in some cases rely on synchronous queries via **Consistent Processing**, it's never transacting or driving follow-on work; which brings us to..._
- **Reactors**: drive reactive actions triggered by either upstream feeds, or events observed in the **Consistent Event Store**. _These services handle anything beyond the duties of **Ingesters** or **Publishers**, and will often drive follow-on processing via Process Managers and/or transacting via **Consistent Processing**. In some cases, a reactor app's function may be to progressively compose a notification for a **Publisher** to eventually publish._

## [C4](https://c4model.com) Container diagram

The relevant pieces of the above break down as follows, when we emphasize the [Containers](https://c4model.com) aspects relevant to Equinox:

![Equinox c4model.com Container Diagram](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.github.com/jet/equinox/master/diagrams/container.puml&fmt=svg)

**[See Overview section in `DOCUMENTATION`.md for further drill down](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#overview)**

## TEMPLATES

The best place to start, sample-wise is with the [QuickStart](#quickstart), which walks you through sample code, tuned for approachability, from `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## SAMPLES

The `samples/` folder contains various further examples (some of the templates are derived from these), with the complementary goals of:

- being a starting point to see how one might consume the libraries.
- acting as [Consumer Driven Contracts](https://martinfowler.com/articles/consumerDrivenContracts.html) to validate new and pin existing API designs.
- providing outline (not official and complete) guidance as to things that are valid to do in an application consuming Equinox components.
- to validate that each specific Storage implementation can fulfill the needs of each of the example Services/Aggregates/Applications. (_unfortunately this concern makes a lot of the DI wiring more complex than a real application should be; it's definitely a non-goal for every Equinox app to be able to switch between backends, even though that's very much possible to achieve._)
- provide sample scripts referenced in the Tutorial

<a name="TodoBackend"></a>
### [TODOBACKEND, see samples/TodoBackend](/samples/TodoBackend)

The repo contains a vanilla ASP.NET Core 2.1 implementation of [the well-known TodoBackend Spec](https://www.todobackend.com). **NB the implementation is largely dictated by spec; no architectural guidance expressed or implied ;)**. It can be run via:

    & dotnet run -f netcoreapp3.1 -p samples/Web -S es # run against eventstore, omit `es` to use in-memory store, or see PROVISIONING EVENTSTORE
    start https://www.todobackend.com/specs/index.html?https://localhost:5001/todos # for low-level debugging / validation of hosting arrangements
    start https://www.todobackend.com/client/index.html?https://localhost:5001/todos # standard JavaScript UI
    start http://localhost:5341/#/events # see logs triggered by `-S` above in https://getseq.net        

### [STORE, see /samples/Store](/samples/Store)

The core sample in this repo is the `Store` sample, which contains code and tests extracted from real implementations (with minor simplifications in some cases).

These facts mean that:

- some of the code may be less than approachable for a beginner (e.g. some of the code is in its present form for reasons of efficiency)
- some of the code may not represent official best practice guidance that the authors would necessarily stand over (e.g., the CQRS pattern is not strictly adhered to in all circumstances; some command designs are not completely correct from an idempotency perspective)

While these things can of course be perfected through PRs, this is definitely not top of the TODO list for the purposes of this repo. (We'd be delighted to place links to other samples, including cleanups / rewrites of these samples written with different testing platforms, web platforms, or DDD/CQRS/ES design flavors right here).

### [m-r](https://github.com/gregoryyoung/m-r/tree/master/SimpleCQRS) port, [see samples/Store/Domain/InventoryItem.fs](samples/Store/Domain/InventoryItem.fs)

For fun, there's a direct translation of the `InventoryItem` Aggregate and Command Handler from Greg Young's [`m-r`](https://github.com/gregoryyoung/m-r/tree/master/SimpleCQRS) demo project [as one could write it in F# using Equinox](https://github.com/jet/equinox/blob/master/samples/Store/Domain/InventoryItem.fs). NB any typical presentation of this example includes copious provisos and caveats about it being a toy example written almost a decade ago.

### [`samples/Tutorial` (in this repo)](/samples/Tutorial): Annotated `.fsx` files with sample aggregate implementations

### [@ameier38](https://github.com/ameier38)'s Tutorial

[Andrew Meier](https://andrewcmeier.com) has written a very complete tutorial modeling a business domain using Equinox and EventStore; includes Dockerized Suave API, test suite using Expecto, build automation using FAKE, and CI using Codefresh; see [the repo](https://github.com/ameier38/equinox-tutorial) and its [overview blog post](https://andrewcmeier.com/bi-temporal-event-sourcing).

## QuickStart

### Spin up a [TodoBackend](https://www.todobackend.com/) `.fsproj` app ([storing in `Equinox.MemoryStore` Simulator](https://github.com/jet/equinox#store-libraries))

0. Make a scratch area

    ```powershell
    mkdir ExampleApp
    cd ExampleApp 
    ```

1. Use a `dotnet new` template to get fresh code in your repo

    ```powershell
    dotnet new -i Equinox.Templates # see source in https://github.com/jet/dotnet-templates
    dotnet new eqxweb -t # -t for todos, defaults to memory store (-m) # use --help to see options regarding storage subsystem configuration etc
    ```

2. Run the `TodoBackend`:

    ```powershell
    dotnet run -p Web
    ```

4. Run the standard `TodoMvc` frontend against your locally-hosted, fresh backend (See generated `README.md` for more details)
    - Todo JavaScript client App: https://www.todobackend.com/client/index.html?https://localhost:5001/todos 
    - Run individual JS specification tests: https://www.todobackend.com/specs/index.html?https://localhost:5001/todos

### Spin up a [TodoBackend](https://www.todobackend.com/) `.csproj` ... with C# code

While Equinox is implemented in F#, and F# is a great fit for writing event-sourced domain models, [the APIs are not F#-specific](https://docs.microsoft.com/en-us/dotnet/fsharp/style-guide/component-design-guidelines); there's a [C# edition of the template](https://github.com/jet/dotnet-templates/tree/master/equinox-web-csharp). The instructions are identical to the rest, but you need to use the `eqxwebcs` template instead of `eqxweb`.

### Store data in [EventStore](https://eventstore.org)

1. install EventStore locally (requires admin privilege)

    - For Windows, install with Chocolatey:
    
      ```powershell
      cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
      ```

	- For OSX, install with `brew cask install eventstore` 

2. start the local EventStore instance

    - Windows

      ```powershell
      # run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
      & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
      ```

    - OSX:

  	  ```bash
      # run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
      eventstore --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
	  ```

3. generate sample app with EventStore wiring from template and start

    ```powershell
    dotnet new eqxweb -t -e # -t for todos, -e for eventstore
    dotnet run -p Web
    ```

4. browse writes at http://localhost:30778/web/index.html#/streams

### Store data in [Azure CosmosDb](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction)

1. *export 3x env vars* (see [provisioning instructions](#run-cosmosdb-benchmark-when-provisioned))

    ```powershell
    $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
    $env:EQUINOX_COSMOS_DATABASE="equinox-test"
    $env:EQUINOX_COSMOS_CONTAINER="equinox-test"
    ```

2. use the `eqx` tool to initialize the database and/or container (using preceding env vars)

    ```powershell
    dotnet tool uninstall Equinox.Tool -g
    dotnet tool install Equinox.Tool -g
    eqx init -ru 400 cosmos # generates a database+container, adds optimized indexes
    ```

3. generate sample app from template, with CosmosDb wiring

    ```powershell
    dotnet new eqxweb -t -c # -t for todos, -c for cosmos
    dotnet run -p Web
    ```

4. Use the `eqx` tool to dump stats relating the contents of the CosmosDb store

    ```powershell
    # run queries to determine how many streams, docs, events there are in the container
    eqx -V -C stats -SDEP cosmos # -P to run in parallel # -V -C to show underlying query being used
    ```

5. Use `propulsion` tool to run a CosmosDb ChangeFeedProcessor

    ```powershell
    dotnet tool uninstall Propulsion.Tool -g
    dotnet tool install Propulsion.Tool -g

    propulsion init -ru 400 cosmos # generates a -aux container for the ChangeFeedProcessor to maintain consumer group progress within
    # -V for verbose ChangeFeedProcessor logging
    # `-g projector1` represents the consumer group - >=1 are allowed, allowing multiple independent projections to run concurrently
    # stats specifies one only wants stats regarding items (other options include `kafka` to project to Kafka)
    # cosmos specifies source overrides (using defaults in step 1 in this instance)
    propulsion -V project -g projector1 stats cosmos
    ```

6. Generate a CosmosDb ChangeFeedProcessor sample `.fsproj` (without Kafka producer/consumer), using `Propulsion.Cosmos`

    ```powershell
    dotnet new -i Equinox.Templates

    # note the absence of -k means the projector code will be a skeleton that does no processing besides counting the events
    dotnet new proProjector

    # start one or more Projectors
    # `-g projector2` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently
    # cosmos specifies source overrides (using defaults in step 1 in this instance)
    dotnet run -- -g projector2 cosmos
    ```

7. Use `propulsion` tool to Run a CosmosDb ChangeFeedProcessor, emitting to a Kafka topic

    ```powershell	
    $env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b	
    # `-V` for verbose logging	
    # `projector3` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently	
    # `-l 5` to report ChangeFeed lags every 5 minutes	
    # `kafka` specifies one wants to emit to Kafka	
    # `temp-topic` is the topic to emit to	
    # `cosmos` specifies source overrides (using defaults in step 1 in this instance)	
    propulsion -V project -g projector3 -l 5 kafka temp-topic cosmos	
    ```	

 8. Generate CosmosDb [Kafka Projector and Consumer](https://github.com/jet/propulsion#feeding-to-kafka) `.fsproj`ects (using `Propulsion.Kafka`)

    ```powershell
    cat readme.md # more complete instructions regarding the code

    # -k requests inclusion of Apache Kafka support
    md projector | cd
    dotnet new proProjector -k

    # start one or more Projectors (see above for more examples/info re the Projector.fsproj)

    $env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b
    $env:PROPULSION_KAFKA_TOPIC="topic0" # or use -t
    dotnet run -- -g projector4 -t topic0 cosmos

    # generate a consumer app
    md consumer | cd
    dotnet new proConsumer

    # start one or more Consumers
    $env:PROPULSION_KAFKA_GROUP="consumer1" # or use -g
    dotnet run -- -t topic0 -g consumer1
    ```

<a name="sqlstreamstore"></a>
9. Use [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore)
   
  The SqlStreamStore consists of:

  - being able to supply `ms`, `my`, `pg` flag to `eqx run`, e.g. `eqx run -t cart -f 50 -d 5 -C -U ms -c "sqlserverconnectionstring" -s schema`
  - being able to supply `ms`, `my`, `pg` flag to `eqx dump`, e.g. `eqx dump -CEU -s "Favoritesab25cc9f24464d39939000aeb37ea11a" ms -c "sqlserverconnectionstring" -s schema`
  - being able to supply `ms`, `my`, `pg` flag to Web sample, e.g. `dotnet run -p samples/Web/ -- my -c "mysqlconnectionstring"`
  - being able to supply `ms`, `my`, `pg` flag to new `eqx config` command e.g. `eqx config pg -c "postgresconnectionstring" -u p "usercredentialsNotToBeLogged" -s schema`

    ```powershell
    cd ~/code/equinox
    
    # set up the DB/schema
    dotnet run -f netcoreapp3.1 -p tools/Equinox.Tool -- config pg -c "connectionstring" -p "u=un;p=password" -s "schema"
    
    # run a benchmark
    dotnet run -c Release -f netcoreapp3.1 -p tools/Equinox.Tool -- run -t saveforlater -f 50 -d 5 -C -U pg -c "connectionstring" -p "u=un;p=password" -s "schema"
    
    # run the webserver, -A to autocreate schema on connection
    dotnet run -p samples/Web/ -- my -c "mysqlconnectionstring" -A
    
    # set up the DB/schema
    eqx config pg -c "connectionstring" -p "u=un;p=password" -s "schema"
    
    # run a benchmark
    eqx run -t saveforlater -f 50 -d 5 -C -U pg -c "connectionstring" -p "u=un;p=password" -s "schema" 
    eqx dump -s "SavedForLater-ab25cc9f24464d39939000aeb37ea11a" pg  -c "connectionstring" -p "u=un;p=password" -s "schema" # show stored JSON (Guid shown in eqx run output) 
    ```

### BENCHMARKS

A key facility of this repo is being able to run load tests, either in process against a nominated store, or via HTTP to a nominated instance of `samples/Web` ASP.NET Core host app. The following test suites are implemented at present:

- `Favorite` - Simulate a very enthusiastic user that favorites something once per second
  - the test generates an ever-growing state that can only be managed efficiently if you apply either caching, snapshotting or both
  - NB due to being unbounded, `Snapshot` and `MultiSnapshot` etc. (even `RollingState` or `Custom`) will eventually hit the Store's limits (4MB/event for EventStore, 3MB/Item (document) for CosmosDB)
- `SaveForLater` - Simulate a happy shopper that saves 3 items per second, and empties the Save For Later list whenever it is full (when it hits 50 items)
  - Snapshotting helps a lot
  - Caching is not as essential as it is for the `Favorite` test (as long as you have either caching or snapshotting, that is)
- `Todo` - Keeps a) getting the list b) adding an item c) clearing the list when it hits 1000 items.
  - the `Cleared` event acts as a natural event to use in the `isOrigin` check. This makes snapshotting less crucial than it is, for example, in the case of the `Favorite` test
  - the `-s` parameter can be used to adjust the maximum item text length from the default (`100`, implying average length of 50)

## BUILDING

Please note the [QuickStart](#quickstart) is probably the best way to gain an overview - these instructions are intended to illustrated various facilities of the build script for people making changes.

### build and run

Run, including running the tests that assume you've got a local EventStore and pointers to a CosmosDb database and container prepared (see [PROVISIONING](#provisioning)):

    ./build.ps1

### build, skipping tests that require a Store instance

    ./build -s

### build, skipping all tests

    dotnet pack build.proj

### build, skip EventStore tests

    ./build -se

### build, skip EventStore tests, skip auto-provisioning / de-provisioning CosmosDb

    ./build -se -scp

### Run EventStore benchmark on Full Framework (when [provisioned](#provisioning))

Continually reads and writes very small events across multiple streams on .NET Full Framework

    dotnet pack -c Release ./build.proj
    & ./tools/Equinox.Tool/bin/Release/net461/eqx.exe run -f 2500 -C -U es

### Run EventStore benchmark on .NET Core (when provisioned)

At present, .NET Core seems to show comparable perf under normal load, but becomes very unpredictable under load. The following benchmark should produce pretty consistent levels of reads and writes, and can be used as a baseline for investigation:

    & dotnet run -c Release -f netcoreapp3.1 -p tools/Equinox.Tool -- run -t saveforlater -f 1000 -d 5 -C -U es

### run Web benchmark

The CLI can drive the Store and TodoBackend samples in the `samples/Web` ASP.NET Core app. Doing so requires starting a web process with an appropriate store (EventStore in this example, but can be `memory` / omitted etc. as in the other examples)

#### in Window 1

    & dotnet run -c Release -f netcoreapp3.1 -p samples/Web -- -C -U es

#### in Window 2

    dotnet tool install -g Equinox.Tool # only once
    eqx run -t saveforlater -f 200 web

### run CosmosDb benchmark (when provisioned)

    $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
    $env:EQUINOX_COSMOS_DATABASE="equinox-test"
    $env:EQUINOX_COSMOS_CONTAINER="equinox-test"

    tools/Equinox.Tool/bin/Release/net461/eqx run `
      cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER
    dotnet run -f netcoreapp3.1 -p tools/Equinox.Tool -- run `
      cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER

## PROVISIONING

### Provisioning EventStore (when not using -s or -se)

For EventStore, the tests assume a running local instance configured as follows to replicate as much as possible the external appearance of a Production EventStore Commercial cluster :-

    # requires admin privilege
    cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
    # run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
    & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778

### Provisioning CosmosDb (when not using -sc)

    dotnet run -f netcoreapp3.1 -p tools/Equinox.Tool -- init -ru 400 `
        cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER

### Provisioning SqlStreamStore

There's a `docker-compose.yml` file in the root, so installing `docker-compose` and then running `docker-compose up` rigs local `equinox-mssql`, `equinox-mysql` and `equinox-postgres` servers and databases at known ports. _NOTE The `Equinox.SqlStreamStore.*.Integration` suites currently assume this is in place and will otherwise fail_.
  
## DEPROVISIONING

### Deprovisioning (aka nuking) EventStore data resulting from tests to reset baseline

While EventStore rarely shows any negative effects from repeated load test runs, it can be useful for various reasons to drop all the data generated by the load tests by casting it to the winds:-

    # requires admin privilege
    rm $env:ProgramData\chocolatey\lib\eventstore-oss\tools\data

### Deprovisioning CosmosDb

The [provisioning](#provisioning) step spins up RUs in CosmosDB for the Container, which will keep draining your account until you reach a spending limit (if you're lucky!). *When finished running any test, it's critical to drop the RU allocations back down again via some mechanism (either delete the container or reset the RU provision down to the lowest possible value)*.

- Kill the container and/or database
- Use the portal to change the allocation

# RELEASING

*The perfect is the enemy of the good; [all this should of course be automated, but the elephant will be consumed in small bites rather than waiting till someone does it perfectly](https://github.com/jet/equinox/issues/80). This documents the actual release checklist as it stands right now. Any small helping bites much appreciated :pray: *

## Tagging releases

This repo uses [MinVer](https://github.com/adamralph/minver); [see here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All non-alpha releases derive from tagged commits on `master`. The tag defines the nuget package id etc. that the release will bear (`dotnet pack` uses the `MinVer` package to grab the value from the commit)

## Checklist

- :cry: the Azure Pipelines script does not run the integration tests, so these need to be run manually via the following steps:

  - [Provision](#provisioning):
    - Start Local EventStore running in simulated cluster mode
    - Set Environment variables X 3 for a CosmosDb database and container (you might need to `eqx init`)
    - `docker-compose up` to start 3 servers for the `SqlStreamStore.*.Integration` test suites
        - [NB `SqlStreamStore.MsSql` has not been tested yet](https://github.com/jet/equinox/issues/175) :see_no_evil: **
  - Run `./build.ps1` in PowerShell (or PowerShell Core on MacOS via `brew install cask pwsh`)

- [CHANGELOG](CHANGELOG.md) should be up to date
- commit should be tagged (remember to do `git push --tags` when pushing)
- after the push has resulted in a successful build, click through from the commit on github thru to the Azure Pipelines build state and verify _all_ artifacts bear the correct version suffix (if the tags were not pushed alongside the commit, they can be wrong). Then, and only then, do the Release (which will upload to nuget.org using a nuget API key that has upload permissions for the packages)
- _When adding new packages_: For safety, the NuGet API Key used by the Azure DevOps Releases step can only upload new versions of existing packages. As a result, the first version of any new package needs to be manually uploaded out of band. (then invite jet.com to become owner so subsequent releases can do an automated upload [after the request has been (manually) accepted])

## FAQ

### What _is_ Equinox?

OK, I've read the README and the tagline. I still don't know what it does! Really, what's the TL;DR ?

- supports storing events in [EventStore](https://eventstore.org), including working with existing data you may have (that's where it got its start)
- includes a proprietary optimized Store implementation that only needs an empty Azure CosmosDb account to get going
- provides all the necessary infrastructure to build idempotent synchronous command processing against all of the stores; your Domain code intentionally doesn't need to reference *any* Equinox modules whatsoever (although for smaller systems, you'll often group `Events`+`Fold`+`interpret`/`decide`+`Service` in a single `module`, which implies a reference to [the core `Equinox` package](src/Equinox)).
- following on from the previous point: you just write the unit tests without any Equinox-specific hoops to jump through; this really works very well indeed, assuming you're writing the domain code and the tests in F#. If you're working in a more verbose language, you may end up building some test helpers. We don't envisage Equinox mandating a specific pattern on the unit testing side (consistent naming such as `Events.Event`+`evolve`+`fold`+`Command`+`interpret`/`decide` can help though).
- it helps with integration testing decision processes by
  - staying out of your way as much as possible
  - providing an in-memory store that implements the same interface as the EventStore and CosmosDb stores do
- There is a projection story, but it's not the last word - any 3 proper architects can come up with at least 3 wrong and 3 right ways of running those perfectly
  - For EventStore, you use its' projections; they're great. There's a `Propulsion.EventStore` which serves the needs of `dotnet new proSync`, [but it's not intended for application level projections as yet](https://github.com/jet/propulsion/issues/8).
  - for CosmosDb, you use the `Propulsion.Cosmos.*` libraries to work off the CosmosDb ChangeFeed using the Microsoft ChangeFeedProcessor library (and, optionally, project to/consume from Kafka) using the sample app templates (`dotnet new proProjector`).

### Should I use Equinox to learn event sourcing ?

You _could_. However the Equinox codebase here is not designed to be a tutorial; it's also extracted from systems with no pedagogical mission whatsoever. [FsUno.Prod](https://github.com/thinkbeforecoding/FsUno.Prod) on the other hand has this specific intention, walking though it is highly recommended. Also [EventStore](https://eventstore.org/), being a widely implemented and well-respected open source system has some excellent learning materials and documentation with a wide usage community (search for `DDD-CQRS-ES` mailing list and slack).

Having said that, we'd love to see a set of tutorials written by people looking from different angles, and over time will likely do one too ... there's no reason why the answer to this question can't become "**of course!**"

### Can I use it for really big projects?

You can. Folks in Jet do; we also have systems where we have no plans to use it, or anything like it. That's OK; there are systems where having precise control over one's data access is critical. And (shush, don't tell anyone!) some find writing this sort of infrastructure to be a very fun design challenge that beats doing domain modelling any day ...

### Can I use it for really small projects and tiny microservices?

You can. Folks in Jet do; but we also have systems where we have no plans to use it, or anything like it as it would be overkill even for people familiar with Equinox.

### OK, but _should_ I use Equinox for a small project ?

You'll learn a lot from building your own equivalent wrapping layer. Given the array of concerns Equinox is trying to address, there's no doubt that a simpler solution is always possible if you constrain the requirements to specifics of your context with regard to a) scale b) complexity of domain c) degree to which you use or are likely to use >1 data store. You can and should feel free to grab slabs of Equinox's implementation and whack it into an `Infrastructure.fs` in your project too (note you should adhere to the rules of the [Apache 2 license](LICENSE)). If you find there's a particular piece you'd really like isolated or callable as a component and it's causing you pain as [you're using it over and over in ~ >= 3 projects](https://en.wikipedia.org/wiki/Rule_of_three_(computer_programming)), please raise an Issue though ! 

Having said that, getting good logging, some integration tests and getting lots of off-by-one errors off your plate is nice; the point of [DDD-CQRS-ES](https://github.com/ddd-cqrs-es/slack-community) is to get beyond toy examples to the good stuff - Domain Modelling on your actual domain.

### What client languages are supported ?

The main language in mind for consumption is of course F# - many would say that F# and event sourcing are a dream pairing; little direct effort has been expended polishing it to be comfortable to consume from other .NET languages, the `dotnet new eqxwebcs` template represents the current state.

## You say I can use volatile memory for integration tests, could this also be used for learning how to get started building event sourcing programs with equinox? 

The `MemoryStore` backend is intended to implement the complete semantics of a durable store (aside from caching). The main benefit of using it is that any tests using it have zero environment dependencies. In some cases this can be very useful for demo apps or generators (rather than assuming a specific store at a specific endpoint and/or credentials, there is something to point at which does not require configuration or assumptions.). The single problem of course is that it's all in-process; the minute you stop the host, your TODO list has been forgotten. In general, EventStore is a very attractive option for prototyping; the open source edition is trivial to install and has a nice UI that lets you navigate events being produced etc.

### OK, so it supports CosmosDb, EventStore and might even support more in the future. I really don't intend to shift datastores. Period. Why would I take on this complexity only to get the lowest common denominator ?

Yes, you have decisions to make; Equinox is not a panacea - there is no one size fits all. While the philosophy of Equinox is a) provide an opinionated store-neutral [Programming Model](DOCUMENTATION.md#Programming-Model) with a good pull toward a big [pit of success](https://blog.codinghorror.com/falling-into-the-pit-of-success/), while not closing the door to using store-specific features where relevant, having a dedicated interaction is always going to afford you more power and control.

### Is there a guide to building the simplest possible hello world "counter" sample, that simply counts with an add and a subtract event? 

Yes; [`Counter.fsx` in th Tutorial project in this repo](https://github.com/jet/equinox/blob/master/samples/Tutorial/Counter.fsx). It may also be worth starting with the [API Guide in DOCUMENTATION.md](DOCUMENTATION.md#api). An alternate way is to look at the `Todo.fs` files emitted by [`dotnet new equinoxweb`](https://github.com/jet/dotnet-templates) in the [QuickStart](#quickstart).

<a name="why-snapshots-in-stream"></a>
### Why do the snapshots go in the same stream in `Equinox.EventStore` and `Equinox.SqlStreamStore` ? :pray: [@chrisjhoare](https://github.com/chrisjhoare)

I've been looking through the snapshotting code recently. Can see the snapshot events go in the same stream as regular events. Presume this is to save on operations to read/write the streams? and a bit less overhead maintaining two serializers? Are there any other advantages? I quite like it this way but think i saw the [geteventstore](https://eventstore.org) advice was separate streams so just interested in any other reasoning behind it

The reason GES recommends against is that the entire db is built on writing stuff once in an append only manner (which is a great design from most aspects). This means your choices are:
  - embed snapshots in the same stream, but do that sparingly, as you can't delete them (impld in equinox as `AccessStrategy.RollingSnapshots`)
  - keep snapshots elsewhere (typically in a sister stream with the max items set to 1
    - which the GES background scavenging process will tidy up (but it aint free)
    - which is a separate roundtrip (which is not the end of the world in GES but is still another thing to go wrong)
    - which can't be written as a transaction, i.e. you'd need to write the snapshot after (and only after) a successful write (and worry about inconsistency)
That general advice/trade-offs on snapshotting applies to most systems.

The answer as to why that strategy is available in in `Equinox.EventStore` is for based on use cases (the second strategy was actually implemented in a bespoke manner initially by [@eiriktsarpalis](https://github.com/eiriktsarpalis):
- streams like `Favorites` where every event is small (add sku, drop sku), and the snapshot is pretty compact (list of skus) (but note it is ever growing)
- streams like `SavedForLater` items where the state rolls over regularly - even after 5 years and 1000s of items moving in and out, there's a constraint of max 50 items which makes a snapshot pretty light. (The other trick is that a `Cleared` event counts as a valid starting state for the fold - and we don't write a snapshot if we have one of those)

The big win is latency in querying contexts - given that access strategy, you're guaranteed to be able to produce the full state of the aggregate with a single roundtrip (if max batch size is 200, the sna[shots are written every 200 items so reading backward 200 guarantees a snapshot will be included)

The secondary benefit is of course that you have an absolute guarantee there will always be a snapshot, and if a given write succeeds, there will definitely be a snapshot in the `maxBatchSize` window (but it still copes if there isn't - i.e. you can add snapshotting after the fact)

`Equinox.SqlStreamStore` implements this scheme too - it's easier to do things like e.g. replace the bodies of snapshot events with `nulls` as a maintenance task in that instance

Initially, `Equinox.Cosmos` implemented the same strategy as the `Equinox.EventStore` (it started as a cut and paste of the it). However the present implementation takes advantage of the fact that in a Document Store, you can ... update documents - thus, snapshots (termed unfolds) are saved in a custom field (it's an array) in the Tip document - every update includes an updated snapshot (which is zipped to save read and write costs) which overwrites the preceding unfolds. You're currently always guaranteed that the snapshots are in sync with the latest event by virtue of how the stored proc writes. A DynamoDb impl would likely follow the same strategy

I expand (too much!) on some more of the considerations in https://github.com/jet/equinox/blob/master/DOCUMENTATION.md

The other thing that should be pointed out is the caching can typically cover a lot of perf stuff as long as stream lengths stay sane - Snapshotting (esp polluting the stream with snapshot events should definitely be toward the bottom of your list of tactics for managing a stream efficiently given long streams are typically a design smell)

<a name="changing-access-strategy"></a>
### Changing Access / Representation strategies in `Equinox.Cosmos` - what happens?

> Does Equinox adapt the stream if we start writing with `Equinox.Cosmos.AccessStrategy.RollingState` and change to `Snapshotted` for instance? It could take the last RollingState writing and make the first snapshot ?

> what about the opposite? It deletes all events and start writing `RollingState` ?

TL;DR yes and no respectively

#### Some context

Firstly, it's recommended to read the [documentation section on Access Strategies](DOCUMENTATION.md#access-strategies)

General rules:
- Events are the atoms from which state is built, they live forever in immutable Batch documents with id <> -1.
- Snapshots/unfolds live in the `.u` array in the Tip doc (id: -1)
 loading/build of state is composed of
- regardless of what happens, Events are _never_ destroyed, updated or touched in any way, ever. Having said that, if your Event DU does not match them, they're also as good as not there from the point of view of how State is established.
- Reads always get the `Tip` first (one exception: `Unoptimized` mode skips reading the `Tip` as, by definition, you're not using snapshots/unfolds/any tricks), Writes always touch the `Tip` (yes, even in `Unoptimized` mode; there's no such thing as a stream that has ever been written to that does not have a `Tip`).
- In the current implementation, the calling code in the server figures out everything that's going to go in the ~~snapshots~~ unfolds list if this sync is successful.
 
 The high level skeleton of the loading in a given access strategy is: 
   a) load and decode unfolds from tip (followed by events, if and only if necessary)
   b) offer the events to an `isOrigin` function to allow us to stop when we've got a start point (a Reset Event, a relevant snapshot, or, failing that, the start of the stream)

It may be helpful to look at [how an `AccessStrategy` is mapped to `isOrigin`, `toSnapshot` and `transmute` lambdas internally](https://github.com/jet/equinox/blob/74129903e85e01ce584b4449f629bf3e525515ea/src/Equinox.Cosmos/Cosmos.fs#L1029)

#### Aaand answering the question

Whenever a State is being built, it always loads `Tip` first and shows any ~~events~~ ~~snapshots~~ _unfolds_ in there...
 
If `isOrigin` says no to those and/or the `EventType`s of those unfolds are not in the union / event type to which the codec is mapping, the next thing is a query backwards of the Batches of events, in order.

All those get pushed onto a stack until we either hit the start, or `isOrigin` says - yes, we can start from here (at which point all the decoded events are then passed (in forward order) to the `fold` to make the `'state`).

So, if you are doing `RollingState` or any other mode, there are still events and unfolds; and they all have `EventType`s - there are just some standard combos of steps that happen.

If the `EventType` of the Event or Unfold matches, the `fold`/`evolve` will see them and build `'state` from that.

Then, whenever you emit events from a `decide` or `interpret`, the `AccessStrategy` will define what happens next; a mix of:
- write actual events (not if `RollingState`)
- write updated unfolds/snapshots
- remove or adjust events before they get passed down to the `sync` stored procedure (`Custom`, `RollingState`, `LatestKnownEvent` modes)

Ouch, not looking forward to reading all that logic :frown: ? [Have a read, it's really not that :scream:](https://github.com/jet/equinox/blob/74129903e85e01ce584b4449f629bf3e525515ea/src/Equinox.Cosmos/Cosmos.fs#L870).

### OK, but you didn't answer my question, you just talked about stuff you wanted to talk about!

😲Please raise a question-Issue, and we'll be delighted to either answer directly, or incorporate the question and answer here

# Acknowledgements

The diagrams in this README.md and the DOCUMENTATION.md would not and could not have happened without the hard work and assistance of at least:
- [@simonbrowndotje](https://github.com/simonbrowndotje) taking the time to define and evangelize the [C4 model](https://c4model.com/). It's highly recommended to view [the talk linked from `c4model.com`](https://www.youtube.com/watch?v=x2-rSnhpw0g&feature=emb_logo).
- the wonder that is [PlantUml](https://plantuml.com/); authoring them [in text](https://github.com/jet/equinox/tree/master/diagrams) with [the CVSCode PlantUML plugin](https://marketplace.visualstudio.com/items?itemName=jebbs.plantuml) is a delight. The inline images are also presently rendered by the [PlantUml proxy](https://plantuml.com/server)
- [@skleanthous](https://github.com/skleanthous) for making _and documenting_ [C4-PlantUmlSkin](https://github.com/skleanthous/C4-PlantumlSkin/blob/master/README.md)

# FURTHER READING

See [`DOCUMENTATION.md`](DOCUMENTATION.md) and [Propulsion's `DOCUMENTATION.md`](https://github.com/jet/propulsion/blob/master/DOCUMENTATION.md)