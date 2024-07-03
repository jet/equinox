# Equinox [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.equinox?branchName=master)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=4?branchName=master) [![release](https://img.shields.io/github/release/jet/equinox.svg)](https://github.com/jet/equinox/releases) [![NuGet](https://buildstats.info/nuget/Equinox?includePreReleases=true)](https://www.nuget.org/packages/Equinox/) [![license](https://img.shields.io/github/license/jet/Equinox.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/equinox.svg) [![docs status](https://img.shields.io/badge/DOCUMENTATION-WIP-important.svg?style=popout)](DOCUMENTATION.md) [![Discord](https://img.shields.io/discord/514783899440775168?color=blue&label=Chat%20in%20equinox%20on%20DDD-CQRS-ES%20Discord)](https://discord.gg/sEZGSHNNbH) [![Gitpod ready-to-code](https://img.shields.io/badge/Gitpod-ready--to--code-908a85?logo=gitpod)](https://gitpod.io/#https://github.com/jet/equinox)

Equinox is a set of low dependency libraries that allow for event-sourced processing against stream-based stores handling:
* Snapshots
* Caching
* [Optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control)

**Not a framework**; *you* compose the libraries into an architecture that fits your apps' evolving needs. 

It does not and will not handle projections and subscriptions. See [Propulsion](https://github.com/jet/propulsion) for that.

# Table of Contents

* [Getting Started](#getting-started)
* [Design Motivation](#design-motivation)
* [Features](#features)
* [Currently Supported Data Stores](#currently-supported-data-stores)
* [Components](#components)
  * [Core library](#core-library)
  * [Serialization Support](#serialization-support)
  * [Data Store Libraries](#data-store-libraries)
  * [Projection Libraries](#projection-libraries)
  * [Tools](#tools)
  * [Starter Project Templates and Sample Applications](#starter-project-templates-and-sample-applications)
* [Overview](#overview)
* [Templates](#templates)
* [Samples](#samples)
* [Building](#building)
* [Releasing](#releasing)
* [FAQ](#faq)
* [Acknowledgements](#acknowledgements)
* [Further Reading](#further-reading)

# Getting Started

- If you want to start with code samples that run in F# interactive, [there's a simple `Counter` example using `Equinox.MemoryStore`](https://github.com/jet/equinox/blob/master/samples/Tutorial/Counter.fsx#L20)
- If you are experienced with event sourcing, CosmosDB and F#, you might gain most from this [100 LOC end-to-end example using CosmosDB](https://github.com/jet/equinox/blob/master/samples/Tutorial/Cosmos.fsx#L42) 
- If you are familiar with basic event sourcing mechanisms and want a meatier example of applying Equinox to a problem, [Einar Norðfjörð](https://github.com/nordfjord)'s article, [The Equinox Programming model](https://nordfjord.io/2022/12/05/equinox.html) walks through a [complete end-to-end sample](https://github.com/nordfjord/minimal-equinox) covering the key design considerations.
- If you are experienced with CosmosDB and something like [CosmoStore](https://github.com/Dzoukr/CosmoStore), but want to understand what sort of facilities Equinox adds on top of raw event management, see the [Access Strategies guide](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#access-strategies)

# Design Motivation

Equinox's design is informed by discussions, talks and countless hours of hard and thoughtful work invested into many previous systems, [frameworks](https://github.com/NEventStore), [samples](https://github.com/thinkbeforecoding/FsUno.Prod), [forks of samples](https://github.com/bartelink/FunDomain), the outstanding continuous work of the [EventStore](https://github.com/eventstore) founders and team and the wider [DDD-CQRS-ES](https://groups.google.com/forum/#!forum/dddcqrs) community. It would be unfair to single out even a small number of people despite the immense credit that is due. Some aspects of the implementation are distilled from [`Jet.com` systems dating all the way back to 2013](http://gorodinski.com/blog/2013/02/17/domain-driven-design-with-fsharp-and-eventstore/).

An event sourcing system usually needs to address the following concerns:
1. Storing events with good performance and debugging capabilities
2. Transaction processing
    - Optimistic concurrency (handle loading conflicting events and retrying if another transaction overlaps on the same stream)
    - Folding events into a State, updating as new events are added
3. Decoding events using codecs and formats
4. Framework and application integration
5. Projections and Reactions

Designing something that supports all of these as a single integrated solution results in an inflexible and difficult to use framework. 
Thus, Equinox focuses on two central aspects of event sourcing: items 1 and 2 on the list above. 

Of course, the other concerns can't be ignored; thus, they are supported via other libraries that focus on them:
- [FsCodec](https://github.com/jet/FsCodec) supports encoding and decoding (concern 3)  
- [Propulsion](https://github.com/jet/propulsion) supports projections and reactions (concern 5)

Integration with other frameworks (e.g., Equinox wiring into ASP.NET Core) is something that is intentionally avoided; as you build your application, the nature of how you integrate things will naturally evolve.

We believe the fact Equinox is a library is critical:

  - It gives you the ability to pick your preferred way of supporting your event sourcing system.
  - There's less coupling to worry about as your application evolves over time.

_If you're looking to learn more about and/or discuss Event Sourcing and it's myriad benefits, trade-offs and pitfalls as you apply it to your Domain, look no further than the thriving 4000+ member community on the [DDD-CQRS-ES Discord](https://github.com/ddd-cqrs-es/community); you'll get patient and impartial world class advice 24x7 (there are [#equinox](https://discord.com/channels/514783899440775168/1002635005429825657), [#eventstore](https://discord.com/channels/514783899440775168/762672037113757746) and [#sql-stream-store](https://discord.com/channels/514783899440775168/762671996550774804) channels for questions or feedback)._ ([invite link](https://discord.gg/sEZGSHNNbH))

# Features

- Designed not to invade application code; your domain tests can be written directly against your models.
- Core ideas and features of the library are extracted from ideas and lessons learned from existing production software.
- Test coverage for it's core features. In addition there are baseline and specific tests for each supported storage system and a comprehensive test and benchmarking story
- Pluggable event serialization. All encoding is specified in terms of the [`FsCodec.IEventCodec` contract](https://github.com/jet/FsCodec#IEventCodec). [FsCodec](https://github.com/jet/FsCodec) provides for pluggable encoding of events based on:
  - `NewtonsoftJson.Codec`: a [versionable convention-based approach](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/) (using `Typeshape`'s `UnionContractEncoder` under the covers), providing for serializer-agnostic schema evolution with minimal boilerplate
  - `SystemTextJson.Codec`: a replacement to support Microsoft's default serializer - [System.Text.Json](https://docs.microsoft.com/en-us/dotnet/api/system.text.json?view=netcore-3.1).  
  - `Box.Codec`: lightweight [non-serializing substitute equivalent to `NewtonsoftJson.Codec` for use in unit and integration tests](https://github.com/jet/FsCodec#boxcodec)
  - `Codec`: an explicitly coded pair of `encode` and `tryDecode` functions for when you need to customize
- Caching using the .NET `MemoryCache` to:
  - Minimize round trips; consistent implementation across stores :pray: [@DSilence](https://github.com/jet/equinox/pull/161)
  - Minimize latency and bandwidth / Request Charges by maintaining the folded state, without needing the Domain Model folded state to be serializable
  - Enable read through caching, coalescing concurrent reads via opt-in `LoadOption.AllowStale`
- Mature and comprehensive logging (using [Serilog](https://github.com/serilog/serilog) internally), with optimal performance and pluggable integration with your apps hosting context (we ourselves typically feed log info to Splunk and the metrics embedded in the `Serilog.Events.LogEvent` Properties to Prometheus; see relevant tests for examples)
- OpenTelemetry Integration (presently only implemented in `Equinox.Core` and `Equinox.MessageDb` ... `#help-wanted`)
- **`Equinox.EventStore`, `Equinox.SqlStreamStore`: In-stream Rolling Snapshots**:
  - No additional round trips to the store needed at either the Load or Sync points in the flow
  - Support for multiple co-existing compaction schemas for a given stream (A 'compaction' event/snapshot is an Event). This is done by the [`FsCodec.IEventCodec`](https://github.com/jet/FsCodec#IEventCodec)
    - Compaction events typically do not get deleted (consistent with how EventStoreDB works), although it is safe to do so in concept (there are no assumptions that the events must be contiguous and/or that the number of events implies a specific version etc)
  - While snapshotting can deliver excellent performance especially when allied with the Cache, [it's not a panacea, as noted in this EventStore article on the topic](https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html)
- **`Equinox.MessageDb`: Adjacent Snapshots**:
  - Maintains snapshot events in an adjacent, separated `{Category}:snapshot-{StreamId}` stream (in contrast to the EventStoreDb and SqlStreamStore `RollingState` strategy, which embeds the snapshots directly within the stream in question)
  - Generating & storing the snapshot takes place subsequent to the normal appending of events, once every `batchSize` events. This means the state of the stream can be reconstructed with exactly 2 round-trips to the database (caching can of course remove the snapshot reads on subsequent calls)
  - Note there's no logic in the system (or in message-db as a whole) to prune snapshots (although it's safe to remove them at any time, including for the 'current' one - a fresh one will get rewritten upon the next successful event append)
- **`Equinox.CosmosStore` 'Tip with Unfolds' schema**: 
  - In contrast to `Equinox.EventStore`'s `AccessStrategy.RollingSnapshots`, when using `Equinox.CosmosStore`, optimized command processing is managed via the `Tip` - a document per stream with an identity enabling syncing the read/write position via a single [point-read](https://devblogs.microsoft.com/cosmosdb/point-reads-versus-queries). The `Tip` maintains the following: 
    - It records the current write position for the stream which is used for [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) - i.e. the index at which the next events will be appended for a given stream (events and the Tip share a common logical partition key)
    - It maintains the current [unfolds](DOCUMENTATION.md#Cosmos-Storage-Model) / snapshot data which is `deflate+base64` compressed.
    - It can maintain events in a buffer when the tip accumulation limit is reached. The limit is up to a specified count or `JSON.stringify` length. When the limit is met, events are shifted to a immutable `Batch`. 
  - Has the benefits of the in-stream Rolling Snapshots approach while reducing latency and RU provisioning requirements due to meticulously tuned Request Charge costs:
    - When the stream is empty, the initial `Load` operation involves a single point read that yields a `404 NotFound` response, costing 1.0 RU
    - When coupled with the cache, a typical read is a point read [with `IfNoneMatch` on an etag], costing 1.0 RU if in-date [to get the `304 Not Modified` response] (when the stream is empty, a `404 NotFound` response, also costing 1.0 RU)
    - Writes are a single invocation of the `Sync` stored procedure which:
        - Does a point read
        - Performs a concurrency check
        - Uses that check to apply the write OR returns the conflicting events and unfolds
    - No additional round trips to the store needed at either the `Load` or `Sync` points in the flow
  - It should be noted that from a querying perspective, the `Tip` shares the same structure as `Batch` documents (a potential future extension would be to carry some events in the `Tip` as [some interim versions of the implementation once did](https://github.com/jet/equinox/pull/58), see also [#109](https://github.com/jet/equinox/pull/109).
- **`Equinox.CosmosStore` `RollingState` and `Custom` 'non-event-sourced' modes**: 
    - Uses 'Tip with Unfolds' encoding to avoid having to write event documents at all. This option benefits from the caching and consistency management mechanisms because the cost of writing and storing infinitely increasing events are removed. Search for `transmute` or `RollingState` in the `samples` and/or see [the `Checkpoint` Aggregate in Propulsion](https://github.com/jet/propulsion/blob/master/src/Propulsion.EventStore/Checkpoint.fs). One chief use of this mechanism is for tracking Summary Event feeds in [the `dotnet-templates` `summaryConsumer` template](https://github.com/jet/dotnet-templates/tree/master/propulsion-summary-consumer).
- **`Equinox.DynamoStore`**:
    - Most features and behaviors are as per `Equinox.CosmosStore`, with the following key differences:
      - Instead of using a Stored Procedure as `CosmosStore` does, the implementation involves:
        - conditional `PutItem` and [`UpdateItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html) requests to accumulate events in the Tip (where there is space available).
        - All event writes are guaranteed to first be inserted or appended to the Tip (to guarantee the DynamoDB Streams output is correctly ordered)
          [see #401](https://github.com/jet/equinox/pull/401) and [Propulsion #222](https://github.com/jet/propulsion/pull/222) :pray: [@epNickColeman](https://github.com/epNickColeman)
        - At the point where the Tip exceeds any of the configured and/or implicit limits, a [`TransactWriteItems`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html) request is used (see [implementation in `FSharp.AWS.DynamoDB`](https://github.com/fsprojects/FSharp.AWS.DynamoDB/pull/48)):
          - maximum event count (not limited by default)
          - maximum accumulated event size (default 32KiB)
          - DynamoDB Item Size Limit (hard limit of 400KiB)
      - DynamoDB does not support an etag-checked Read API, which means a cache hit is not as efficient as it is on CosmosDB (and the data hence travels and is deserialized unnecessarily)
      - Concurrency conflicts necessitate an additional roundtrip to resync [as the DynamoDB Service does not yield the item in the event of a `ConditionalCheckFailedException`](https://stackoverflow.com/questions/71622525)
        - NOTE: As of 30 June 2023, DDB now supports returning the conflicting events; TODO implement resync without an extra roundtrip via https://github.com/fsprojects/FSharp.AWS.DynamoDB/issues/68 
      - `Equinox.Cosmos.Core.Events.appendAtEnd`/`NonIdempotentAppend` has not been ported (there's no obvious clean and efficient way to do a conditional insert/update/split as the CosmosDB stored proc can, and this is a low usage feature)
      - The implementation uses [the excellent `FSharp.AWS.DynamoDB` library](https://github.com/fsprojects/FSharp.AWS.DynamoDB)) (which wraps the standard AWS `AWSSDK.DynamoDBv2` SDK Package), and leans on [significant preparatory research](https://github.com/pierregoudjo/dynamodb_conditional_writes) :pray: [@pierregoudjo](https://github.com/pierregoudjo)
      - `CosmosStore` dictates (as of V4) that event bodies be supplied as `System.Text.Json.JsonElement`s (in order that events can be included in the Document/ Items as JSON directly. This is also to underscore the fact that the only reasonable format to use is valid JSON; binary data would need to be base64 encoded. `DynamoStore` accepts and yields event bodies as arbitrary `ReadOnlyMemory<byte>` BLOBs (the AWS SDK round-trips such blobs as a `MemoryStream` and does not impose any restrictions on the blobs in terms of required format).
      - `CosmosStore` defaults to compressing (with `System.IO.Compression.DeflateStream`) event bodies for Unfolds; `DynamoStore` round-trips an `encoding: int` value, which enables the `IEventCodec` to manage that concern. Regardless, minimizing Request Charges is imperative when request size directly maps to financial charges, 429s, reduced throughput and a lowered scaling ceiling.
    - Azure CosmosDB's ChangeFeed API intrinsically supports replays of all the events in a Store, whereas the DynamoDB Streams facility only retains 24h of actions. As a result, there are ancillary components that provide equivalent functionality composed of:
      - `Propulsion.DynamoStore.Lambda`: an AWS Lambda that is configured via a DynamoDB Streams Trigger to Index the Events (represented as Equinox Streams, typically in a separated `<tableName>-index` Table) as they are appended 
      - `Propulsion.DynamoStore.DynamoStoreSource`: consumes the Index Streams akin to how `Propulsion.CosmosStore.CosmosStoreSource` consumes the CosmosDB Change Feed 

# Currently Supported Data Stores

- `MemoryStore`: In-memory store (volatile, for unit or integration test purposes). Fulfils the full contract Equinox imposes on a store, but without I/O costs [(it's ~100 LOC wrapping a `ConcurrentDictionary`)](https://github.com/jet/equinox/blob/master/src/Equinox.MemoryStore/MemoryStore.fs). Also enables [take serialization/deserialization out of the picture](https://github.com/jet/FsCodec#boxcodec) in tests. See also [`Propulsion.MemoryStore` Change Feed Simulator for integration testing of Reactors](https://github.com/jet/dotnet-templates#eqxshipping). 
- [Amazon Dynamo DB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html): Shares most features with `Equinox.CosmosStore` ([from which it was ported in #321](https://github.com/jet/equinox/pull/321)). See above for detailed comparison.
- [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db): contains some fragments of code dating back to 2016, however [the storage model](DOCUMENTATION.md#Cosmos-Storage-Model) was arrived at based on intensive benchmarking (squash-merged in [#42](https://github.com/jet/equinox/pull/42)). The V2, 3 and 4 release lines are being used in production systems. (The V3 release provides support for significantly more efficient packing of events ([storing events in the 'Tip'](https://github.com/jet/equinox/pull/251))). :pray: [@dongdongcai](https://github.com/dongdongcai)
- [EventStoreDB](https://eventstore.org/): this codebase itself has been in production since 2017 (see commit history), with key elements dating back to approx 2016. Current versions require EventStoreDB Server editions `21.10` or later, and communicate over the modern gRPC interface.
- [MessageDB](http://docs.eventide-project.org/user-guide/message-db): bindings for the `message-db` Postgres event storage system. [See `MessageDB` docs](http://docs.eventide-project.org/user-guide/message-db). :pray: [@nordfjord](https://github.com/nordfjord)
- [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore): bindings for the powerful and widely used (**but presently unmaintained**) SQL-backed Event Storage system, derived from the EventStoreDB adapter. [See SqlStreamStore docs](https://sqlstreamstore.readthedocs.io/en/latest/#introduction). :pray: [@rajivhost](https://github.com/rajivhost)

  __NOTE: The underlying `SqlStreamStore` project is presently unmaintained; as such its hard to recommend using it for production scenarios__
  - For SQL Server, the implementation works, people are believed to be using it to varying degrees and there are no obviously better replacements. However it should be pointed out that something as simple as fixing a bug in a SqlStreamStore.SqlServer library is not a paved path - there is no CI system for it, and nobody to call

  - For MySql, it's pretty much the same as for SQL Server, with the proviso that it's likely that it's had (and has) significantly lower adoption and/or proper scrutiny.
  
  - For Postgres, _`Equinox.MessageDb`_ is a better choice as the underlying [MessageDB](http://docs.eventide-project.org/user-guide/message-db/) project is actively maintained and has significantly better documentation than SqlStreamStore. The other thing to point out is that `Equinox.SqlStreamStore.Postgres` does not depend on the final version of `SqlStreamStore.Postgres` as there are no equivalent releases of the `.Mysql` and `.SqlServer` variants.

# Components

The components within this repository are delivered as multi-targeted Nuget packages supporting `net6.0` (F# >= 6) profiles; each of the constituent elements is designed to be easily swappable as dictated by the task at hand. Each of the components can be inlined or customized easily:-

## Core library

- `Equinox` [![NuGet](https://img.shields.io/nuget/v/Equinox.svg)](https://www.nuget.org/packages/Equinox/): Store-agnostic decision flow runner that manages the optimistic concurrency protocol and application-level API surface, together with the default [`System.Runtime.Caching.MemoryCache`-based] `Cache` implementation. ([depends](https://www.fuget.org/packages/Equinox) only on `FSharp.Core` v `6.0.7`, `FsCodec` v `3.0.0`, `System.Runtime.Caching`, `Serilog` (but not specific Serilog sinks, i.e. you configure to emit to `NLog` etc)

## Serialization support

- `FsCodec` [![Codec NuGet](https://img.shields.io/nuget/v/FsCodec.svg)](https://www.nuget.org/packages/FsCodec/): Defines minimal `IEventData`, `ITimelineEvent` and `IEventCodec` contracts, which are the sole aspects the Stores bind to. No dependencies.
  - [`FsCodec.IEventCodec`](https://github.com/jet/FsCodec/blob/master/src/FsCodec/FsCodec.fs#L31): defines a base interface for a serializer/deserializer.
  - `FsCodec.Codec`: enables plugging in a serializer and/or Union Encoder of your choice (typically this is used to supply a pair of functions:- `encode` and `tryDecode`)
  ([depends](https://www.fuget.org/packages/FsCodec) on nothing 
- `FsCodec.NewtonsoftJson` [![Newtonsoft.Json Codec NuGet](https://img.shields.io/nuget/v/FsCodec.NewtonsoftJson.svg)](https://www.nuget.org/packages/FsCodec.NewtonsoftJson/)
  - As described in [a scheme for the serializing Events modelled as an F# Discriminated Union](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/), allows tagging of F# Discriminated Union cases in a versionable manner with low-dependencies using[TypeShape](https://github.com/eiriktsarpalis/TypeShape)'s [`UnionContractEncoder`](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores)
  - uses Json.net to serialize the event bodies.
  - `FsCodec.Box.Codec`: Testing substitute for `FsCodec.NewtonsoftJson.Codec`, included in same package.
  - ([depends](https://www.fuget.org/packages/FsCodec.NewtonsoftJson) on `FsCodec`, `Newtonsoft.Json >= 11.0.2`, `TypeShape`, see [FsCodec repo](https://github.com/jet/FsCodec) for details)
- `FsCodec.SystemTextJson` [![SystemTextJson Codec NuGet](https://img.shields.io/nuget/v/FsCodec.SystemTextJson.svg)](https://www.nuget.org/packages/FsCodec.SystemTextJson/): Drop in replacement that allows one to target the .NET `System.Text.Json` serializer solely by changing the referenced namespace.

## Data Store libraries

- `Equinox.Core` [![NuGet](https://img.shields.io/nuget/v/Equinox.Core.svg)](https://www.nuget.org/packages/Equinox.Core/): Hosts generic utility types frequently useful alongside Equinox: [`TaskCell`](https://github.com/jet/equinox/blob/master/src/Equinox.Core/TaskCell.fs#L36), [`Batcher`, `BatcherCache`, `BatcherDictionary`](https://github.com/jet/equinox/blob/master/src/Equinox.Core/Batching.fs#L44). ([depends](https://www.fuget.org/packages/Equinox.Core) on `System.Runtime.Caching`)
- `Equinox.MemoryStore` [![MemoryStore NuGet](https://img.shields.io/nuget/v/Equinox.MemoryStore.svg)](https://www.nuget.org/packages/Equinox.MemoryStore/): In-memory store for integration testing/performance base-lining/providing out-of-the-box zero dependency storage for examples. ([depends](https://www.fuget.org/packages/Equinox.MemoryStore) on `Equinox`)
- `Equinox.CosmosStore` [![CosmosStore NuGet](https://img.shields.io/nuget/v/Equinox.CosmosStore.svg)](https://www.nuget.org/packages/Equinox.CosmosStore/): Azure CosmosDB Adapter with integrated 'unfolds' feature, facilitating optimal read performance in terms of latency and RU costs, instrumented to meet Jet's production monitoring requirements. ([depends](https://www.fuget.org/packages/Equinox.CosmosStore) on `Equinox` v `4.0.3`, `Equinox`, `Microsoft.Azure.Cosmos` >= `3.38.1`, `System.Text.Json`, `FSharp.Control.TaskSeq`)
- `Equinox.CosmosStore.Prometheus` [![CosmosStore.Prometheus NuGet](https://img.shields.io/nuget/v/Equinox.CosmosStore.Prometheus.svg)](https://www.nuget.org/packages/Equinox.CosmosStore.Prometheus/): Integration package providing a `Serilog.Core.ILogEventSink` that extracts detailed metrics information attached to the `LogEvent`s and feeds them to the `prometheus-net`'s `Prometheus.Metrics` static instance. ([depends](https://www.fuget.org/packages/Equinox.CosmosStore.Prometheus) on `Equinox.CosmosStore`, `prometheus-net >= 3.6.0`)
- `Equinox.DynamoStore` [![DynamoStore NuGet](https://img.shields.io/nuget/v/Equinox.DynamoStore.svg)](https://www.nuget.org/packages/Equinox.DynamoStore/): Amazon DynamoDB Adapter with integrated 'unfolds' feature, facilitating optimal read performance in terms of latency and RC costs, patterned after `Equinox.CosmosStore`. ([depends](https://www.fuget.org/packages/Equinox.DynamoStore) on `Equinox`, `FSharp.AWS.DynamoDB` >= `0.12.0-beta`, `FSharp.Control.TaskSeq`)
- `Equinox.DynamoStore.Prometheus` [![DynamoStore.Prometheus NuGet](https://img.shields.io/nuget/v/Equinox.DynamoStore.Prometheus.svg)](https://www.nuget.org/packages/Equinox.DynamoStore.Prometheus/): Integration package providing a `Serilog.Core.ILogEventSink` that extracts detailed metrics information attached to the `LogEvent`s and feeds them to the `prometheus-net`'s `Prometheus.Metrics` static instance. ([depends](https://www.fuget.org/packages/Equinox.CosmosStore.Prometheus) on `Equinox.DynamoStore`, `prometheus-net >= 3.6.0`)
- `Equinox.EventStore` [![EventStore NuGet](https://img.shields.io/nuget/v/Equinox.EventStore.svg)](https://www.nuget.org/packages/Equinox.EventStore/): [EventStoreDB](https://eventstore.org/) Adapter designed to meet Jet's production monitoring requirements. ([depends](https://www.fuget.org/packages/Equinox.EventStore) on `Equinox`, `EventStore.Client >= 22.0.0`, `FSharp.Control.TaskSeq`), EventStore Server version `21.10` or later). **NO NOT use for new projects - the TCP interface to EventStoreDB has long been deprecated, this package is only provided to ease migration scenarios and will be removed in due course**
- `Equinox.EventStoreDb` [![EventStoreDb NuGet](https://img.shields.io/nuget/v/Equinox.EventStoreDb.svg)](https://www.nuget.org/packages/Equinox.EventStoreDb/): Production-strength [EventStoreDB](https://eventstore.org/) Adapter. ([depends](https://www.fuget.org/packages/Equinox.EventStoreDb) on `Equinox`, `EventStore.Client.Grpc.Streams` >= `22.0.0`, `FSharp.Control.TaskSeq`, EventStore Server version `21.10` or later)
- `Equinox.MessageDb` [![MessageDb NuGet](https://img.shields.io/nuget/v/Equinox.MessageDb.svg)](https://www.nuget.org/packages/Equinox.MessageDb/): [MessageDb](http://docs.eventide-project.org/user-guide/message-db/) Adapter. ([depends](https://www.fuget.org/packages/Equinox.MessageDb) on `Equinox`, `Npgsql` >= `7.0.7`, `FSharp.Control.TaskSeq`))
- `Equinox.SqlStreamStore` [![SqlStreamStore NuGet](https://img.shields.io/nuget/v/Equinox.SqlStreamStore.svg)](https://www.nuget.org/packages/Equinox.SqlStreamStore/): [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore) Adapter derived from `Equinox.EventStore` - provides core facilities (but does not connect to a specific database; see sibling `SqlStreamStore`.* packages). ([depends](https://www.fuget.org/packages/Equinox.SqlStreamStore) on `Equinox`, `SqlStreamStore >= 1.2.0-beta.8`, `FSharp.Control.TaskSeq`)
- `Equinox.SqlStreamStore.MsSql` [![MsSql NuGet](https://img.shields.io/nuget/v/Equinox.SqlStreamStore.MsSql.svg)](https://www.nuget.org/packages/Equinox.SqlStreamStore.MsSql/): [SqlStreamStore.MsSql](https://sqlstreamstore.readthedocs.io/en/latest/sqlserver) Sql Server `Connector` implementation for `Equinox.SqlStreamStore` package). ([depends](https://www.fuget.org/packages/Equinox.SqlStreamStore.MsSql) on `Equinox.SqlStreamStore`, `SqlStreamStore.MsSql >= 1.2.0-beta.8`)
- `Equinox.SqlStreamStore.MySql` [![MySql NuGet](https://img.shields.io/nuget/v/Equinox.SqlStreamStore.MySql.svg)](https://www.nuget.org/packages/Equinox.SqlStreamStore.MySql/): `SqlStreamStore.MySql` MySQL `Connector` implementation for `Equinox.SqlStreamStore` package). ([depends](https://www.fuget.org/packages/Equinox.SqlStreamStore.MySql) on `Equinox.SqlStreamStore`, `SqlStreamStore.MySql >= 1.2.0-beta.8`)
- `Equinox.SqlStreamStore.Postgres` [![Postgres NuGet](https://img.shields.io/nuget/v/Equinox.SqlStreamStore.Postgres.svg)](https://www.nuget.org/packages/Equinox.SqlStreamStore.Postgres/): [SqlStreamStore.Postgres](https://sqlstreamstore.readthedocs.io/en/latest/postgres) PostgreSQL `Connector` implementation for `Equinox.SqlStreamStore` package). ([depends](https://www.fuget.org/packages/Equinox.SqlStreamStore.Postgres) on `Equinox.SqlStreamStore`, `SqlStreamStore.Postgres >= 1.2.0-beta.8`)

## Projection libraries

Equinox does not focus on projection logic - each store brings its own strengths, needs, opportunities and idiosyncrasies. Here's a list of some relevant libraries from sibling projects that get used with regard to this:

- `FsKafka` [![FsKafka NuGet](https://img.shields.io/nuget/v/FsKafka.svg)](https://www.nuget.org/packages/FsKafka/): Wraps `Confluent.Kafka` to provide efficient batched Kafka Producer and Consumer configurations, with basic logging instrumentation. Used in the [`propulsion sync kafka`](https://github.com/jet/propulsion#dotnet-tool-provisioning--projections-test-tool) tool command; see [`dotnet new proProjector -k; dotnet new proConsumer` to generate a sample app](https://github.com/jet/dotnet-templates#propulsion-related) using it (see the `BatchedAsync` and `BatchedSync` modules in `Examples.fs`).
- `Propulsion` [![Propulsion NuGet](https://img.shields.io/nuget/v/Propulsion.svg)](https://www.nuget.org/packages/Propulsion/): A library that provides an easy way to implement projection logic. It defines `Propulsion.Streams.StreamEvent` used to interop with `Propulsion.*` in processing pipelines for the `proProjector` and `proSync` templates in the [templates repo](https://github.com/jet/dotnet-templates), together with the `Ingestion`, `Streams`, `Progress` and `Parallel` modules that get composed into those processing pipelines. ([depends](https://www.fuget.org/packages/Propulsion) on `Serilog`)
- `Propulsion.Cosmos` [![Propulsion.Cosmos NuGet](https://img.shields.io/nuget/v/Propulsion.Cosmos.svg)](https://www.nuget.org/packages/Propulsion.Cosmos/): Wraps the [Microsoft .NET `ChangeFeedProcessor` library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet) providing a [processor loop](DOCUMENTATION.md#change-feed-processors) that maintains a continuous query loop per CosmosDB Physical Partition (Range) yielding new or updated documents (optionally unrolling events written by `Equinox.CosmosStore` for processing or forwarding). ([depends](https://www.fuget.org/packages/Propulsion.Cosmos) on `Equinox.Cosmos`, `Microsoft.Azure.DocumentDb.ChangeFeedProcessor >= 2.2.5`)
- `Propulsion.CosmosStore` [![Propulsion.CosmosStore NuGet](https://img.shields.io/nuget/v/Propulsion.CosmosStore.svg)](https://www.nuget.org/packages/Propulsion.CosmosStore/): Wraps the CosmosDB V3 SDK's Change Feed API, providing a [processor loop](DOCUMENTATION.md#change-feed-processors) that maintains a continuous query loop per CosmosDB Physical Partition (Range) yielding new or updated documents (optionally unrolling events written by `Equinox.CosmosStore` for processing or forwarding). Used in the [`propulsion sync stats from cosmos`](dotnet-tool-provisioning--benchmarking-tool) tool command; see [`dotnet new proProjector` to generate a sample app](#quickstart) using it. ([depends](https://www.fuget.org/packages/Propulsion.CosmosStore) on `Equinox.CosmosStore`)
- `Propulsion.DynamoStore` [![Propulsion.DynamoStore NuGet](https://img.shields.io/nuget/v/Propulsion.DynamoStore.svg)](https://www.nuget.org/packages/Propulsion.DynamoStore/): Provides a `DynamoStoreSource` that provides equivalent functionality to `Propulsion.CosmosStore` in concert with `Propulsion.DynamoStore.Lambda`; ([depends](https://www.fuget.org/packages/Propulsion.DynamoStore) on `Equinox.DynamoStore`)
- `Propulsion.DynamoStore.Lambda` [![Propulsion.DynamoStore.Lambda NuGet](https://img.shields.io/nuget/v/Propulsion.DynamoStore.Lambda.svg)](https://www.nuget.org/packages/Propulsion.DynamoStore.Lambda/): Indexes events written by `Equinox.DynamoStore` via a DynamoDB Streams-triggered Lambda. (Depends on `Propulsion.DynamoStore`)
- `Propulsion.EventStore` [![Propulsion.EventStore NuGet](https://img.shields.io/nuget/v/Propulsion.EventStore.svg)](https://www.nuget.org/packages/Propulsion.EventStore/) Used in the [`propulsion sync stats from es`](dotnet-tool-provisioning--benchmarking-tool) tool command; see [`dotnet new proSync` to generate a sample app](#quickstart) using it. ([depends](https://www.fuget.org/packages/Propulsion.EventStore) on `Equinox.EventStore`)
- `Propulsion.EventStoreDb` [![Propulsion.EventStoreDb NuGet](https://img.shields.io/nuget/v/Propulsion.EventStoreDb.svg)](https://www.nuget.org/packages/Propulsion.EventStoreDb/) Consumes from `EventStoreDB` v `21.10` or later using the gRPC interface. ([depends](https://www.fuget.org/packages/Propulsion.EventStoreDb) on `Equinox.EventStoreDb`)
- `Propulsion.MessageDb` [![Propulsion.MessageDb NuGet](https://img.shields.io/nuget/v/Propulsion.MessageDb.svg)](https://www.nuget.org/packages/Propulsion.MessageDb) Consumes from a `MessageDB` store (in Postgres) using `Npgsql`. ([depends](https://www.fuget.org/packages/Propulsion.MessageDb) on `Npgsql`, `Propulsion.Feed`)
- `Propulsion.Kafka` [![Propulsion.Kafka NuGet](https://img.shields.io/nuget/v/Propulsion.Kafka.svg)](https://www.nuget.org/packages/Propulsion.Kafka/): Provides a canonical `RenderedSpan` that can be used as a default format when projecting events via e.g. the Producer/Consumer pair in `dotnet new proProjector -k; dotnet new proConsumer`. ([depends](https://www.fuget.org/packages/Propulsion.Kafka) on `Newtonsoft.Json >= 11.0.2`, `Propulsion`, `FsKafka`)

## `dotnet tool` provisioning / benchmarking tool

- `Equinox.Tool` [![Tool NuGet](https://img.shields.io/nuget/v/Equinox.Tool.svg)](https://www.nuget.org/packages/Equinox.Tool/)

    - can render events from any of the stores via `eqx dump`.
    - incorporates a benchmark scenario runner, running load tests composed of transactions in `samples/Store` and `samples/TodoBackend` against any supported store; this allows perf tuning and measurement in terms of both latency and transaction charge aspects. (Install via: `dotnet tool install Equinox.Tool -g`)
    - can configure indices in Azure CosmosDB for an `Equinox.CosmosStore` Container via `eqx init`. See [here](#store-data-in-azure-cosmosdb).
    - can search for streams and/or perform a JSON export based on name (`p`) or Uncompressed `u`nfold values for an `Equinox.CosmosStore` Container in Azure CosmosDB`. See [here](#eqx-query).
    - can create tables in Amazon DynamoDB for `Equinox.DynamoStore` via `eqx initaws`.
    - can initialize databases for `SqlStreamStore` via `eqx initsql`

## Starter Project Templates and Sample Applications 

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

The repo contains a vanilla ASP.NET Core implementation of [the well-known TodoBackend Spec](https://www.todobackend.com). **NB the implementation is largely dictated by spec; no architectural guidance expressed or implied ;)**. It can be run via:

    & dotnet run --project samples/Web -S es # run against eventstore, omit `es` to use in-memory store, or see PROVISIONING EVENTSTORE
    start https://www.todobackend.com/specs/index.html?https://localhost:5001/todos # for low-level debugging / validation of hosting arrangements
    start https://www.todobackend.com/client/index.html?https://localhost:5001/todos # standard JavaScript UI
    start http://localhost:5341/#/events # see logs triggered by `-S` above in https://getseq.net        

### [STORE, see /samples/Store](/samples/Store)

The core sample in this repo is the `Store` sample, which contains code and tests extracted from real implementations (with minor simplifications in some cases).

These facts mean that:

- some of the code may be less than approachable for a beginner (e.g. some of the code is in its present form for reasons of efficiency)
- some of the code may not represent official best practice guidance that the authors would necessarily stand over (e.g., the CQRS pattern is not strictly adhered to in all circumstances; some command designs are not completely correct from an idempotency perspective)

While these things can of course be perfected through PRs, this is definitely not top of the work list for the purposes of this repo. (We'd be delighted to place links to other samples, including cleanups / rewrites of these samples written with different testing platforms, web platforms, or DDD/CQRS/ES design flavors right here).

### [m-r](https://github.com/gregoryyoung/m-r/tree/master/SimpleCQRS) port, [see samples/Store/Domain/InventoryItem.fs](samples/Store/Domain/InventoryItem.fs)

For fun, there's a direct translation of the `InventoryItem` Aggregate and Command Handler from Greg Young's [`m-r`](https://github.com/gregoryyoung/m-r/tree/master/SimpleCQRS) demo project [as one could write it in F# using Equinox](https://github.com/jet/equinox/blob/master/samples/Store/Domain/InventoryItem.fs). NB any typical presentation of this example includes copious provisos and caveats about it being a toy example written almost a decade ago.

### [`samples/Tutorial` (in this repo)](/samples/Tutorial): Annotated `.fsx` files with sample aggregate implementations

### [@ameier38](https://github.com/ameier38)'s Tutorial

[Andrew Meier](https://andrewcmeier.com) has written a very complete tutorial modeling a business domain using Equinox and EventStoreDB; includes Dockerized Suave API, test suite using Expecto, build automation using FAKE, and CI using Codefresh; see [the repo](https://github.com/ameier38/equinox-tutorial) and its [overview blog post](https://andrewcmeier.com/bi-temporal-event-sourcing).

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
    dotnet run --project Web
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

2. start the local EventStore instance on any OS:

    - Check out the github.com/jet/equinox repo
    - `docker compose up`

    For more complete instructions, follow https://developers.eventstore.com/server/v21.10/installation.html#use-docker-compose

3. generate sample app with EventStore wiring from template and start

    ```powershell
    dotnet new eqxweb -t -e # -t for todos, -e for eventstore
    dotnet run --project Web
    ```

4. browse writes at http://localhost:2113/web/index.html#/streams

### Store data in [Azure CosmosDB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction)

1. *export 3x env vars* (see [provisioning instructions](#run-cosmosdb-benchmark-when-provisioned))

    ```powershell
    $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
    $env:EQUINOX_COSMOS_DATABASE="equinox-test"
    $env:EQUINOX_COSMOS_CONTAINER="equinox-test"
    ```

2. use the `eqx` tool to initialize the database and/or container (using preceding env vars)

    ```powershell
    dotnet tool uninstall Equinox.Tool -g
    dotnet tool install Equinox.Tool -g --prerelease
    eqx init -ru 400 cosmos # generates a database+container, adds optimized indexes
    ```

3. generate sample app from template, with CosmosDB wiring

    ```powershell
    dotnet new eqxweb -t -c # -t for todos, -c for cosmos
    dotnet run --project Web
    ```

4. Use the `eqx` tool to dump stats relating the contents of the CosmosDB store

    ```powershell
    # run queries to determine how many streams, docs, events there are in the container
    eqx -V stats -P cosmos # -P to run in parallel # -V to show underlying query being used
    ```

5. Use the `eqx` tool to query streams and/or snapshots in a CosmosDB store

    <a name="eqx-query"></a>
    ```powershell
    # Add indexing of the `u`nfolds borne by Tip Items: 1) `c` for the case name 2) `d` for fields of uncompressed unfolds 
    eqx init -m serverless --indexunfolds cosmos -d db -c $EQUINOX_COSMOS_VIEWS
   
    # query all streams LIKE "$User-%" with `Snapshotted2` unfolds. Batches of up to 100,000 events
    eqx query -cn '$User' -un Snapshotted2 cosmos -d db -c $EQUINOX_COSMOS_VIEWS -b 100000
    
    # use a wild card (LIKE) for the stream name 
    eqx query -cl '$Us%' -un Snapshotted cosmos -d db -c $EQUINOX_COSMOS_VIEWS -b 100000
    # > Querying Default: SELECT c.p, c._etag, c.u[0].d FROM c WHERE c.p LIKE "$Us%" AND EXISTS (SELECT VALUE u FROM u IN c.u WHERE u.c = "Snapshotted") {}
    # > Page 7166s, 7166u, 0e 320.58RU 3.9s {}
    # > Page 1608s, 1608u, 0e 68.59RU 0.9s {}
    # > TOTALS 1c, 8774s, 389.17RU 4.7s {}   
   
    # Skip loading the _etag to simulate a query where you will only render the result (not `Transact` against it)
    eqx query -cn '$User' -m readonly -un Snapshotted cosmos -d db -c $EQUINOX_COSMOS_VIEWS -b 100000
    # > Querying ReadOnly: SELECT c.u FROM c WHERE c.p LIKE "$User-%" AND EXISTS (SELECT VALUE u FROM u IN c.u WHERE u.c = "Snapshotted") {}
    # > Page 8774s, 8774u, 0e 342.33RU 3.8s {}
    # > TOTALS 0c, 8774s, 342.33RU 3.8s {} # 👈 cheaper and only one batch as no .p or ._etag 
   
    # add criteria filtering based on an Uncompressed Unfold
    eqx query -cn '$User' -un EmailIndex -uc 'u.d.email = "a@b.com"' cosmos -d db -c $EQUINOX_COSMOS_VIEWS -b 100000
    # > Querying Default: SELECT c.p, c._etag, c.u[0].d FROM c WHERE c.p LIKE "$User-%" AND EXISTS (SELECT VALUE u FROM u IN c.u WHERE u.c = "EmailIndex" AND u.d.email = "a@b.com") {}
    # > Page 0s, 0u, 0e 2.8RU 0.7s {}
    # > TOTALS 0c, 0s, 2.80RU 0.7s {} # 👈 only 2.8RU if nothing is returned
   
    # DUMP ONE STREAM TO A FILE (equivalent to queries performed by CosmosStore.AccessStrategy.Unoptimized)
    # Can be imported into another store via `propulsion sync cosmos from json`
    eqx query -sn 'user-f28fb6feea00550e93ca77b6f29899cd' -o dump-user.json cosmos -d db -c $EQUINOX_COSMOS_CONTAINER -b 9999
    # > Dumping Raw content to ./dump-user.json {}
    # > Querying Raw: SELECT * FROM c WHERE c.p = "user-f28fb6feea00550e93ca77b6f29899cd" AND 1=1 {}
    # > Page 9s, 1u, 10e 3.23RU 0.5s 0.0MiB age 0002.10:04:13 {} # 👈 2.80 if no results, adds per KiB charge if there are results 
    # > TOTALS 1c, 9s, 3.23RU R/W 0.0/0.0MiB 3.9s {}
 
    # DUMP FULL CONTENT OF THE CONTAINER TO A FILE
    # Can be imported into another store via `propulsion sync cosmos from json`
    eqx query -o ../dump-240216.json cosmos -d db -c $EQUINOX_COSMOS_CONTAINER -b 9999                             
    # > Dumping Raw content to ~/dumps/dump-240216.json {}
    # > No StreamName or CategoryName/CategoryLike specified - Unfold Criteria better be unambiguous {}
    # > Querying Raw: SELECT * FROM c WHERE 1=1 AND 1=1 {}
    # > Page 2972s, 748u, 3112e 108.9RU 3.8s 4.0MiB age 0212.18:00:45 {}
    # > Page 3211s, 777u, 3161e 112.29RU 3.3s 4.0MiB age 0212.09:06:02 {}
    # > Page 3003s, 663u, 3172e 110.33RU 3.4s 4.0MiB age 0211.04:09:12 {}
    # <chop>
    # > Page 2768s, 498u, 3153e 107.46RU 3.0s 4.0MiB age 0016.13:09:02 {}
    # > Page 2806s, 505u, 3198e 107.17RU 3.0s 4.0MiB age 0010.18:52:45 {}
    # > Page 2903s, 601u, 3188e 107.53RU 3.1s 4.0MiB age 0004.05:24:51 {}
    # > Page 2638s, 316u, 3019e 93.09RU 2.5s 3.4MiB age 0000.05:08:38 {}
    # > TOTALS 11c, 206,356s, 7,886.75RU R/W 290.4/290.4MiB 225.3s {}
   
    # Prepare a breakdown of which categories are using the most capacity within the store
    eqx -Q top cosmos -d db -c $EQUINOX_COSMOS_CONTAINER
    # Page 3276>3276i 3276s    0e 3991u 4.00>4.00<4.22MiB 103.74RU  3.5s D+M 5.1 C+C 0.00 201ms age 0000.00:33:13 {}
    # Page 3177>3177i 3177s    0e 4593u 4.00>4.01<4.20MiB 105.22RU  3.2s D+M 4.7 C+C 0.00 146ms age 0000.02:23:48 {}
    # Page 2708>2708i 2708s    0e 5044u 4.00>4.00<4.19MiB 105.76RU  3.4s D+M 4.5 C+C 0.00  84ms age 0002.23:10:55 {}
    ...
    # Page 4334>4334i 4334s    0e 5038u 4.00>4.00<4.19MiB 112.59RU  2.9s D+M 4.2 C+C 0.00 109ms age 0000.00:00:59 {}
    # Page 1637>1637i 1637s    0e 2939u 2.40>2.41<2.52MiB  64.12RU  1.7s D+M 2.5 C+C 0.00  39ms age 0000.00:18:03 {}
    # TOTALS 47,200i 9c 47,200s 0e 79,262u read 0.1GiB output 0.1GiB JSON 0.1GiB D+M(inflated) 0.1GiB C+C 0.00MiB Parse 1.516s Total 1,750.73RU 54.2s {}
    #    24064i   40.75MiB E       0     0.0 U   48128    33.6 D+M   35.0 C+C   0.0 $Friend {}
    #     6372i   13.18MiB E       0     0.0 U   12744    11.4 D+M   23.9 C+C   0.0 $Tenant {}
    #     6374i    5.41MiB E       0     0.0 U    6374     3.6 D+M    5.4 C+C   0.0 $Role0 {}
    #     5992i    5.09MiB E       0     0.0 U    5992     3.4 D+M    5.1 C+C   0.0 $Role {}
    #     1574i    1.95MiB E       0     0.0 U    1574     1.5 D+M    2.0 C+C   0.0 $Permission {}
    #     1575i    1.79MiB E       0     0.0 U    3150     1.3 D+M    1.2 C+C   0.0 $User {}
    #      445i    0.51MiB E       0     0.0 U     483     0.4 D+M    0.8 C+C   0.0 $Invoice3 {}
    #      410i    0.46MiB E       0     0.0 U     423     0.3 D+M    0.8 C+C   0.0 $Invoice2 {}
    #      394i    0.44MiB E       0     0.0 U     394     0.3 D+M    0.7 C+C   0.0 $Invoice {}
   
    # Drill into the Friend data (different test data to preceding article)
    eqx top -cn '$Friend' cosmos -d db -c $EQUINOX_COSMOS_CONTAINER
    # Page 4787>4787i 4787s    0e 4787u 4.00>4.00<4.19MiB 218.54RU  3.6s D+M 4.5 C+C 0.00 259ms age 0013.22:52:15 {}
    # Page 4955>4955i 4955s    0e 4955u 4.00>4.00<4.19MiB 200.20RU  3.2s D+M 4.1 C+C 0.00 202ms age 0013.22:52:18 {}
    # Page 4715>4715i 4715s    0e 4715u 4.00>4.00<4.21MiB 201.26RU  3.2s D+M 4.4 C+C 0.00 145ms age 0013.22:52:22 {}
    # Page 4884>4884i 4884s    0e 4884u 4.00>4.00<4.20MiB 198.97RU  3.2s D+M 4.1 C+C 0.00  95ms age 0013.22:52:31 {}
    # Page 4620>4620i 4620s    0e 4620u 4.00>4.00<4.20MiB 194.76RU  3.0s D+M 4.7 C+C 0.00 140ms age 0013.22:52:28 {}
    # Page 4840>4840i 4840s    0e 4840u 4.00>4.00<4.19MiB 198.43RU  3.2s D+M 4.2 C+C 0.00 136ms age 0013.22:52:34 {}
    # Page 4791>4791i 4791s    0e 4791u 4.00>4.00<4.21MiB 200.20RU  3.0s D+M 4.2 C+C 0.00 137ms age 0014.02:23:24 {}
    # Page 3906>3906i 3906s    0e 3906u 3.01>3.02<3.15MiB 158.28RU  2.6s D+M 2.9 C+C 0.00 142ms age 0013.23:13:51 {}
    # TOTALS 37,498i 1c 37,498s 0e 37,498u read 0.0GiB output 0.0GiB JSON 0.0GiB D+M(inflated) 0.0GiB C+C 0.00MiB Parse 1.264s Total 1,570.64RU 30.0s {}
    #    37498i   32.55MiB E       0     0.1 U   37498    21.7 D+M   33.2 C+C   0.0 $Friend {}

    # DRY RUN of deleting (note no `-f` supplied)
    eqx destroy -cn '$Friend' cosmos -d db -c $EQUINOX_COSMOS_CONTAINER
    # W Dry-run of deleting items based on SELECT c.p, c.id, ARRAYLENGTH(c.e) AS es, ARRAYLENGTH(c.u) AS us FROM c WHERE c.p LIKE "$Friend%" {}
    # I Page  9999> 9999i    9999s      0e   9999u    8.21>0.76   415.07RRU   1.4s 0.00WRU/s   0.0s {}
    # I Page  9999> 9999i    9999s      0e   9999u    8.48>0.76   404.70RRU   0.8s 0.00WRU/s   0.0s {}
    # I Page  9999> 9999i    9999s      0e   9999u    8.32>0.76   395.36RRU   1.1s 0.00WRU/s   0.0s {}
    # I Page  7501> 7501i    7501s      0e   7501u    6.01>0.57   299.60RRU   1.0s 0.00WRU/s   0.0s {}
    # I TOTALS 37,498i 1c 37,498s 0e 37,498u read 31.0MiB output 2.9MiB 1,514.73RRU Avg 0.00WRU/s Delete 0.00WRU Total 7.8s {}
   
    # Whack them (note the `--force` supplied)
    eqx destroy -cn '$Friend' --force cosmos -d db -c $EQUINOX_COSMOS_CONTAINER
    # W DESTROYING all Items WHERE c.p LIKE "$ResourceRole%" {}
    # I .. Deleted  6347i    6347s      0e   6347u 1,671.52WRU/s   30.0s {}
    # I Page  9999> 9999i    9999s      0e   9999u    8.21>0.76   415.17RRU   1.2s 1,678.54WRU/s  47.2s {}
    # I .. Deleted  6363i    6363s      0e   6363u 1,703.29WRU/s   30.0s {}
    # I Page  9999> 9999i    9999s      0e   9999u    8.48>0.76   404.70RRU   1.1s 1,685.49WRU/s  47.8s {}
    # I .. Deleted  6001i    6001s      0e   6001u 1,571.94WRU/s   30.0s {}
    # I Page  9999> 9999i    9999s      0e   9999u    8.32>0.76   395.36RRU   1.0s 1,582.18WRU/s  50.1s {}
    ^C           
   
    # Get impatient; up the concurrency (-w 192) from the default 32 (note the `--force` supplied)
    eqx destroy -cn '$Friend' --force -w 192 cosmos -d db -c $EQUINOX_COSMOS_CONTAINER
    # W DESTROYING all Items WHERE c.p LIKE "$ResourceRole%" {}
    # I Page  3946> 3946i    3946s      0e   3946u    3.05>0.30   176.23RRU   0.8s 5,107.71WRU/s   6.1s {}
    # I TOTALS 3,946i 1c 3,946s 0e 3,946u read 3.0MiB output 0.3MiB 176.23RRU Avg 3,058.48WRU/s Delete 31,360.10WRU Total 10.3s {}

    # Analyze the largest streams in the '$Permission' category 
    eqx top -S -cl '$Perm%' cosmos -d db -c $EQUINOX_COSMOS_CONTAINER
    # I Page  254> 254i  254s    0e  254u 4.33>4.33<4.65MiB 349.76RU  3.9s D+M 8.2 C+C 0.00 105ms age 0013.23:34:02 {}
    # I Page 1671>1671i 1671s    0e 1671u 2.39>2.40<2.54MiB  91.57RU  2.1s D+M 2.9 C+C 0.00  99ms age 0013.23:34:07 {}
    # I TOTALS 1,925i 1,925c 1,925s 0e 1,925u read 0.0GiB output 0.0GiB JSON 0.0GiB D+M(inflated) 0.0GiB C+C 0.00MiB Parse 0.207s Total 441.33RU 9.4s {}
    # I     1925i    7.19MiB E       0     0.0 U    1925     6.6 D+M   11.1 C+C   0.0 $Permission {}
    # I        1i    1.75MiB E       0     0.0 U       1     1.8 D+M    3.1 C+C   0.0 $Permission-5292b7cd524d509bb969bd82abf39461 {}
    # I        1i    1.38MiB E       0     0.0 U       1     1.4 D+M    2.5 C+C   0.0 $Permission-244b72fb0238595494b5cb3f9bd1abf7 {}
    # I        1i    0.79MiB E       0     0.0 U       1     0.8 D+M    1.5 C+C   0.0 $Permission-68a13e8398b352c5b8e22ec18ab2bbb6 {}
    # I        1i    0.57MiB E       0     0.0 U       1     0.6 D+M    1.1 C+C   0.0 $Permission-ea4d1f46014a5bf6bbd97d3ec5723266 {}
    # I        1i    0.13MiB E       0     0.0 U       1     0.1 D+M    0.2 C+C   0.0 $Permission-65b58d132ff857bb81b08a5bb69732d2 {}
    # I        1i    0.02MiB E       0     0.0 U       1     0.0 D+M    0.0 C+C   0.0 $Permission-a7bcc3370ad15ae68041745ca55166cf {}
    # I        1i    0.02MiB E       0     0.0 U       1     0.0 D+M    0.0 C+C   0.0 $Permission-03032ccf597857d9aa9c64b10288af8c {}
    ```

6. Use `propulsion sync` tool to run a CosmosDB ChangeFeedProcessor

    ```powershell
    dotnet tool uninstall Propulsion.Tool -g
    dotnet tool install Propulsion.Tool -g --prerelease

    propulsion init -ru 400 cosmos # generates a -aux container for the ChangeFeedProcessor to maintain consumer group progress within
    # -V for verbose ChangeFeedProcessor logging
    # `-g projector1` represents the consumer group - >=1 are allowed, allowing multiple independent projections to run concurrently
    # stats specifies one only wants stats regarding items (other options include `kafka` to project to Kafka)
    # cosmos specifies source overrides (using defaults in step 1 in this instance)
    propulsion -V sync -g projector1 stats from cosmos
    ```

7. Generate a CosmosDB ChangeFeedProcessor sample `.fsproj` (without Kafka producer/consumer), using `Propulsion.CosmosStore`

    ```powershell
    dotnet new -i Equinox.Templates

    # note the absence of -k means the projector code will be a skeleton that does no processing besides counting the events
    dotnet new proProjector

    # start one or more Projectors
    # `-g projector2` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently
    # cosmos specifies source overrides (using defaults in step 1 in this instance)
    dotnet run -- -g projector2 cosmos
    ```
8. Use `propulsion` tool to Run a CosmosDB ChangeFeedProcessor, emitting to a Kafka topic

    ```powershell	
    $env:PROPULSION_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b	
    # `-V` for verbose logging	
    # `projector3` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently	
    # `-l 5` to report ChangeFeed lags every 5 minutes	
    # `kafka` specifies one wants to emit to Kafka	
    # `temp-topic` is the topic to emit to	
    # `cosmos` specifies source overrides (using defaults in step 1 in this instance)	
    propulsion -V sync -g projector3 -l 5 kafka temp-topic from cosmos	
    ```	

9. Generate CosmosDB [Kafka Projector and Consumer](https://github.com/jet/propulsion#feeding-to-kafka) `.fsproj`ects (using `Propulsion.Kafka`)

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

10. Generate an Archive container; Generate a ChangeFeedProcessor App to mirror desired streams from the Primary to it

    ```powershell
    # once
    eqx init -ru 400 cosmos -c equinox-test-archive
   
    md archiver | cd
   
    # Generate a template app that'll sync from the Primary (i.e. equinox-test)
    # to the Archive (i.e. equinox-test-archive)
    dotnet new proArchiver
   
    # TODO edit Handler.fs to add criteria for what to Archive
    # - Normally you won't want to Archive stuff like e.g. `Sync-` checkppoint streams
    # - Any other ephemeral application streams can be excluded too
   
    # -w 4 # constrain parallel writers in order to leave headroom for readers; Archive container should be cheaper to run
    # -S -t 40 # emit log messages for Sync calls costing > 40 RU
    # -md 20 (or lower) is recommended to be nice to the writers - the archiver can afford to lag
    dotnet run -c Release -- -w 4 -S -t 40 -g ArchiverConsumer `
      cosmos -md 20 -c equinox-test -a equinox-test-aux `
      cosmos -c equinox-test-archive 
    ``` 

11. Use a ChangeFeedProcessor driven from the Archive Container to Prune the Primary

    ```powershell
    md pruner | cd
   
    # Generate a template app that'll read from the Archive (i.e. equinox-test-archive)
    # and prune expired events from the Primary (i.e. equinox-test)
    dotnet new proPruner
   
    # TODO edit Handler.fs to add criteria for what to Prune
    # - While its possible to prune the minute it's archived, normally you'll want to allow a time lag before doing so
    
    # -w 2 # constrain parallel pruners in order to not consume RUs excessively on Primary
    # -md 10 (or lower) is recommended to contrain consumption on the Archive - Pruners lagging is rarely critical
    dotnet run -c Release -- -w 2 -g PrunerConsumer `
      cosmos -md 10 -c equinox-test-archive -a equinox-test-aux `
      cosmos -c equinox-test
    ``` 

<a name="sqlstreamstore"></a>
### Use [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore)

SqlStreamStore is provided in the samples and the `eqx` tool:

- being able to supply `ms`, `my`, `pg` flag to `eqx loadtest`, e.g. `eqx loadtest -t cart -f 50 -d 5 -C -U ms -c "sqlserverconnectionstring" -s schema`
- being able to supply `ms`, `my`, `pg` flag to `eqx dump`, e.g. `eqx dump -CU "Favorites-ab25cc9f24464d39939000aeb37ea11a" ms -c "sqlserverconnectionstring" -s schema`
- being able to supply `ms`, `my`, `pg` flag to Web sample, e.g. `dotnet run --project samples/Web/ -- my -c "mysqlconnectionstring"`
- being able to supply `ms`, `my`, `pg` flag to new `eqx initsql` command e.g. `eqx initsql pg -c "postgresconnectionstring" -u p "usercredentialsNotToBeLogged" -s schema`

```powershell
cd ~/code/equinox

# set up the DB/schema
dotnet run --project tools/Equinox.Tool -- initsql pg -c "connectionstring" -p "u=un;p=password" -s "schema"

# run a benchmark
dotnet run -c Release --project tools/Equinox.Tool -- loadtest -t saveforlater -f 50 -d 5 -C -U pg -c "connectionstring" -p "u=un;p=password" -s "schema"

# run the webserver, -A to autocreate schema on connection
dotnet run --project samples/Web/ -- my -c "mysqlconnectionstring" -A

# set up the DB/schema
eqx initsql pg -c "connectionstring" -p "u=un;p=password" -s "schema"

# run a benchmark
eqx loadtest -t saveforlater -f 50 -d 5 -C -U pg -c "connectionstring" -p "u=un;p=password" -s "schema" 
eqx dump "SavedForLater-ab25cc9f24464d39939000aeb37ea11a" pg -c "connectionstring" -p "u=un;p=password" -s "schema" # show stored JSON (Guid shown in eqx loadtest output) 
```

<a name="message-db"></a>
### Use [MessageDB](http://docs.eventide-project.org/user-guide/message-db/)

MessageDb support is provided in the samples and the `eqx` tool:

- being able to supply `mdb` flag to `eqx loadtest`, e.g. `eqx loadtest -f 50 -d 5 -C -U mdb -c "pgconnectionstring"`
- being able to supply `mdb` flag to `eqx dump`, e.g. `eqx dump -CU "Favorites-ab25cc9f24464d39939000aeb37ea11a" mdb -c "pgconnectionstring"`
- being able to supply `mdb` flag to Web sample, e.g. `dotnet run --project samples/Web/ -- mdb -c "pgconnectionstring"`

Equinox does not provide utilities for configuring or installing MessageDB. See [MessageDB's installation documentation](http://docs.eventide-project.org/user-guide/message-db/install.html).

In addition to the default access strategy of reading the whole stream forwards in batches, the following access strategies are supported in MessageDb:

`AccesStrategy.LatestKnownEvent`
- Uses message-db's `get_last_stream_message` API to only ever fetch the last event in a stream
- This is useful for aggregates whose entire state can be constructed from the latest event (e.g. a stream that stores checkpoints)
- NOTE: The last event should be decodable by the supplied codec. Otherwise you'll receive the initial state.

`AccessStrategy.AdjacentSnapshots`
- Generates and stores a snapshot event in an adjacent `{Category}:snapshot-{StreamId}` stream
- The generation happens every `batchSize` events. This means the state of the stream can be reconstructed with exactly 2 round-trips to the database.
  - The first round-trip fetches the most recent event of type `snapshotEventCaseName` from the snapshot stream.
  - The second round-trip fetches `batchSize` events from the position of the snapshot

<a name="dynamodb"></a>
### Use [Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html)

DynamoDB is supported in the samples and the `eqx` tool equivalent to the CosmosDB support as described:
- being able to supply `dynamo` source to `eqx loadtest` wherever `cosmos` works, e.g. `eqx loadtest -t cart -f 50 -d 5 -CU dynamo -s http://localhost:8000 -t TableName`
- being able to supply `dynamo` flag to `eqx dump`, e.g. `eqx dump -CU "Favorites-ab25cc9f24464d39939000aeb37ea11a" dynamo`
- being able to supply `dynamo` flag to Web sample, e.g. `dotnet run --project samples/Web/ -- dynamo -s http://localhost:8000`
- being able to supply `dynamo` flag to `eqx initaws` command e.g. `eqx initaws -r 10 -w 10 -s new dynamo -t TableName`

1. The tooling and samples in this repo default to using the following environment variables (see [AWS CLI UserGuide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)
   for more detailed guidance as to specific configuration)

    ```zsh
    $env:EQUINOX_DYNAMO_SERVICE_URL="https://dynamodb.us-west-2.amazonaws.com" # Simulator: "http://localhost:8000"
    $env:EQUINOX_DYNAMO_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
    $env:EQUINOX_DYNAMO_SECRET_ACCESS_KEY="AwJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    $env:EQUINOX_DYNAMO_TABLE="equinox-test"
    $env:EQUINOX_DYNAMO_TABLE_ARCHIVE="equinox-test-archive"
    ```

2. Tour of the tools/samples:

    ```zsh
    cd ~/code/equinox

    # start the simulator at http://localhost:8000 and an admin console at http://localhost:8001/
    docker compose up dynamodb-local dynamodb-admin -d

    # Establish the table in us-east-1 - keys come from $EQUINOX_DYNAMO_ACCESS_KEY_ID and $EQUINOX_DYNAMO_SECRET_ACCESS_KEY
    dotnet run --project tools/Equinox.Tool -- initaws -r 10 -w 10 -s new dynamo -t TableName -su https://dynamodb.us-east-1.amazonaws.com

    # Check the status and get the streams ARN - keys come from AWS SDK config for us-east-1 region
    dotnet run --project tools/Equinox.Tool -- stats dynamo -t TableName -sr us-east-1
   
    # run a benchmark
    dotnet run -c Release --project tools/Equinox.Tool -- loadtest -t saveforlater -f 50 -d 5 -CU dynamo

    # run the webserver
    dotnet run --project samples/Web/ -- dynamo -t TableName

    # run a benchmark connecting to the webserver
    eqx loadtest -t saveforlater -f 50 -d 5 -CU web
    eqx dump "SavedForLater-ab25cc9f24464d39939000aeb37ea11a" dynamo # show stored JSON (Guid shown in eqx loadtest output) 
    ```

3. Useful articles

- [Troubleshooting throttling issues in Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TroubleshootingThrottling.html)
- [Troubleshooting latency issues in Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TroubleshootingLatency.html)

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

Run, including running the tests that assume you've got a local EventStore and pointers to a CosmosDB database and container prepared (see [PROVISIONING](#provisioning)):

    ./build.ps1

### build, skipping tests that require a Store instance

    ./build -s

### build, skipping all tests

    dotnet pack build.proj

### build, skip EventStore tests

    ./build -se

### build, skip EventStore tests, skip auto-provisioning / de-provisioning CosmosDB

    ./build -se -scp

### Run EventStore benchmark on .NET Core (when provisioned)

At present, .NET Core seems to show comparable perf under normal load, but becomes very unpredictable under load. The following benchmark should produce pretty consistent levels of reads and writes, and can be used as a baseline for investigation:

    & dotnet run -c Release --project tools/Equinox.Tool -- loadtest -t saveforlater -f 1000 -d 5 -C -U es

### run Web benchmark

The CLI can drive the Store and TodoBackend samples in the `samples/Web` ASP.NET Core app. Doing so requires starting a web process with an appropriate store (EventStore in this example, but can be `memory` / omitted etc. as in the other examples)

#### in Window 1

    & dotnet run -c Release --project samples/Web -- -C -U es

#### in Window 2

    dotnet tool install -g Equinox.Tool --prerelease # only once
    eqx loadtest -t saveforlater -f 200 web

### run CosmosDB benchmark (when provisioned)

    dotnet run --project tools/Equinox.Tool -- loadtest `
      cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER

## PROVISIONING

### Provisioning EventStore (when not using -s or -se)

There's a `docker-compose.yml` file in the root, so installing `docker-compose` and then running `docker-compose up` rigs a local 3-node cluster, which is assumed to be configured for `Equinox.EventStore.Integration` and `Equinox.EventStoreDb.Integration`

For more complete instructions, follow https://developers.eventstore.com/server/v21.10/installation.html#use-docker-compose

<a name="provisioning-cosmosdb"></a>
### Provisioning CosmosDB (when not using build.ps1 -sc to skip verification)

#### Using Azure Cosmos DB Service

```bash
dotnet run --project tools/Equinox.Tool -- init -ru 400 `
    cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER
# Same for a Archive Container for integration testing of the archive store fallback mechanism
$env:EQUINOX_COSMOS_CONTAINER_ARCHIVE="equinox-test-archive"
dotnet run --project tools/Equinox.Tool -- init -ru 400 `
    cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER_ARCHIVE
```

#### Using Cosmos Emulator on an Intel Mac 

NOTE There's [no Apple Silicon emulator available as yet](https://github.com/Azure/azure-cosmos-db-emulator-docker/issues/54#issuecomment-1399067365).

NOTE Have not tested with the Windows Emulator, but it should work with analogous steps.

```bash
docker compose up equinox-cosmos -d
bash docker-compose-cosmos.sh
```

### Provisioning SqlStreamStore

There's a `docker-compose.yml` file in the root, so installing `docker-compose` and then running `docker-compose up` rigs local `equinox-mssql`, `equinox-mysql` and `equinox-postgres` servers and databases at known ports. _NOTE The `Equinox.SqlStreamStore.*.Integration` suites currently assume this is in place and will otherwise fail_.

## DEPROVISIONING

### Deprovisioning (aka nuking) EventStore data resulting from tests to reset baseline

While EventStore rarely shows any negative effects from repeated load test runs, it can be useful for various reasons to drop all the data generated by the load tests by casting it to the winds:-

    # requires admin privilege
    rm $env:ProgramData\chocolatey\lib\eventstore-oss\tools\data

### Deprovisioning CosmosDB

The [provisioning](#provisioning) step spins up RUs in CosmosDB for the Container, which will keep draining your account until you reach a spending limit (if you're lucky!). *When finished running any test, it's critical to drop the RU allocations back down again via some mechanism (either delete the container or reset the RU provision down to the lowest possible value)*.

- Kill the container and/or database
- Use the portal to change the allocation

# RELEASING

*The perfect is the enemy of the good; [all this should of course be automated, but the elephant will be consumed in small bites rather than waiting till someone does it perfectly](https://github.com/jet/equinox/issues/80). This documents the actual release checklist as it stands right now. Any small helping bites much appreciated :pray: *

## Tagging releases

This repo uses [MinVer](https://github.com/adamralph/minver); [see here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All non-alpha releases derive from tagged commits on `master` or `vX` branch. The tag defines the nuget package id etc. that the release will bear (`dotnet pack` uses the `MinVer` package to grab the value from the commit)

## Checklist

- :cry: the Azure Pipelines script does not run the integration tests, so these need to be run manually via the following steps:

  - [Provision](#provisioning):
    - Set environment variables x 4 for a CosmosDB database and container (you might need to `eqx init`)
    - Add a `EQUINOX_COSMOS_CONTAINER_ARCHIVE` environment variable referencing a separate (`eqx init` initialized) CosmosDB Container that will be used to house fallback events in the [Fallback mechanism's tests](https://github.com/jet/equinox/pull/247)
    - `docker-compose up` to start
      - 3 servers for the `SqlStreamStore.*.Integration` test suites (NOTE: manual step for MS SQL)
      - 3 `EventStoreDB` cluster nodes
      - DynamoDB local and admin images and (`dynamodb-local`, `dynamodb-admin`) 
  - Run `./build.ps1` in PowerShell (or PowerShell Core on MacOS via `brew install cask pwsh`)

- [CHANGELOG](CHANGELOG.md) should be up to date
- commit should be tagged (remember to do `git push --tags` when pushing)
- after the push has resulted in a successful build, click through from the commit on github thru to the Azure Pipelines build state and verify _all_ artifacts bear the correct version suffix (if the tags were not pushed alongside the commit, they can be wrong). Then, and only then, do the Release (which will upload to nuget.org using a nuget API key that has upload permissions for the packages)
- _When adding new packages_: For safety, the NuGet API Key used by the Azure DevOps Releases step can only upload new versions of existing packages. As a result, the first version of any new package needs to be manually uploaded out of band. (then invite jet.com to become owner so subsequent releases can do an automated upload [after the request has been (manually) accepted])

## FAQ

### What _is_ Equinox?

OK, I've read the README and the tagline. I still don't know what it does! Really, what's the TL;DR ?

- supports storing events in [EventStore](https://eventstore.org), including working with existing data you may have (that's where it got its start)
- includes a proprietary optimized Store implementation that only needs an empty Azure CosmosDB Account or Amazon DynamoDB Table to get going
- provides all the necessary infrastructure to build idempotent synchronous command processing against all of the stores; your Domain code intentionally doesn't need to reference *any* Equinox modules whatsoever (although for smaller systems, you'll often group `Events`+`Fold`+`interpret`/`decide`+`Service` in a single `module`, which implies a reference to [the core `Equinox` package](src/Equinox)).
- following on from the previous point: you just write the unit tests without any Equinox-specific hoops to jump through; this really works very well indeed, assuming you're writing the domain code and the tests in F#. If you're working in a more verbose language, you may end up building some test helpers. We don't envisage Equinox mandating a specific pattern on the unit testing side (consistent naming such as `Events.Event`+`evolve`+`fold`+`Command`+`interpret`/`decide` can help though).
- it helps with integration testing decision processes by
  - staying out of your way as much as possible
  - providing an in-memory store that implements the same interface as the concrete stores (CosmosDB, EventStore, etc.)  stores do
- There is a projection story, but it's not baked in - any 3 proper architects can come up with at least 3 wrong and 3 right ways of running those:-
  - For EventStore, you can use its' projections facilities directly. There's also a `Propulsion.EventStore` that serves the needs of `dotnet new proSync`..
  - for CosmosDB, you use the `Propulsion.CosmosStore` libraries to consume the CosmosDB ChangeFeed using the `Microsoft.Azure.Cosmos` library's change feed support (and, optionally, project to/consume from Kafka) using the sample app templates (`dotnet new proProjector`).

### Should I use Equinox to learn event sourcing ?

You _could_. However the Equinox codebase itself is not designed to be a tutorial; it's extracted from production systems and optimized; there is no pedagogical mission. [FsUno.Prod](https://github.com/thinkbeforecoding/FsUno.Prod) on the other hand has this specific intention, walking though that is highly recommended. Also [EventStore](https://eventstore.org/), being a widely implemented and well-respected open source system has some excellent learning materials and documentation with a wide usage community (search for `DDD-CQRS-ES` Discord).

Having said that, we'd love to see a set of tutorials written by people looking from different angles, and over time will likely do one too ... there's no reason why the answer to this question can't become "**of course!**"

### Can I use it for really big projects?

You can. Folks in Jet do; we also have systems where we have no plans to use it, or anything like it. That's OK; there are systems where having precise control over one's data access is critical. And (shush, don't tell anyone!) some find writing this sort of infrastructure to be a very fun design challenge that beats doing domain modelling any day...

### Can I use it for really small projects and tiny microservices?

You can. Folks in Jet do; but we also have systems where we have no plans to use it, or anything like it as it would be overkill even for people familiar with Equinox.

### OK, but _should_ I use Equinox for a small project ?

You'll learn a lot from building your own equivalent wrapping layer. Given the array of concerns Equinox is trying to address, there's no doubt that a simpler solution is always possible if you constrain the requirements to specifics of your context with regard to a) scale b) complexity of domain c) degree to which you use or are likely to use >1 data store. You can and should feel free to grab slabs of Equinox's implementation and whack it into an `Infrastructure.fs` in your project too (note you should adhere to the rules of the [Apache 2 license](LICENSE)). If you find there's a particular piece you'd really like isolated or callable as a component and it's causing you pain as [you're using it over and over in ~ >= 3 projects](https://en.wikipedia.org/wiki/Rule_of_three_(computer_programming)), please raise an Issue though ! 

Having said that, getting good logging, some integration tests and getting lots of off-by-one errors off your plate is nice; the point of [DDD-CQRS-ES](https://github.com/ddd-cqrs-es/community) is to get beyond toy examples to the good stuff - Domain Modelling on your actual domain.

### What client languages are supported ?

The main language in mind for consumption is of course F# - many would say that F# and event sourcing are a dream pairing; little direct effort has been expended polishing it to be comfortable to consume from other .NET languages, the `dotnet new eqxwebcs` template represents the current state. In Equinox V4, the `DeciderCore` interface offers an interface that uses C#-friendly `Task` and `Func` types (compared to `Decider`, which uses `async` and curried function signatures to provide an idiomatic F# experience, which is possible, but cumbersome to use from C#) 

## You say I can use volatile memory for integration tests, could this also be used for learning how to get started building event sourcing programs with equinox? 

The `MemoryStore` is intended to implement the complete semantics of a durable store (aside from caching). The main benefit of using it is that any tests using it have zero environment dependencies. In some cases this can be very useful for demo apps or generators (rather than assuming a specific store at a specific endpoint and/or credentials, there is something to point at which does not require configuration or assumptions.). The single problem of course is that it's all in-process; the minute you stop the host, the items on your list will of course disappear. In general, EventStore is also an attractive option for prototyping; the open source edition is trivial to install and has a Web UI that lets you navigate events being produced etc.

### OK, so it supports CosmosDB, DynamoDB, EventStoreDB, MessageDB and SqlStreamStore and might even support more in the future. I really don't intend to shift datastores. Period. Why would I take on this complexity only to get the lowest common denominator ?

Yes, you have decisions to make; Equinox is not a panacea - there is no one size fits all. While the philosophy of Equinox is a) provide an opinionated store-neutral [Programming Model](DOCUMENTATION.md#Programming-Model) with a good pull toward a big [pit of success](https://blog.codinghorror.com/falling-into-the-pit-of-success/), while not closing the door to using store-specific features where relevant, having a dedicated interaction is always going to afford you more power and control.

### Is there a guide to building the simplest possible hello world "counter" sample, that simply counts with an add and a subtract event? 

Yes; [`Counter.fsx` in th Tutorial project in this repo](https://github.com/jet/equinox/blob/master/samples/Tutorial/Counter.fsx). It may also be worth starting with the [API Guide in DOCUMENTATION.md](DOCUMENTATION.md#api). An alternate way is to look at the `Todo.fs` files emitted by [`dotnet new equinoxweb`](https://github.com/jet/dotnet-templates) in the [QuickStart](#quickstart).

<a name="why-snapshots-in-stream"></a>
### Why do the snapshots go in the same stream in `Equinox.EventStore` and `Equinox.SqlStreamStore` ? :pray: [@chrisjhoare](https://github.com/chrisjhoare)

I've been looking through the snapshotting code recently. Can see the snapshot events go in the same stream as regular events. Presume this is to save on operations to read/write the streams? and a bit less overhead maintaining two serializers? Are there any other advantages? I quite like it this way but think i saw the [geteventstore](https://eventstore.org) advice was separate streams so just interested in any other reasoning behind it

The reason GES recommends against is that the entire db is built on writing stuff once in an append only manner (which is a great design from most aspects). This means your choices are:
  - embed snapshots in the same stream, but do that sparingly, as you can't delete them (implemented in Equinox as `AccessStrategy.RollingSnapshots`)
  - keep snapshots elsewhere (typically in a sister stream with the max items set to 1
    - which the EventStoreDB background scavenging process will tidy up (but it ain't free)
    - which is a separate roundtrip (which is not the end of the world in GES but is still another thing to go wrong)
    - which can't be written as a transaction, i.e. you'd need to write the snapshot after (and only after) a successful write (and worry about inconsistency)
That general advice/trade-offs on snapshotting applies to most systems.

The answer as to why that strategy is available in in `Equinox.EventStore` is for based on use cases (the second strategy was actually implemented in a bespoke manner initially by [@eiriktsarpalis](https://github.com/eiriktsarpalis):
- streams like `Favorites` where every event is small (add sku, drop sku), and the snapshot is pretty compact (list of skus) (but note it is ever growing)
- streams like `SavedForLater` items where the state rolls over regularly - even after 5 years and 1000s of items moving in and out, there's a constraint of max 50 items which makes a snapshot pretty light. (The other trick is that a `Cleared` event counts as a valid starting state for the fold - and we don't write a snapshot if we have one of those)

The big win is latency in querying contexts - given that access strategy, you're guaranteed to be able to produce the full state of the aggregate with a single roundtrip (if max batch size is 200, the snapshots are written every 200 items so reading backward 200 guarantees a snapshot will be included)

The secondary benefit is of course that you have an absolute guarantee there will always be a snapshot, and if a given write succeeds, there will definitely be a snapshot in the `maxBatchSize` window (but it still copes if there isn't - i.e. you can add snapshotting after the fact)

`Equinox.SqlStreamStore` implements this scheme too - it's easier to do things like e.g. replace the bodies of snapshot events with `nulls` as a maintenance task in that instance

Initially, `Equinox.CosmosStore` implemented the same strategy as the `Equinox.EventStore` (it started as a cut and paste of the it). However the present implementation takes advantage of the fact that in a Document Store, you can ... update documents - thus, snapshots (termed unfolds) are saved in a custom field (it's an array) in the Tip document - every update includes an updated snapshot (which is zipped to save read and write costs) that overwrites the unfolds entirely. You're currently always guaranteed that the snapshots are in sync with the latest event by virtue of how the stored proc writes. The DynamoDB impl follows the same strategy.

I expand (too much!) on some more of the considerations in https://github.com/jet/equinox/blob/master/DOCUMENTATION.md

The other thing that should be pointed out is the caching can typically cover a lot of perf stuff as long as stream lengths stay sane - Snapshotting (esp polluting the stream with snapshot events should definitely be toward the bottom of your list of tactics for managing a stream efficiently given long streams are typically a design smell)

NOTE The newer `Equinox.MessageDb` store binding implements snapshotting as separated events in a separate category.

<a name="changing-access-strategy"></a>
### Changing Access / Representation strategies in `Equinox.CosmosStore` - what happens?

> Does Equinox adapt the stream if we start writing with `Equinox.CosmosStore.AccessStrategy.RollingState` and change to `Snapshotted` for instance? It could take the last RollingState writing and make the first snapshot ?

> what about the opposite? It deletes all events and start writing `RollingState` ?

TL;DR yes and no respectively

#### Some context

Firstly, it's recommended to read the [documentation section on Access Strategies](DOCUMENTATION.md#access-strategies)

General rules:
- Events are the atoms from which state is built, they live forever in immutable Batch documents.
- There is a special Batch with `id = "-1"`, entitled the *Tip*.
- Snapshots/unfolds live in the `.u` array in the Tip doc.
 loading/build of state is composed of
- regardless of what happens, Events are _never_ destroyed, updated or touched in any way, ever. Having said that, if your Event DU does not match them, they're also as good as not there from the point of view of how State is established.
- Reads always get the `Tip` first (one exception: `Unoptimized` mode skips reading the `Tip` as, by definition, you're not using snapshots/unfolds/any tricks), Writes always touch the `Tip` (yes, even in `Unoptimized` mode; there's no such thing as a stream that has ever been written to that does not have a `Tip`).
- In the current implementation, the calling code in the server figures out everything that's going to go in the ~~snapshots~~ unfolds list if this sync is successful.
 
 The high level skeleton of the loading in a given access strategy is: 
   a) load and decode unfolds from tip (followed by events, if and only if necessary)
   b) offer the events to an `isOrigin` function to allow us to stop when we've got a start point (a Reset Event, a relevant snapshot, or, failing that, the start of the stream)

It may be helpful to look at [how an `AccessStrategy` is mapped to `isOrigin`, `toSnapshot` and `transmute` lambdas internally](https://github.com/jet/equinox/blob/master/src/Equinox.CosmosStore/CosmosStore.fs#L1295)

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

Ouch, not looking forward to reading all that logic :frown: ? [Have a read, it's really not that :scream:](https://github.com/jet/equinox/blob/master/src/Equinox.CosmosStore/CosmosStore.fs#1109).

<a name="how-is-expectedVersion-managed"/></a>
### Help me understand how the `expectedVersion` is used with EventStoreDB - it seems very confusing :pray: [@dharmaturtle](https://github.com/dharmaturtle) 

> I'm having some trouble understanding how Equinox+ESDB handles "expected version". Most of the examples use `Equinox.Decider.Transact` which is storage agnostic and doesn't offer any obvious concurrency checking. In `Equinox.EventStore.Context`, there's a `Sync` that takes a `Token` which holds a `streamVersion`. Should I be be using that instead of `Transact`?

The bulk of the implementation is in [`Equinox/Stream.fs`](https://github.com/jet/equinox/blob/master/src/Equinox/Stream.fs#L32), see the `let run` function.

There are [sequence diagrams in Documentation MD](https://github.com/jet/equinox/blob/master/DOCUMENTATION.md#code-diagrams-for-equinoxeventstore--equinoxsqlstreamstore) but I'll summarize here:

- As you suggest, `Transact` is definitely the API you want to be be using
- The assumption in Equinox is that you _always_ want to do a version check - if you don't, you can't process idempotently, why incur the cost of an ordered append only store? (there is a lower `Sync` operation which does a blind write to the store in `Equinox.CosmosStore` which allows you to do a non-version-checked write in that context (its implemented and exposed as the stored procedure needs to handle the concept). For EventStoreDB, if you have such a special case, you can use its APIs directly)
- The inner API with the `Sync` is the 'store interface' which represents the actual processing needed to do a version-checked write (The `Sync` one does not handle retries and is only used for the last attempt, when there are no subsequent retries)
- The main reason for the separation is that no ephemeral state is held by Equinox in anything like e.g. Unit Of Work during the course of a `decide` function being invoked - the `(token,state)` tuple represents all the things known at the point of loading, and the `Sync` can use anything it stashed in there when it has proposed events passed to it, as the contract involves the caller resupplying that context.
- Another consideration is that its easy to introduce off by one errors when there's an expectedVersion in play, so encapsulating this is no bad thing (in addition to it being something that you don't want to be passing around in your domain logic)

But why, you might ask? the API is designed such that the token can store any kind of state relevant to the `Sync` operation.

a. for SqlStreamStore and EventStore, when writing rolling snapshots, we need to retain the index of the last Rolling Snapshot that was written, if we encountered it during loading (e.g. if we read V198-100 and there was a snapshot at at V101, then we need to write a new one iff the events we are writing would make event 101 be > batchSize events away, i.e. we need to always include a RollingSnapshot to maintain the "if you read the last page, it will include a rolling snapshot" guarantee)

b. for CosmosDB, the `expectedVersion` can actually be an `expectedEtag` - this is how `AccessStrategy.RollingState` works - this allows one to update Unfolds without having to add an event every time just to trigger a change in the version

(The second usage did not necessitate an interface change - i.e. the Token mechanism was introduced to handle the first case, and just happened to fit the second case)

> Alternatively, I'm seeing in `proReactor` that there's a `decide` that does version checking. Is this recommended? [code](https://github.com/jet/dotnet-templates/blob/3329510601450ab77bcc40df7a407c5f0e3c8464/propulsion-reactor/TodoSummary.fs#L30-L52) 

If you need to know the version in your actual handler, QueryEx and other such APIs alongside Transact expose it (e.g. if you want to include a version to accompany a directly rendered piece of data). (Note that doing this - including a version in a rendering of something should not be a goto strategy - i.e. having APIs that pass around expectedVersion is not a good idea in general)

The typical case for using the version in the output is to be able to publish a versioned summary on a feed, so someone else can build a version-checking idempotent Ingester..... Which brings us to:

For that particular reactor, a different thing is going on though: the input value is versioned, and we don't write if the value is in date e.g. if you reset the checkpoint on the projector, it can re-run all the work idempotently:

a. version 3 of something is never temporarily overwritten with V2 and then V3

b. no redundant writes take place (and no expensive RU costs are incurred in Cosmos)

### What kind of values does `ISyncContext.Version` return; i.e. what numeric value is yielded for an empty stream? :pray: [@ragiano215](https://github.com/ragiano215)

Independent of the backing store being used, Equinox uses `0`-based versioning, i.e. the version value is equal to the number of events in the stream. Each event's `Index` is `0`-based, akin to how a .NET array is numbered:

* **`1` when the first event is written**
* _`0` when there's no events_

> Side note: for contrast, EventStoreDB employs a different (`-1`-based) scheme in order to have `-1`/`-2` etc represent various `expectedVersion` conditions 
>
> * **`0` when the first event is written to the stream**
> * _`-1` when the stream exists and is empty but has metadata defined_
> * _`-2` when the stream doesn't exist_

Note that for `Equinox.CosmosStore` with a
[pruner](https://github.com/jet/dotnet-templates/tree/master/propulsion-pruner)-[archiver](https://github.com/jet/dotnet-templates/tree/master/propulsion-archiver)
pair configured, the primary store may have been stripped of events due to the operation of the pruner.
In this case, it will however retain the version of the stream in the tip document, and if that's non-`0`,
will attempt to load the archived events from the Archive store.

### What is Equinox's behavior if one does a `Query` on a 'non-existent' stream? :pray: [@ragiano215](https://github.com/ragiano215)

Example: I have an app serving a GET endpoint for a customer order, but the id supplied within the URL is for an order that hasn't yet been created.

Note firstly that Equinox treats a non-existent stream as an empty stream. For the use case stated, it's first
recommended that the state is defined to represent this non-existent / uninitialized phase, e.g.: defining a DU with a
variant `Initial`, or in some way following the [Null Object Pattern](https://en.wikipedia.org/wiki/Null_object_pattern).
This value would thus be used as the `Fold.initial` for the Category. The app will use a `.Query`/`.QueryEx` on the relevant Decider,
and Equinox will supply the _initial_ value for the `project` function to render from (as a pattern match).

> Side note: the original question is for a read operation, but there's an interesting consideration if we are doing a `Transact`. Say,
> for instance, that there's a PUT API endpoint where the code would register a fresh customer order for the customer in its order list
> via the Decider's `Transact` operation. As an optimization, one can utilize the `AssumeEmpty` hint as the `Equinox.LoadOption` to
> hint that it's worth operating on the assumption that the stream is empty. When the internal sync operation attempts to perform the write,
> that assumption will be tested; every write is always version checked.
> In the scenario where we are dealing with a rerun of an attempt to create an order (lets say the call timed out, but the processing actually
> concluded successfully on another node of the API server cluster just prior to the caller giving up), the version check will determine that
> the expected version is _not_ `0` (as expected when a stream is _Empty_), but instead `1` (as the preceding invocation wrote one event).
> In this case, the loop will then use the `fold` function from the `initial` state, folding in the single event (via the `evolve` function),
> passing that state to the `decider` function, which, assuming it's implemented in an [_idempotent_](https://en.wikipedia.org/wiki/Idempotence)
> manner, will indicate that there are no events to be written.

<a name="what-is-a-decider"/></a>
### What is a Decider? How does the Equinox `type Decider` relate to Jérémie's concept of one? :pray: [@rmaziarka](https://github.com/rmaziarka)

The best treatments of the concept of a Decider are:
1. [Jérémie's intro post](https://thinkbeforecoding.com/post/2021/12/17/functional-event-sourcing-decider) - it's not short, but its required reading for anyone considering event sourcing, regardless of whether you're even going to use a functional programmign language to do so.
2. There's a very thorough treatment with code walk-through and discussion in [this 2h45m video](https://www.youtube.com/watch?v=kgYGMVDHQHs) on [Event Driven Information Systems](https://www.youtube.com/channel/UCSoUh4ikepF3LkMchruSSaQ) with [Jérémie Chassaing, @thinkb4coding](https://twitter.com/thinkb4coding)

As teased in both, there will hopefully eventually (but hopefully not [inevitably](https://github.com/ylorph/The-Inevitable-Event-Centric-Book)) be a book at some point too :fingers_crossed: 

#### In Equinox

The Equinox `type Decider` exposes an [API that covers the needs of making Consistent Decisions against a State derived from Events on a Stream](
https://github.com/jet/equinox/blob/master/src/Equinox/Decider.fs#L11-L96). At a high level, we have:
- `Transact*` functions - these run a decision function that may result in a change to the State, including management of the retry cycle when a consistency violation occurs during the syncing of the state with the backing store (See [Optmimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control)). Some variants can also yield an outcome to the caller after the syncing to the store has taken place.
- `Query*` functions - these run a render function projecting from the State that the Decider manages (but can't mutate it or trigger changes). The concept of [CQRS](https://martinfowler.com/bliki/CQRS.html) is a consideration here - using the Decider to read state should not be a default approach (but equally should not be considered off limits).

> NOTE the `Decider` itself in Equinox does not directly touch all three of the ingredients - while you pass it a `decide` function, the `initial` and `fold` functions, are  supplied to the specific store library (e.g. `Equinox.CosmosStore.CosmosStoreCategory`), as that manages the loading, snapshotting, syncing and caching of the state and events.

#### In general

While the concept of a Decider plays well with Event Sourcing and many different types of Stores, it's important to note that neither storage or event sourcing is a prerequisite. A lot of the value of the concept is that you can and should be able to talk about and implement one without reference to any specific store implementation (or even thinking about it ever being stored - it can also be used to manage in-memory structures such as UI trees etc). By the same token, you can decorate/proxy a Decider with loading or saving behavior (not limited to just 'copying the commands'), e.g. you might be syncing saves of changes to a backend in near-real time while the front end is reflecting changes instantaneously.

#### Consistency

_In any system, any decision (or even query) processed by a Decider should be concurrency controlled_.

> NOTE: the situation might be different if working in an environment where a particular concurrency model is emphasized. E.g.: if you're running in an Actor based system, one may map a decider to an actor. With this, any impetus to change state would be forwarded to that one actor and in a serial fashion. Potentials for conflicts would be managed by a supervisor.
>
> Another example where the situation could be different is if you're building an in-memory decision system to support a game etc as Jérémie does in the talk. There's only one so that concern is side-stepped.

_When applying the concept of a Decider to event sourcing, the consistency requirement means [there's more to the exercise than emitting events into a thing those marketing centers on Events](https://domaincentric.net/blog/eventstoredb-vs-kafka)_. There needs to be a way in the overall processing of a decision that manages a concurrency conflict by taking the state that superseded the one you based the original decision on (the _origin state_), and re-running the decision based on the reality of that conflicting _actual state_. The resync operation that needs to take place in that instance can be managed by reloading from events, reloading from a snapshot, or by taking events since your local state and `fold`ing those Events on top of that.

#### The ingredients

With Deciders in general, and Equinox in particular, the following elements are involved:
- a `State` type, on which decisions can be based. This can be updated as a consequence of a decision, e.g. the item identifiers in a cart and the associated quantities
- a `decide` function, which is presented a `State`, and returns a decision, which we use to update the State, if, and only if there is no concurrency conflict when applying them, e.g. the `decider` might validate that it's acceptable to start a process at the present time, returning the identifier of the process; if there is already one in flight, it can return the identifier of that already-started process (covered under the term idempotency further onwards)
- an `initial` State, from which we start if there's nothing in the store, e.g. an empty list of product codes representing nothing in the cart
- an `Event` type (think a Discriminated union). This might have cases like `Cleared`, `QuantityChanged`, `ItemAdded` (in some cases only a snapshot of the state is persisted, but in Equinox, changes are always represented in terms of the relevant Event type for a given Decider)
- the Events are logically represented as an ordered list of events (could be an in memory array, a stream in ESDB, documents with a Tip as with `Equinox.CosmosStore` etc). In some cases e.g. `RollingState` mode, they may get folded and snapshotted when stored)
- the State is established by [`fold`](https://en.wikipedia.org/wiki/Fold_(higher-order_function))ing the sequence of `Event`s, starting from an `initial` state. This value [should not be mutated by the `fold` function](https://en.wikipedia.org/wiki/Persistent_data_structure).

With the Equinox `type Decider`, the typical `decide` signature used with the `Transact` API has the following signature:

      context -> inputsAndOrCommand -> 'State -> Event list

> NOTE: There are more advanced forms that allow the `decide` function to be `Async`, inspect the State's `Version` and/or to also return a `'result`, which will be yielded to the caller driving the Decision as the return value of the `Transact` function.

#### So what kind of a thing is a Decider then?

Is it a marketing term? Jérémie's way of explaining an app?

I'd present the fact that Equinox:
- was initially generalised and extracted from working code using ESDB in (by most measures) a successful startup written in a mix of F# and C# by the usual mix of devs, experience levels, awareness of event sourcing patterns and language/domain backgrounds
- for a long time only had its MemoryStore as the thing to force it to be store agnostic
- did not fundamentally change to add idiomatic support for a Document database (CosmosDB)
- did not change to add idiomatic support for DynamoDB
- can and has been used at every stage in an e-commerce pipeline
- is presently aligning pretty neatly with diverse domains without any changes/extensions, both for me and others

... as evidence for Decider being *a pattern language* (analogous to how event sourcing and many modern event sourced UI paradigms share a lot of common patterns).

#### ... about the process of making _decisions_

Finally, I'd say that a key thing the Decider concept brings is a richer way of looking at event sourcing than the typical event sourcing 101 examples you might see:
- de-emphasizing one way calls into the void that map commands to events without deduplicating and/or yielding a result (not saying that you shouldn't do the simplest thing -- you absolutely should)
- de-emphasizing the notion that all projection handlers don't get more exciting than sitting there looking for `MyThingCreated` and doing an `INSERT` with a try/catch for duplicate inserts as a way to make it idempotent (and that every stream design must have a Created event because that's the magic recipe)

The missing part beyond that basic anemic stuff is where the value lies:
- any interesting system *makes _decisions_*:
- a decision can yield a result alongside the events that are needed to manifest the state change
- any decision process can and should consider [idempotency](https://en.wikipedia.org/wiki/Idempotence) - if you initiate a process/request something, a retry can't be a special case you don't worry about. Taking correct handling of such retry and/or replay scenarios into consideration should not be an afterthought, but instead be a concern on your day to day checklist when writing a decision function. Of course idempotency can be handled in many ways
  - sometimes before processing gets to the Decider - i.e. any outer layer of the processing can have semantics that cover the idempotency requirement
  - sometimes within the Decider itself (e.g. a decision can yield the unique id generated the first time the request was triggered on every subsequent invocation)
  - sometimes it can be handled externally (e.g. one might not maintain the state that would be necessary to fully deduplicate triggerings and rely on the EventStoreDB and SqlStreamStore idempotent write deduplication mechanism to elide the redundant the writes just in time)
  - A Decider can also be decorated/proxied to add idempotency. As [Jérémie](https://github.com/thinkbeforecoding) [mentioned here](https://github.com/jet/equinox/pull/299#discussion_r748744034), and in his talk, you can also naturally _layer idempotency on a decider. You can make a generic function
`D<Cmd,Event,State> -> D<(IdCmd), Event, (State(Id Set))>` where `Id` is a command identifier. In the `decide` function it checks whether the id is in the set. In the `evolve` function, it adds the id to the set._
- it can let you drive a set of reactions in a fault-tolerant and scalable (both perf/system size, and management/separation of complexity)  manner
- a Decider should generally be maintaining one or more invariants associated with the underlying state it represents; if there isn't some element of your system doing that, you might as well be dumping stuff in a log or mutating a CRUD model.

Quite frequently, a Decider may internally operate as a Process Manager, encapsulating a [State Machine](https://en.wikipedia.org/wiki/Finite-state_machine). That is to say, there will be a subset of the Deciders in a system that are providing APIs that support some overall protocol that enforces some lifecycle rules.

### With `Equinox.CosmosStore`, it seems it should be possible to handle saving multiple events from multiple streams as an atomic transaction, as long as they share the same partition key in Cosmos DB. However there doesn't seem to be any way to do that with APIs such as `Equinox.Decider.Transact`? :pray: [@rmaziarka](https://github.com/rmaziarka)

> I'm asking because I had this idea which I was workshopping with a friend, that it could solve typical sync problems in typical availability domains.

> Let's assume that our domain is Bike Sharing in different cities. Users can reserve a bike, and then access it and ride.

> In our system we would have two subdomains:
> * Inventory - responsible for being sure that is bike (resource) is ready to be used
> * Orders - responsible for tracking information regarding reservations, orders etc

> There could be different subdomains as Orders, that would be using Inventory under the hood - like Repairing.

> In the system we would like to a) block a particular bike for the user and b) at the same time create a reservation for them to store all important business information.

> So we use and save data to the 2 different streams of data - typical ES problem.

> We could use event handers / sagas but it brings another level of complexity.

> In the case above we could assume that data inside a single city will be so small, that even with prolonged usage it won't fill the whole CosmosDB partition. So we could use it to handle saving 2 events in the same time. (My actual system is not Bike Sharing, and I'm confident of a lack of explosive growth!)

> Saving a `BikeBlocked` event would be handled in a transaction along with the `ReservationMade`. So we wouldn't end up with the situation that bike is blocked and reservation is not made or conversely.

> What do you think of this idea? Does it sound reasonable?

#### Why not keep it simple and have it one logical partition: a high level perspective

I'd actually attack this problem from an event modeling perspective first (Event Storming and other such things are reasonable too, but I personally found the rampup on EM to be reasonable, and it definitely forces you to traverse the right concerns. [Good intro article re Event Modeling](https://eventmodeling.org/posts/what-is-event-modeling).

Once you cover enough scenarios of any non-CRUD system, I'd be surprised if you truly end up with a model with just 2 logical streams that you want to combine into 1 for simplicity of event storage because you are covering all the cases and can reason about the model cleanly.

When you have a reasonable handle from a few passes over that (watch our for analysis paralysis, but also don't assume you can keep it simple via :see_no_evil::hear_no_evil: and not talking to enough people who understand the whole needs of the system, aka :speak_no_evil:)

For any set of decisions you control in a single _Decider_ you need to:
- be able to establish all relevant state to back the decision making efficiently (point reads of single documents, etag-backed caching, small streams, not having to filter out things you don't need)
- be able to write it with a concurrency check (all writers to all stuff under control of the decider are all contenting for the write capacity, i.e provisioned RUs)
- be able to see the changes on a feed in a useful way (in order of writes, with related stuff easily accessible and no stuff you don't care about)
- be able to write it efficiently - if you can't absorb the maximum writes needed, you need to find a way to split it out to multiple logical streams (and hence Deciders)
- have a set of event contracts, folding logic and decision making logic that a person can read, reason about and test
- have ways to manage evolution over time of the rules, the event schema and the inevitable need to handle scenarios you didnt envisage, or are genuinely new
- minimize speculative just in case, well intentioned and/or future-proofing complexity - there'll be plenty challenging complexity fun without you adding to the mix
- you don't want to have to think about anything outside a given Decider when drawing stuff on a whiteboard, looking at a dashboard, looking at metrics, writing a test unless it makes things fundamentally easier

That's a lot of things.

**Before we go on, consider this: you want to minimise how much stuff a single Decider does. Adding stuff into a Decider does not add complexity linearly. There is no technical low level silver bullet solution to this problem.**

Right, strap yourself in; there's no TL;DR for this one ;)

#### Preamble

First, I'd refer to some good resources in this space, which describe key functions of an Event Store

- Yves Lorphelin's [Expectations for an Event Store](https://github.com/ylorph/RandomThoughts/blob/master/2019.08.09_expectations_for_an_event_store.md)
- The [Evolved Version of that, with a slightly different focus](https://www.eventstore.com/blog/requirements-for-the-storage-of-events)

Next, I'd like to call out some things that Equinox is focused on delivering, regardless of the backing store:

  - concurrency-controlled updates
    - to a single consistency control unit (stream) at a time (underlying stores in general rarely provide support for more than that, but more importantly, a huge number of use cases in a huge number of systems have natural mappings to this without anyone having to do evil things or write thousands of lines of code)
    - no major focus on blind-writes, even if there is low level support and/or things work if you go direct and do it out of band)
    - provide a good story for managing the writing of the first event in a stream in an efficient manner
  - have a story for providing a Change Feed
    - typically via a matching Propulsion library (fully ordered for SSS and ESDB, ordered at stream level for CosmosDB and DynamoDB)
  - have a story for caching and efficient usage of each store to the best degree possible
    - `Equinox.SqlStreamStore`
      - caching is supported and recommended to minimise reads
      - in-stream snapshots sometimes help but there are tradeoffs
    - `Equinox.EventStore`
      - caching is supported, but less important/relevant than it is with SSS as ESDB has good caching support and options
      - Equinox in-stream snapshots sometimes help but there are tradeoffs)
    - `Equinox.CosmosStore` and `Equinox.DynamoStore`
        - multiple events are packed into each document (critical to avoid per-Item space and indexing overhead - limits are configurable)
        - etag-checked RollingState access mode enables allow you to achieve optimal perf and RU cost via the same API without writing an event every time
    - `Equinox.CosmosStore`
      - etag-checked read caching (use without that is not recommended in general, though you absolutely will and should turn it off for some streams)

**The provision of the changefeed needs to be considered as a primary factor in the overall design if you're trying to build a general store - the nature of what you are seeking to provide (max latency and ordering guarantees etc) will be a major factor in designing the schema for how you manage the encoding and updating of the items in the store**

#### Sagas?

>In the system we would like to block particular bike for the user. But at the same time create a reservation for them to store all important business information. So we use and save data to the 2 different streams of data - typical ES problem. We could use event handers / sagas but it brings another level of complexity. 

There will always be complexity in any interesting system that should not just be a CRUD layer over a relational DB.
Taking particular solution patterns off the table from the off is definitely something you need to be careful to avoid.
As an analogy: Having lots of classes in a system can make a mess. But collapsing it all into as few as possible can result in ISP and SRP violations and actually make for a hard to navigate and grok system, despite there being less files and less lines of code (aka complexity). Coupling things can sometimes keep things Simple, but can also sometimes simply couple things.

In my personal experience
1) [Sagas, PMs and related patterns and techniques](https://event-driven.io/en/saga_process_manager_distributed_transactions/) can be scary, and there are not many good examples out there in an event sourcing context
2) You can build a significant number of systems without ever intentionally applying any of those patterns

But, also IME:
3) They're pretty fundamental
4) They are not as hard as you think when you've done two of them
5) Proper stores enable good ways to manage them
6) They enable you to keep the overall complexity of a system down by decoupling things one might artificially couple if you're working with a toolbox where you've denied yourself a space for PMs and Sagas

In other words, my YAGNI detector was on high alert for it, as it seems yours is :wink:

#### Transactional writes?

>In the case above we could assume that data inside a single city will be so small, that even with the long usage it won't complete the whole CosmosDB partition. So we could use it to handle saving 2 events in the same time.

For avoidance of doubt:
- being able to write two events to a single stream as an atomic action is a perfectly normal thing to do (Equinox itself, and any credible Event Store supports it).
- being able to write to two _streams_ atomically is not a commonly supported operation for Event Stores.

You're correct to identify the maximum amount of data being managed in a scope as a critical consideration when coupling stuff within a logical partition in order to be able to achieve consistency when managing updates across two set of related pieces of information.

Specifically wrt CosmosDB, the following spring to mind without looking anything up:
- The max amount of data in any logical partition is 20GB. I would not be shocked if it was 50GB some time soon. But it's not going to be a TB any time soon. When that's gone, you need to delete things
- All updates in a logical partition are hitting the exact same thing on the same node of a physical partition
- All reads are also hitting that
- The maximum amount of RUs you can give that physical partition is 5000 RU (or is it 10000 RU?)
- the more data you have in a single logical partition, the more pages in the indexes, the higher the RU cost for equivalent things (the amount in other logical partitions does not have this effect)
- [hotspots at either logical partition or physical partition levels](DOCUMENTATION.md#growth-handling-strategies)
- if you do an event per document, you are hosed
- if you don't have an e-tag checked write or read scheme, you won't be able to load state efficiently without resorting to queries

In other words, it's looking like you're painted into a corner: you can't shard, can't scale and are asking for hotspotted partition issues. Correct, that doesn't always matter. But even if you have 10 cities, you can't afford for the two busiest ones to be hosted on the same node as that's going to be the one suffering rate limiting. Trust me, it's not even worth thinking about tricks to manage this fundamental problem (trying to influence the sharding etc is going to be real complexity you do not want to be explaining to other engineers on a whiteboard or a 5am conf call)

#### So why do all these things spring to mind ?

- This desire comes up all the time - I've had this conversation with tens of engineers with various years of programming, years as users of document databases, years of writing event sourced systems on this topic. I don't believe many have walked away still believing there's an easy way around this either.
- I have done lots of benchmarking (measuring latency, RUs, handling of concurrency conflicts) of pretty much every possible approach on Cosmos DB
- I've done lots of CosmosDB specific reading on this - MS has actually got pretty good docs (most useful ones I'm aware of are linked from [DOCUMENTATION.md](DOCUMENTATION.md))
- I've read and watched lots of stuff on various DBs. Most of that can be replaced with https://www.amazon.com/Designing-Data-Intensive-Applications-Reliable-Maintainable/dp/1449373321 and some in-date equivalent of https://martinfowler.com/books/nosql.html
- Cosmos DB, when used with Mechanical Sympathy (read: Billing Model Sympathy) at the forefront of one's mind is absolutely a good product that can be extremely cost effective and provide excellent performance
_However_, you are pretty much guaranteed to not get that good :point_up: experience without reading, measuring and iterating (I've seen many people with much bigger brains than me prove that over and over - the bad things you hear about Cosmos DB are true too, and are not only caused by the below average drivers)

#### TL;DR on Cosmos DB for storing events (but really for all usages)

- You can't update or the changefeed gets out of order (but there are lots of other reasons not to do updates beyond that)
- You need to do etag-checked reads and writes or go home
- Each Item/Document costs you about 500 bytes in headers and space in all the indexes so you need to strongly consider >1 event/document
- Queries are way more costly than point reads. It's called a Document database because the single thing it does best on this planet is read or update ONE document. Read/write cost rule of thumb is per KB, but has logarithmic properties too, i.e. 50K is not always 50x the cost of 1K
- Keep documents on the small side. (Max doc/item size for CosmosDB is 2MB, max stored proc arg size is 1MB, 1MB induces roundtrip latency and write contention etc). DynamoDB sets max doc size at 400KB for similar reasons. ESDB allows 4MB but that's less of a problem for it.
- Keep the combined sizes of items/documents in a logical partition as small as possible but no smaller. Cosmos DB has a hard limit of 20GB per logical partition, but in practice, the latency to read or walk that amount of content and/or documents will hit you long before that.
- Keep documents uniform in size where possible. Consider taking a write overhead hit to preserve read efficiency or scalability. (inc considering compression)
wrt 3+4+5, Equinox.Cosmos default max doc size is 30KB. On occasion where paying a write overhead is merited, I've used/recommended 500K and 1 MB for various reasons. That's the exception more than the rule.

#### Why think about it and explain it at such a low level?

Why think about this problem from such a low level perspective? Why not just stick to the high level given that's equally important to get right, and if correctly will more often yield a cleanly implementable solution?

Many people have a strong desire to write the least amount of code possible, and that's not unreasonable. The most critical question is going to be, does it work at all? Due to the combination of factors above, the answer is looking pretty clear. You can write the code and run it to be sure. I already have, in spike branches, and will save you the spoiler.

However, the fundamental things that arise when viewing it at a low/storage design level, also have high level issues in terms of modelling the software too, and different people will understand them better from different angles

I've witnessed people attempt to 'solve' the fundamental low level issues by working around reality, moving it all into a Cosmos DB Stored Procedure (Yes, you can guess the outcome). Please don't even think about that, no matter how much tech tricks you'll learn!

#### Conclusion

**You know what's coming next: You don't want to merge two Deciders and 'Just' bring it all under a nice tidy transaction to avoid thinking about Process Managers and/or other techniques.**

If you're still serious about making the time investment to write a PoC (or a real) implementation of a Store on a Document DB such as CosmosDB (and/or even writing a SQL-backed one without studying the prior art in that space intently), you can't afford _not_ to invest that time in [watching a 2h45m video about Deciders!](https://www.youtube.com/watch?v=kgYGMVDHQHs). 

### OK, but you didn't answer my question, you just talked about stuff you wanted to talk about!

😲Please raise a question-Issue, and we'll be delighted to either answer directly, or incorporate the question and answer here

# Acknowledgements

The diagrams in this README.md and the DOCUMENTATION.md would not and could not have happened without the hard work and assistance of at least:
- [@simonbrowndotje](https://github.com/simonbrowndotje) taking the time to define and evangelize the [C4 model](https://c4model.com/). It's highly recommended to view [the talk linked from `c4model.com`](https://www.youtube.com/watch?v=x2-rSnhpw0g&feature=emb_logo).
- the wonder that is [PlantUml](https://plantuml.com/); authoring them [in text](https://github.com/jet/equinox/tree/master/diagrams) with [the CVSCode PlantUML plugin](https://marketplace.visualstudio.com/items?itemName=jebbs.plantuml) is a delight. The inline images are also presently rendered by the [PlantUml proxy](https://plantuml.com/server)
- [@skleanthous](https://github.com/skleanthous) for making _and documenting_ [C4-PlantUmlSkin](https://github.com/skleanthous/C4-PlantumlSkin/blob/master/README.md)

# FURTHER READING

See [`DOCUMENTATION.md`](DOCUMENTATION.md) and [Propulsion's `DOCUMENTATION.md`](https://github.com/jet/propulsion/blob/master/DOCUMENTATION.md)
