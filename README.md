Foldunk
=======
Foldunk is a low-dependency set of infrastructure, examples and tests illustrating a consistent approach to Event-sourced Decision processing against ordered stream stores in F#.

Intended audience
-----------------
The key consumer of this codebase is the Jet.com production site.

However,the codebase as a whole; particularly the, Samples are intended to achieve a critical secondary goal of sharing/discussing/prototyping usage patterns. As such, it's intended to be feasible to use it in the following contexts:
- a testbed for illustrating and prototyping decision/state processing mechanisms
- sample codebase for both internal and external facing Jet.com training events and talks
- base infrastructure for third party talks / tutorials

Please raise GitHub issues for any questions so others can benefit from the discussion.

Elements
--------
- Foldunk.Handler: a store agnostic Decision flow runner that fulfills key request processing objectives as dictated by the requirements of Jet.com's front-end API layer
- `Foldunk.Serialization`:
	- helpers for simplifying the writing of [json.net](https://github.com/JamesNK/Newtonsoft.Json) `JsonConverter`s
	- a set of [json.net](https://github.com/JamesNK/Newtonsoft.Json) Converters, mainly specific to F# types
- `Foldunk.EventStore`: Production-strength [EventStore](http://geteventstore.com) Adapter instrumented to the degree necessitated by Jet's production monitoring requirements
- sufficient logging and metrics instrumentation capabilties to be used in a Production context

Features
--------
- Does not emit any specific logs, but is sufficiently instrumented (using [Serilog](github.com/serilog/serilog) to allow one to adapt it to ones hosting context (we feed log info to  Splunk atm and feed metrics embedded in the LogEvent Properties to Prometheus; see relevant tests for examples)
- Uses `UnionEncoder`: a scheme for the serializing Events modelled as an F# Discriminated Union with the following capabilities:
	- independent of any specific serializer
	- allows tagging of Discriminated Union cases with low-dependency `DataMember(Name=` tags in a versionable manner using [TypeShape](https://github.com/eiriktsarpalis/TypeShape)'s [`UnionEncoder`](https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionEncoderTests.fs)
- _Compaction_: A pattern employed to optimize command processing by employing in-stream 'snapshot' events with the following properties:
	- no additional roundtrips to the store needed at either the Load or Sync points in the flow
	- support (via `UnionEncoder`) for the maintenance of multiple co-existing snapshot schemas in a given stream (A snapshot isa Event)