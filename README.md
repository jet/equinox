Foldunk
=======
Foldunk is a low-dependency set of infrastructure, examples and tests illustrating a consistent approach Event Sourced Decision processing against ordered stream stores in F#.

Intended audience
-----------------
The key consumer of this codebase is the Jet.com production site.

However,the codebase as a whole; particularly the, Samples are intended to achieve a critical secondary goal of sharing/discussing/prototyping usage patterns. As such, it's intended to be feasible to use it in the following contexts:
- a testbed for illustrating and prototyping decision/state processing mechanisms
- sample codebase for both internal and external facing Jet.com training events and talks
- base infrastructure for third party talks / tutorials

Please raise GitHub issues for any questions so others can benefit from the discussion.

Features
--------
- Foldunk.Handler: a store agnostic Decision flow runner that fulfills key request processing objectives as dictated by the requirements of Jet.com's front-end API layer
- Foldunk.EventSum: a scheme for the serializing Events modelled as an F# Discriminated Union with the following capabilities:
	- independent of any specific serializer
	- allowes tagging of Discriminated Union cases with `eventType` tags in a versionable manner
- Foldunk.EventStore: Production-strength [EventStore](http://geteventstore.com) Adapter instrumented to the degree necessitated by Jet's production monitoring requirements
- _Compaction_: A pattern employed to optimize command processing by employing in-stream 'snapshot' events with the following properties:
	- no additional roundtrips to the store at either the Load or Sync points in the flow to facilitate snapshotting
	- support (via `EventSum`) for the maintenance of multiple co-existing snapshot schemas in a given stream
- sufficient logging to be used in a Production context