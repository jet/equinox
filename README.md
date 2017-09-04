Foldunk
=======
Foldunk is a low-dependency set of infrastructure, examples and tests illustrating Event Sourced request processing in F#.

Features
--------
- a generalized command handler that can accomodates key Command Handling requirements by Jet's front-end API layer
- a scheme for the serializing Events modelled as an F# Discriminated Union with the following capabilities:
	- independence from any specific serializer
	- tagging Cases with versionable `eventType` tags
- an [EventStore](http://geteventstore.com) adapter
- an example scheme utilizing periodic in-stream 'snapshot' events with the following properties
	- no additional roundtrips at either read or write time to facilitate snapshotting
	- support for multiple co-existing snapshot schemas
- sufficient logging to be used in a Production context

Intended audience
-----------------
It's intended to be feasible to use it in the following contexts:
- workshops / internal Jet.com training bootcamps
- base infrastructure for talks / tutorials
- a testbed for illustrating command handling mechanisms