#I "bin/Debug/netstandard2.0/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Newtonsoft.Json.dll"
#r "TypeShape.dll"
#r "Equinox.dll"
#r "Equinox.Codec.dll"
//#r "FSharp.Control.AsyncSeq.dll"
//#r "Microsoft.Azure.DocumentDb.Core.dll"
#r "Microsoft.Azure.Cosmos.Client.dll"
#r "System.Net.Http"
#r "Serilog.Sinks.Seq.dll"
#r "Equinox.Cosmos.dll"

[<AutoOpen>]
module Types =

    type Id = string
    type FCInfo = { id : Id; code : string; name : string }

    type PhoneNumber = string
    type PhoneExtension = string
    type Email = string
    type ContactInformation = { name : string; phone : PhoneNumber; phoneExt : PhoneExtension; email : Email; title : string }

    type FCDetails = { dcCode : string; countryCode : string; financialGroupCode : string }

    type FCName = { code : string; name : string }
        
    type Address =
        {   address1    : string
            address2    : string
            city        : string
            state       : string
            zip         : string 
            isBusiness  : bool option
            isWeekendDeliveries  : bool option
            businessName  : string option }
    type Summary = { name : FCName option; address : Address option; contact : ContactInformation option; details : FCDetails option } with
        static member Zero = { name = None; address = None; contact = None; details = None }

module FulfilmentCenter =

    module Events =
        type Event =
            | FCCreated of FCName
            | FCAddressChanged of {| address : Address |}
            | FCContactChanged of {| contact : ContactInformation |}
            | FCDetailsChanged of {| details : FCDetails |}
            | FCRenamed of FCName
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Event>(Newtonsoft.Json.JsonSerializerSettings())

    let initial = Summary.Zero
    let evolve state : Event -> Summary = function 
        | FCCreated x | FCRenamed x -> { state with name = Some x }
        | FCAddressChanged x -> { state with address = Some x.address }
        | FCContactChanged x -> { state with contact = Some x.contact }
        | FCDetailsChanged x -> { state with details = Some x.details }
    let fold s xs = Seq.fold evolve s xs

    type Command =
        | Register of FCName
        | UpdateAddress of Address
        | UpdateContact of ContactInformation
        | UpdateDetails of FCDetails

    let interpret command state =
        match command with
        | Register c when state.name = Some c -> []
        | Register c -> [FCCreated c]
        | UpdateAddress c when state.address = Some c -> []
        | UpdateAddress c -> [FCAddressChanged {| address = c |}]
        | UpdateContact c when state.contact = Some c -> []
        | UpdateContact c -> [FCContactChanged {| contact = c |}]
        | UpdateDetails c when state.details = Some c -> []
        | UpdateDetails c -> [FCDetailsChanged {| details = c |}]

    type Service(log, resolveStream, ?maxAttempts) =
        let (|AggregateId|) id = Equinox.AggregateId("FulfilmentCenter", id)
        let (|Stream|) (AggregateId aggregateId) = Equinox.Stream(log, resolveStream aggregateId, defaultArg maxAttempts 3)

        let execute (Stream stream) command : Async<unit> = stream.Transact(interpret command)
        let read (Stream stream) : Async<Summary> = stream.Query id

        member __.UpdateName(id, value) = execute id (Register value)
        member __.UpdateAddress(id, value) = execute id (UpdateAddress value)
        member __.UpdateContact(id, value) = execute id (UpdateContact value)
        member __.UpdateDetails(id, value) = execute id (UpdateDetails value)
        member __.Read id: Async<Summary> = read id

open Equinox.Cosmos
open System

module Log =
    open Serilog
    open Serilog.Events
    let verbose = true // false will remove lots of noise
    let log =
        let c = LoggerConfiguration()
        let c = if verbose then c.MinimumLevel.Debug() else c
        let c = c.WriteTo.Sink(Store.Log.InternalMetrics.Stats.LogSink()) // to power Log.InternalMetrics.dump
        let c = c.WriteTo.Seq("http://localhost:5341") // https://getseq.net
        let c = c.WriteTo.Console(if verbose then LogEventLevel.Debug else LogEventLevel.Information)
        c.CreateLogger()
    let dumpMetrics () = Store.Log.InternalMetrics.dump log

module Store =
    let read key = Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get

    let connector = Connector(requestTimeout=TimeSpan.FromSeconds 5., maxRetryAttemptsOnThrottledRequests=2, maxRetryWaitTimeInSeconds=5, log=Log.log)
    let conn = connector.Connect("equinox-tutorial", Discovery.FromConnectionString (read "EQUINOX_COSMOS_CONNECTION")) |> Async.RunSynchronously
    let gateway = Gateway(conn, BatchingPolicy())
    let context = Context(gateway, read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER")
    let cache = Caching.Cache("equinox-tutorial", 20)
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching

open FulfilmentCenter

let resolve = Resolver(Store.context, Events.codec, fold, initial, Store.cacheStrategy).Resolve
let service = Service(Log.log, resolve)

let fc = "fc0"
service.UpdateName(fc, { code="FC000"; name="Head" }) |> Async.RunSynchronously
service.Read(fc) |> Async.RunSynchronously 

Log.dumpMetrics ()

module FulfilmentCenterSummary =

    module Events =
        type Event =
            | Updated of {| version: int64; state: Summary |}
            interface TypeShape.UnionContract.IUnionContract
        let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Event>(Newtonsoft.Json.JsonSerializerSettings())

    type State = {| version: int64; state: Types.Summary |}
    let initial = None
    let evolve _state = function
        | Updated v -> Some v
    let fold s xs = Seq.fold evolve s xs

    type Command =
        | Update of version : int64 * Types.Summary
    let interpret command (state : State option) =
        match command with
        | Update (uv,us) when state |> Option.exists (fun s -> s.version > uv) -> []
        | Update (uv,us) -> [Updated {| version = uv; state = us|}]

    type Service(log, resolveStream, ?maxAttempts) =
        let (|AggregateId|) id = Equinox.AggregateId("FulfilmentCenterSummary", id)
        let (|Stream|) (AggregateId aggregateId) = Equinox.Stream(log, resolveStream aggregateId, defaultArg maxAttempts 3)

        let execute (Stream stream) command : Async<unit> = stream.Transact(interpret command)
        let read (Stream stream) : Async<Summary option> = stream.Query(Option.map (fun s -> s.state))

        member __.Update(id, version, value) = execute id (Update (version,value))
        member __.TryRead id: Async<Summary option> = read id

//    let createFC conn code name = async {
//        let serialStream = Hosting.EventStore.serialIdStream "FCAssignment"
//        let! id = EventStore.serialId conn serialStream (fun i -> sprintf "FC%03d" i ) false
//        let fc = { FCName.code = code; name = name}
//        let eventData = EventCodec.encode Codecs.created fc |> EventData.withMetadata ["id",id]
//        let stream = fcstream id
//        return! EventStore.appendEventDataIgnore conn stream eventData |> Async.map(fun _ -> id)
//    }
