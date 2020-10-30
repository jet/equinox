#I "bin/Debug/netstandard2.1/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Newtonsoft.Json.dll"
#r "TypeShape.dll"
#r "Equinox.dll"
#r "Equinox.Core.dll"
#r "FSharp.UMX.dll"
#r "FSCodec.dll"
#r "FsCodec.NewtonsoftJson.dll"
#r "Microsoft.Azure.Cosmos.Client.dll"
#r "Microsoft.Azure.Cosmos.Direct.dll"
#r "System.Net.Http"
#r "Serilog.Sinks.Seq.dll"
#r "Equinox.CosmosStore.dll"

open FSharp.UMX

[<AutoOpen>]
module Types =

    type [<Measure>] phoneNumber and PhoneNumber = string<phoneNumber>
    type [<Measure>] phoneExtension and PhoneExtension = string<phoneExtension>
    type [<Measure>] email and Email = string<email>
    type ContactInformation = { name : string; phone : PhoneNumber; phoneExt : PhoneExtension; email : Email; title : string }

    type FcDetails = { dcCode : string; countryCode : string; financialGroupCode : string }

    type FcName = { code : string; name : string }
        
    type Address =
        {   address1    : string
            address2    : string
            city        : string
            state       : string
            zip         : string 
            isBusiness  : bool option
            isWeekendDeliveries  : bool option
            businessName  : string option }
    type Summary = { name : FcName option; address : Address option; contact : ContactInformation option; details : FcDetails option }

module FulfilmentCenter =

    let streamName id = FsCodec.StreamName.create "FulfilmentCenter" id

    module Events =

        type AddressData = { address : Address }
        type ContactInformationData = { contact : ContactInformation }
        type FcData = { details : FcDetails }
        type Event =
            | FcCreated of FcName
            | FcAddressChanged of AddressData
            | FcContactChanged of ContactInformationData
            | FcDetailsChanged of FcData
            | FcRenamed of FcName
            interface TypeShape.UnionContract.IUnionContract
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    module Fold =

        type State = Summary
        let initial = { name = None; address = None; contact = None; details = None }
        let evolve state : Events.Event -> Summary = function
            | Events.FcCreated x | Events.FcRenamed x -> { state with name = Some x }
            | Events.FcAddressChanged x -> { state with address = Some x.address }
            | Events.FcContactChanged x -> { state with contact = Some x.contact }
            | Events.FcDetailsChanged x -> { state with details = Some x.details }
        let fold : State -> Events.Event seq -> State = Seq.fold evolve

    type Command =
        | Register of FcName
        | UpdateAddress of Address
        | UpdateContact of ContactInformation
        | UpdateDetails of FcDetails

    let interpret command state =
        match command with
        | Register c when state.name = Some c -> []
        | Register c -> [Events.FcCreated c]
        | UpdateAddress c when state.address = Some c -> []
        | UpdateAddress c -> [Events.FcAddressChanged { address = c }]
        | UpdateContact c when state.contact = Some c -> []
        | UpdateContact c -> [Events.FcContactChanged { contact = c }]
        | UpdateDetails c when state.details = Some c -> []
        | UpdateDetails c -> [Events.FcDetailsChanged { details = c }]

    type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =

        let execute fc command : Async<unit> =
            let stream = resolve fc
            stream.Transact(interpret command)
        let read fc : Async<Summary> =
            let stream = resolve fc
            stream.Query id
        let queryEx fc (projection : Fold.State -> 't) : Async<int64*'t> =
            let stream = resolve fc
            stream.QueryEx(fun c -> c.Version, projection c.State)

        member __.UpdateName(id, value) = execute id (Register value)
        member __.UpdateAddress(id, value) = execute id (UpdateAddress value)
        member __.UpdateContact(id, value) = execute id (UpdateContact value)
        member __.UpdateDetails(id, value) = execute id (UpdateDetails value)
        member __.Read id : Async<Summary> = read id
        member __.QueryWithVersion(id, render : Fold.State -> 'res) : Async<int64*'res> = queryEx id render

open Equinox.CosmosStore
open System

module Log =

    open Serilog
    open Serilog.Events
    let verbose = true // false will remove lots of noise
    let log =
        let c = LoggerConfiguration()
        let c = if verbose then c.MinimumLevel.Debug() else c
        let c = c.WriteTo.Sink(Core.Log.InternalMetrics.Stats.LogSink()) // to power Log.InternalMetrics.dump
        let c = c.WriteTo.Seq("http://localhost:5341") // https://getseq.net
        let c = c.WriteTo.Console(if verbose then LogEventLevel.Debug else LogEventLevel.Information)
        c.CreateLogger()
    let dumpMetrics () = Core.Log.InternalMetrics.dump log

module Store =

    let read key = Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get
    let appName = "equinox-tutorial"
    let factory = CosmosStoreClientFactory(TimeSpan.FromSeconds 5., 2, TimeSpan.FromSeconds 5., mode=Microsoft.Azure.Cosmos.ConnectionMode.Gateway)
    let client = factory.Create(Discovery.ConnectionString (read "EQUINOX_COSMOS_CONNECTION"))
    let conn = CosmosStoreConnection(client, read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER")
    let context = CosmosStoreContext(conn)
    let cache = Equinox.Cache(appName, 20)
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching

open FulfilmentCenter

let category = CosmosStoreCategory(Store.context, Events.codec, Fold.fold, Fold.initial, Store.cacheStrategy, AccessStrategy.Unoptimized)
let resolve id = Equinox.Stream(Log.log, category.Resolve(streamName id), maxAttempts = 3)
let service = Service(resolve)

let fc = "fc0"
service.UpdateName(fc, { code="FC000"; name="Head" }) |> Async.RunSynchronously
service.Read(fc) |> Async.RunSynchronously 

Log.dumpMetrics ()

/// Manages ingestion of summary events tagged with the version emitted from FulfilmentCenter.Service.QueryWithVersion
module FulfilmentCenterSummary =

    let streamName id = FsCodec.StreamName.create "FulfilmentCenterSummary" id

    module Events =
        type UpdatedData = { version : int64; state : Summary }
        type Event =
            | Updated of UpdatedData
            interface TypeShape.UnionContract.IUnionContract
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    type StateSummary = { version : int64; state : Types.Summary }
    type State = StateSummary option
    let initial = None
    let evolve _state = function
        | Events.Updated v -> Some v
    let fold s xs = Seq.fold evolve s xs

    type Command =
        | Update of version : int64 * Types.Summary
    let interpret command (state : State) =
        match command with
        | Update (uv,_us) when state |> Option.exists (fun s -> s.version > uv) -> []
        | Update (uv,us) -> [Events.Updated { version = uv; state = us }]

    type Service internal (resolve : string -> Equinox.Stream<Events.Event, State>) =

        let execute fc command : Async<unit> =
            let stream = resolve fc
            stream.Transact(interpret command)
        let read fc : Async<Summary option> =
            let stream = resolve fc
            stream.Query(Option.map (fun s -> s.state))

        member __.Update(id, version, value) = execute id (Update (version,value))
        member __.TryRead id : Async<Summary option> = read id
