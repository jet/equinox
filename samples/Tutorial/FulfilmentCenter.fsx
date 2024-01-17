#if !LOCAL
#I "bin/Debug/net6.0/"
#r "System.Net.Http"
#r "System.Runtime.Caching.dll"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Equinox.dll"
#r "FSharp.UMX.dll"
#r "FsCodec.dll"
#r "TypeShape.dll"
#r "FsCodec.SystemTextJson.dll"
#r "Microsoft.Azure.Cosmos.Client.dll"
#r "Serilog.Sinks.Seq.dll"
#r "Equinox.CosmosStore.dll"
#else
#r "nuget:Equinox.MemoryStore, *-*"
#r "nuget:Equinox.CosmosStore, *-*"
#r "nuget:FsCodec.SystemTextJson, *-*"
#r "nuget:Serilog.Sinks.Console"
#r "nuget:Serilog.Sinks.Seq"
#endif

open FSharp.UMX
open System

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
            isWeekendDeliveries : bool option
            businessName : string option }
    type Summary = { name : FcName option; address : Address option; contact : ContactInformation option; details : FcDetails option }

module FulfilmentCenter =

    let [<Literal>] CategoryName = "FulfilmentCenter"
    let streamId = FsCodec.StreamId.gen id

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
        let codec = FsCodec.SystemTextJson.CodecJsonElement.Create<Event>()

    module Fold =

        type State = Summary
        let initial = { name = None; address = None; contact = None; details = None }
        let evolve state : Events.Event -> Summary = function
            | Events.FcCreated x | Events.FcRenamed x -> { state with name = Some x }
            | Events.FcAddressChanged x -> { state with address = Some x.address }
            | Events.FcContactChanged x -> { state with contact = Some x.contact }
            | Events.FcDetailsChanged x -> { state with details = Some x.details }
        let fold = Array.fold evolve

    module Decisions =

        let register n state = [|
            if state.name <> Some n then
                Events.FcCreated n |]
        let updateAddress a state = [|
            if state.address <> Some a then
                Events.FcAddressChanged { address = a } |]
        let updateContact c state = [|
            if state.contact <> Some c then
                Events.FcContactChanged { contact = c } |]
        let updateDetails d state= [|
            if state.details <> Some d then
                Events.FcDetailsChanged { details = d } |]

    type Service internal (resolve: string -> Equinox.Decider<Events.Event, Fold.State>) =

        member _.UpdateName(fc, value) =
            let decider = resolve fc
            decider.Transact(Decisions.register value)
        member _.UpdateAddress(fc, value) =
            let decider = resolve fc
            decider.Transact(Decisions.updateAddress value)
        member _.UpdateContact(fc, value) =
            let decider = resolve fc
            decider.Transact(Decisions.updateContact value)
        member _.UpdateDetails(fc, value) =
            let decider = resolve fc
            decider.Transact(Decisions.updateDetails value)
        member _.Read fc: Async<Summary> =
            let decider = resolve fc
            decider.Query id
        member _.QueryWithVersion(fc, render: Fold.State -> 'res): Async<int64*'res> =
            let decider = resolve fc
            decider.QueryEx(fun c -> c.Version, render c.State)

open Equinox.CosmosStore

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
    let connector = CosmosStoreConnector(Discovery.ConnectionString (read "EQUINOX_COSMOS_CONNECTION"), TimeSpan.FromSeconds 5., 2, TimeSpan.FromSeconds 5.)
    let databaseId, containerId = read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER"
    let client = connector.Connect(databaseId, [| containerId |]) |> Async.RunSynchronously
    let context = CosmosStoreContext(client, databaseId, containerId, tipMaxEvents = 256)
    let cache = Equinox.Cache(appName, 20)
    let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching

open FulfilmentCenter

let service =
    let cat = CosmosStoreCategory(Store.context, CategoryName, Events.codec, Fold.fold, Fold.initial, AccessStrategy.Unoptimized, Store.cacheStrategy)
    Service(streamId >> Equinox.Decider.forStream Log.log cat)

let fc = "fc0"
service.UpdateName(fc, { code="FC000"; name="Head" }) |> Async.RunSynchronously
service.Read(fc) |> Async.RunSynchronously

Log.dumpMetrics ()

/// Manages ingestion of summary events tagged with the version emitted from FulfilmentCenter.Service.QueryWithVersion
module FulfilmentCenterSummary =

    let [<Literal>] private CategoryName = "$FulfilmentCenter"
    let private streamId = FsCodec.StreamId.gen id

    module Events =
        type UpdatedData = { version : int64; state : Summary }
        type Event =
            | Updated of UpdatedData
            interface TypeShape.UnionContract.IUnionContract
        let codec = FsCodec.SystemTextJson.Codec.Create<Event>()

    type StateSummary = { version : int64; state : Types.Summary }
    type State = StateSummary option
    let initial = None
    let evolve _state = function
        | Events.Updated v -> Some v
    let fold = Array.fold evolve

    let decideIngest version summary (state: State) = [|
        if state |> Option.exists (fun s -> s.version > version) |> not then
            Events.Updated { version = version; state = summary } |]

    type Service internal (resolve: string -> Equinox.Decider<Events.Event, State>) =

        member _.Update(id, version, value) =
            let decider = resolve id
            decider.Transact(decideIngest version value)
        member _.TryRead id: Async<Summary option> =
            let decider = resolve id
            decider.Query(Option.map _.state)
