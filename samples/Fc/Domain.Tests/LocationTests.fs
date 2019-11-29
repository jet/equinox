module LocationTests

open FsCheck.Xunit
open FSharp.UMX
open Location
open Swensen.Unquote
open System

module Location =

    open Equinox.MemoryStore

    module Series =

        let resolve store = Resolver(store, Series.Events.codec, Series.Folds.fold, Series.Folds.initial).Resolve

    module Epoch =

        let resolve store = Resolver(store, Epoch.Events.codec, Epoch.Folds.fold, Epoch.Folds.initial).Resolve

    module MemoryStore =

        let createService (zeroBalance, shouldClose) store =
            let maxAttempts = Int32.MaxValue
            let series = Series.create (Series.resolve store) maxAttempts
            let epochs = Epoch.create (Epoch.resolve store) maxAttempts
            create (zeroBalance, shouldClose) (series, epochs)

let run (service : LocationService) (IdsAtLeastOne locations, deltas : _[]) = Async.RunSynchronously <| async {
    let runId = mkId () // Need to make making state in store unique when replaying or shrinking
    let locations = locations |> Array.map (fun x -> % (sprintf "%O_%O" runId x))

    let updates = deltas |> Seq.mapi (fun i x -> locations.[i % locations.Length], x) |> Seq.cache

    (* Apply random deltas *)

    let adjust delta (bal : Epoch.Folds.Balance) =
        let value = max -bal delta
        if value = 0 then 0, []
        else value, [Location.Epoch.Events.Delta { value = value }]
    let! appliedDeltas = seq { for loc,x in updates -> async { let! _,eff = service.Execute(loc, adjust x) in return loc,eff } } |> Async.Parallel
    let expectedBalances = Seq.append (seq { for l in locations -> l, 0}) appliedDeltas |> Seq.groupBy fst |> Seq.map (fun (l,xs) -> l, xs |> Seq.sumBy snd) |> Set.ofSeq

    (* Verify loading yields identical state *)

    let! balances = seq { for loc in locations -> async { let! bal,() = service.Execute(loc,(fun _ -> (),[])) in return loc,bal } } |> Async.Parallel
    test <@ expectedBalances = Set.ofSeq balances @> }

let [<Property>] ``MemoryStore properties`` maxEvents args =
    let store = Equinox.MemoryStore.VolatileStore()
    let zeroBalance = 0
    let maxEvents = max 1 maxEvents
    let shouldClose (state : Epoch.Folds.OpenState) = state.count > maxEvents
    let service = Location.MemoryStore.createService (zeroBalance, shouldClose) store
    run service args

type Cosmos(testOutput) =

    let context,cache = Cosmos.connect ()

    let log = testOutput |> TestOutputAdapter |> createLogger
    do Serilog.Log.Logger <- log

    let [<Property>] properties maxEvents args =
        let zeroBalance = 0
        let maxEvents = max 1 maxEvents
        let shouldClose (state : Epoch.Folds.OpenState) = state.count > maxEvents
        let service = Location.Cosmos.createService (zeroBalance, shouldClose) (context,cache,Int32.MaxValue)
        run service args