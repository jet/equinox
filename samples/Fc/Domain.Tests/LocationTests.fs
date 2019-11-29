module LocationTests

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

open FsCheck

type FsCheckGenerators =
    static member NonNullStrings = Arb.Default.String() |> Arb.filter (fun s -> s <> null)

type NonNullStringsPropertyAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(QuietOnSuccess = true, Arbitrary=[| typeof<FsCheckGenerators> |])

let [<NonNullStringsProperty>] ``parallel properties`` loc1 (locations : _[]) (deltas : _[]) maxEvents = Async.RunSynchronously <| async {
    let store = Equinox.MemoryStore.VolatileStore()
    let zeroBalance = 0
    let maxEvents = max 1 maxEvents
    let locations = Seq.append locations (Seq.singleton loc1) |> Seq.toArray
    let shouldClose (state : Epoch.Folds.OpenState) = state.count > maxEvents
    let service = Location.MemoryStore.createService (zeroBalance, shouldClose) store
    let adjust delta (bal : Epoch.Folds.Balance) =
        let value = max -bal delta
        if value = 0 then 0, []
        else value, [Location.Epoch.Events.Delta { value = value }]
    let updates = deltas |> Seq.mapi (fun i x -> locations.[i % locations.Length], x) |> Seq.cache

    let! applied = seq { for loc,x in updates -> async { let! _,eff = service.Execute(loc, adjust x) in return loc,eff } } |> Async.Parallel
    let! balances = seq { for loc in locations -> async { let! bal,() = service.Execute(loc,(fun _ -> (),[])) in return loc,bal } } |> Async.Parallel
    let expectedBalances = Seq.append (seq { for l in locations -> l, 0}) applied |> Seq.groupBy fst |> Seq.map (fun (l,xs) -> l, xs |> Seq.sumBy snd) |> Set.ofSeq
    test <@ expectedBalances = Set.ofSeq balances @> }