module Equinox.CosmosStore.Integration.AsyncBatchingGateTests

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open Equinox.Core
open FsCheck.Xunit
open Swensen.Unquote
open System.Threading
open Xunit

[<Fact>]
let ``AsyncBatchingGate correctness`` () = async {
    let batches, active = ref 0, ref 0
    let dispatch (reqs : int[]) = async {
        let concurrency = Interlocked.Increment active
        1 =! concurrency
        Interlocked.Increment batches |> ignore
        do! Async.Sleep(10 * reqs.Length)
        let concurrency = Interlocked.Decrement active
        0 =! concurrency
        return reqs
    }
    let cell = AsyncBatchingGate dispatch
    let! results = [1 .. 100] |> Seq.map cell.Execute |> Async.Parallel
    test <@ set (Seq.collect id results) = set [1 .. 100] @>
    // Default linger of 5ms makes this tend strongly to only be 1 batch
    test <@ !batches < 2 @>
}

[<Property>]
let ``AsyncBatchingGate error handling`` shouldFail = Async.RunSynchronously <| async {
    let fails = ConcurrentBag() // Could be a ResizeArray per spec, but this removes all doubt
    let dispatch (reqs : int[]) = async {
        if shouldFail () then
            reqs |> Seq.iter fails.Add
            failwithf "failing %A" reqs
        return reqs
    }
    let cell = AsyncBatchingGate dispatch
    let input = [1 .. 100]
    let! results = input |> Seq.map (cell.Execute >> Async.Catch) |> Async.Parallel
    let oks = results |> Array.choose (function Choice1Of2 r -> Some r | _ -> None)
    // Note extraneous exceptions we encounter (e.g. if we remove the catch in TryAdd)
    let cancels = results |> Array.choose (function Choice2Of2 e when not (e.Message.Contains "failing") -> Some e | _ -> None)
    let inputs, outputs = set input, set (Seq.collect id oks |> Seq.append fails)
    test <@ inputs.Count = outputs.Count
            && Array.isEmpty cancels
            && inputs = outputs @>
}
