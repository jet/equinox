module Equinox.Core.Tests.BatchingTests

open Equinox.Core.Batching
open FsCheck.Xunit
open Swensen.Unquote
open System
open System.Collections.Concurrent
open System.Threading
open Xunit

[<Fact>]
let ``Batcher correctness`` () = async {
    let mutable batches = 0
    let mutable active = 0
    let dispatch (reqs: int[]) = async {
        let concurrency = Interlocked.Increment &active
        1 =! concurrency
        Interlocked.Increment &batches |> ignore
        do! Async.Sleep(10 * reqs.Length)
        let concurrency = Interlocked.Decrement &active
        0 =! concurrency
        return reqs
    }
    let cell = Batcher(dispatch, LingerMs = 40)
    let! results = [1 .. 100] |> Seq.map cell.Execute |> Async.Parallel
    test <@ set (Seq.concat results) = set [1 .. 100] @>
    // Linger of 40ms makes this tend strongly to only be 1 batch, but no guarantees
    test <@ 1 <= batches && batches < 3 @>
}

[<Property>]
let ``Batcher error handling`` shouldFail = async {
    let fails = ConcurrentBag() // Could be a ResizeArray per spec, but this removes all doubt
    let dispatch (reqs: int[]) = async {
        if shouldFail () then
            reqs |> Seq.iter fails.Add
            failwith $"failing %A{reqs}"
        return reqs
    }
    let cell = Batcher dispatch
    let input = [1 .. 100]
    let! results = input |> Seq.map (cell.Execute >> Async.Catch) |> Async.Parallel
    let oks = results |> Array.choose (function Choice1Of2 r -> Some r | _ -> None)
    // Note extraneous exceptions we encounter (e.g. if we remove the catch in TryAdd)
    let cancels = results |> Array.choose (function Choice2Of2 e when not (e.Message.Contains "failing") -> Some e | _ -> None)
    let inputs, outputs = set input, set (Seq.concat oks |> Seq.append fails)
    test <@ inputs.Count = outputs.Count
            && Array.isEmpty cancels
            && inputs = outputs @>
}
