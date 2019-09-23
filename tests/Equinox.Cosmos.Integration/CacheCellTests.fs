module Equinox.Cosmos.Integration.CacheCellTests

open Equinox.Core
open Swensen.Unquote
open System.Threading
open Xunit
open System

[<Fact>]
let ``AsyncLazy correctness`` () = async {
    // ensure that the encapsulated computation fires only once
    let count = ref 0
    let cell = AsyncLazy (async { return Interlocked.Increment count })
    let! accessResult = [|1 .. 100|] |> Array.map (fun i -> cell.AwaitValue ()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 1) @> }

[<Fact>]
let ``AsyncCacheCell correctness`` () = async {
    // ensure that the encapsulated computation fires only once and that expiry functions as expected
    let state = ref 0
    let expectedValue = ref 1
    let cell = AsyncCacheCell (async { return Interlocked.Increment state }, fun value -> value <> !expectedValue)

    let! accessResult = [|1 .. 100|] |> Array.map (fun _i -> cell.AwaitValue ()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 1) @>

    incr expectedValue

    let! accessResult = [|1 .. 100|] |> Array.map (fun _i -> cell.AwaitValue ()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 2) @> }

[<Theory; InlineData false; InlineData true>]
let ``AsyncCacheCell correctness with throwing`` initiallyThrowing = async {
    // ensure that the encapsulated computation fires only once and that expiry functions as expected
    let state = ref 0
    let expectedValue = ref 1
    let mutable throwing = initiallyThrowing
    let update = async {
        let r = Interlocked.Increment state
        if throwing then
            do! Async.Sleep 2000
            invalidOp "fails"
        return r
    }

    let cell = AsyncCacheCell (update, fun value -> value <> !expectedValue)

    // If the runner is throwing, we want to be sure it doesn't place us in a failed state forever, per the semantics of Lazy<T>
    // However, we _do_ want to be sure that the function only runs once
    if initiallyThrowing then
        let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.AwaitValue () |> Async.Catch) |> Async.Parallel
        test <@ accessResult |> Array.forall (function Choice2Of2 (:? InvalidOperationException) -> true | _ -> false) @>
        throwing <- false
    else
        let! r = cell.AwaitValue()
        test <@ 1 = r @>
    
    incr expectedValue

    let! accessResult = [|1 .. 100|] |> Array.map (fun _ -> cell.AwaitValue ()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 2) @>

    // invalidate the cached value
    incr expectedValue
    // but make the comptutation ultimately fail
    throwing <- true
    // All share the failure
    let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.AwaitValue () |> Async.Catch) |> Async.Parallel
    test <@ accessResult |> Array.forall (function Choice2Of2 (:? InvalidOperationException) -> true | _ -> false) @>
    // Restore normality
    throwing <- false

    incr expectedValue

    let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.AwaitValue ()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 4) @> }