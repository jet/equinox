module Equinox.CosmosStore.Integration.CacheCellTests

open Equinox.Core
open Swensen.Unquote
open System
open System.Threading
open Xunit

[<Fact>]
let ``AsyncLazy correctness`` () = async {
    // ensure that the encapsulated computation fires only once
    let count = ref 0
    let cell = AsyncLazy (async { return Interlocked.Increment count })
    false =! cell.IsValid()
    let! accessResult = [|1 .. 100|] |> Array.map (fun _ -> cell.AwaitValue()) |> Async.Parallel
    true =! cell.IsValid()
    test <@ accessResult |> Array.forall ((=) 1) @>
}

[<Fact>]
let ``AsyncCacheCell correctness`` () = async {
    // ensure that the encapsulated computation fires only once and that expiry functions as expected
    let state = ref 0
    let expectedValue = ref 1
    let cell = AsyncCacheCell (async { return Interlocked.Increment state }, fun value -> value <> !expectedValue)
    false =! cell.IsValid()

    let! accessResult = [|1 .. 100|] |> Array.map (fun _i -> cell.AwaitValue()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 1) @>
    true =! cell.IsValid()

    incr expectedValue

    let! accessResult = [|1 .. 100|] |> Array.map (fun _i -> cell.AwaitValue()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 2) @>
    true =! cell.IsValid()
}

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
    false =! cell.IsValid()

    // If the runner is throwing, we want to be sure it doesn't place us in a failed state forever, per the semantics of Lazy<T>
    // However, we _do_ want to be sure that the function only runs once
    if initiallyThrowing then
        let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.AwaitValue() |> Async.Catch) |> Async.Parallel
        test <@ accessResult |> Array.forall (function Choice2Of2 (:? InvalidOperationException) -> true | _ -> false) @>
        throwing <- false
        false =! cell.IsValid()
    else
        let! r = cell.AwaitValue()
        true =! cell.IsValid()
        test <@ 1 = r @>

    incr expectedValue

    let! accessResult = [|1 .. 100|] |> Array.map (fun _ -> cell.AwaitValue()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 2) @>
    true =! cell.IsValid()

    // invalidate the cached value
    incr expectedValue
    false =! cell.IsValid()
    // but make the computation ultimately fail
    throwing <- true
    // All share the failure
    let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.AwaitValue() |> Async.Catch) |> Async.Parallel
    test <@ accessResult |> Array.forall (function Choice2Of2 (:? InvalidOperationException) -> true | _ -> false) @>
    // Restore normality
    throwing <- false
    false =! cell.IsValid()

    incr expectedValue

    let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.AwaitValue()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 4) @>
    true =! cell.IsValid()
}
