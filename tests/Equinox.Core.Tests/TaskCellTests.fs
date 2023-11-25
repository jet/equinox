module Equinox.Core.Tests.TaskCellTests

open Equinox.Core
open Swensen.Unquote
open System
open Xunit

[<Fact>]
let ``TaskCell correctness`` () = async {
    // ensure that the encapsulated computation fires only once and that expiry functions as expected
    let mutable state = 0
    let mutable expectedValue = 1
    let cell = TaskCell((fun _ct -> task { return Interlocked.Increment &state }), fun value -> value <> expectedValue)

    false =! cell.IsValid()

    let! accessResult = [|1 .. 100|] |> Array.map (fun _i -> cell.Await CancellationToken.None |> Async.AwaitTaskCorrect) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 1) @>
    true =! cell.IsValid()

    expectedValue <- expectedValue + 1

    let! accessResult = [|1 .. 100|] |> Array.map (fun _i -> cell.Await CancellationToken.None |> Async.AwaitTaskCorrect) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 2) @>
    true =! cell.IsValid()
}

[<Theory; InlineData false; InlineData true>]
let ``TaskCell correctness with throwing`` initiallyThrowing = async {
    // ensure that the encapsulated computation fires only once and that expiry functions as expected
    let mutable state = 0
    let mutable expectedValue = 1
    let mutable throwing = initiallyThrowing
    let update ct = task {
        let r = Interlocked.Increment &state
        if throwing then
            do! Task.Delay(2000, ct)
            invalidOp "fails"
        return r
    }

    let cell = TaskCell(update, fun value -> value <> expectedValue)
    false =! cell.IsValid()

    // If the runner is throwing, we want to be sure it doesn't place us in a failed state forever, per the semantics of Lazy<T>
    // However, we _do_ want to be sure that the function only runs once
    if initiallyThrowing then
        let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.Await CancellationToken.None |> Async.AwaitTaskCorrect |> Async.Catch) |> Async.Parallel
        test <@ accessResult |> Array.forall (function Choice2Of2 (:? InvalidOperationException) -> true | _ -> false) @>
        throwing <- false
        false =! cell.IsValid()
    else
        let! r = cell.Await CancellationToken.None |> Async.AwaitTaskCorrect
        true =! cell.IsValid()
        test <@ 1 = r @>

    expectedValue <- expectedValue + 1

    let! accessResult = [|1 .. 100|] |> Array.map (fun _ -> cell.Await CancellationToken.None |> Async.AwaitTaskCorrect) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 2) @>
    true =! cell.IsValid()

    // invalidate the cached value
    expectedValue <- expectedValue + 1
    false =! cell.IsValid()
    // but make the computation ultimately fail
    throwing <- true
    // All share the failure
    let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.Await CancellationToken.None |> Async.AwaitTaskCorrect |> Async.Catch) |> Async.Parallel
    test <@ accessResult |> Array.forall (function Choice2Of2 (:? InvalidOperationException) -> true | _ -> false) @>
    // Restore normality
    throwing <- false
    false =! cell.IsValid()

    expectedValue <- expectedValue + 1

    let! accessResult = [|1 .. 10|] |> Array.map (fun _ -> cell.Await CancellationToken.None |> Async.AwaitTaskCorrect) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 4) @>
    true =! cell.IsValid()
}
