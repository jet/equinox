module Equinox.Tests.LazyTaskTests

open Equinox.Core
open Swensen.Unquote
open Xunit

[<Fact>]
let ``LazyTask correctness`` () = async {
    // ensure that the encapsulated computation fires only once
    let mutable count = 0
    let cell = LazyTask(fun () -> task { return System.Threading.Interlocked.Increment &count })
    test <@ cell.TryCompleted() |> ValueOption.isNone @>
    let! accessResult = [|1 .. 100|] |> Array.map (fun _ -> cell.Await() |> Async.AwaitTask) |> Async.Parallel
    test <@ cell.TryCompleted() |> ValueOption.isSome @>
    test <@ accessResult |> Array.forall ((=) 1) @> }

// Pinning the fact that the algorithm is not sensitive to the reuse of the initial value of a cache entry
let [<Fact>] ``LazyTask.Empty is a true singleton, does not allocate`` () =
    let i1 = LazyTask<int>.Empty
    let i2 = LazyTask<int>.Empty
    test <@ obj.ReferenceEquals(i1, i2) @>

[<Theory; InlineData false; InlineData true>]
let ``LazyTask TryAwaitValid fault handling`` immediately = async {
    let expected = if immediately then "bad beginning" else "bad ending"
    let cell = LazyTask(fun () -> task {  if immediately then failwith "bad beginning"
                                          do! Task.Delay 10
                                          failwith "bad ending" })
    let res = cell.TryAwaitValid()
    // We've not awaited it yet, so nothing bad yet
    test <@ not res.IsFaulted @>
    let! res = res |> Async.AwaitTaskCorrect |> Async.Catch
    // Depending on whether there's a continuation in the task, we'll see different outcomes
    if immediately then Choice1Of2 ValueNone =! res
    else test <@ match res with Choice2Of2 e -> e.Message = expected | _ -> false @>

    let! res2 = cell.TryAwaitValid() |> Async.AwaitTaskCorrect
    // next attempt does not propagate the fault
    res2 =! ValueOption.None }

[<Theory; InlineData false; InlineData true>]
let ``LazyTask TryAwaitValid cancellation`` immediately = async {
    let cts = new System.Threading.CancellationTokenSource()
    let cell: LazyTask<unit> =
        if immediately then
            cts.Cancel()
            LazyTask(fun () -> Task.FromCanceled<unit> cts.Token)
        else
            LazyTask(fun () -> task { do! Task.Delay(10000, cts.Token)
                                      return () })
    if immediately then
        let! res = cell.TryAwaitValid() |> Async.AwaitTaskCorrect
        res =! ValueNone
    else
        cts.CancelAfter 100
        let! res = cell.TryAwaitValid() |> Async.AwaitTaskCorrect |> Async.Catch
        test <@ match res with Choice2Of2 (:? TaskCanceledException) -> true | _ -> false @>
    // Next time, we know it's faulted
    let! res = cell.TryAwaitValid() |> Async.AwaitTaskCorrect
    res =! ValueOption.None }
