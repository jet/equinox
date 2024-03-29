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
