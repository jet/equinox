module Equinox.Tests.AsyncLazyTests

open Equinox.Core
open Swensen.Unquote
open Xunit

[<Fact>]
let ``AsyncLazy correctness`` () = async {
    // ensure that the encapsulated computation fires only once
    let mutable count = 0
    let cell = AsyncLazy(fun () -> task { return System.Threading.Interlocked.Increment &count })
    test <@ cell.TryCompleted() |> ValueOption.isNone @>
    let! accessResult = [|1 .. 100|] |> Array.map (fun _ -> cell.Await() |> Async.AwaitTask) |> Async.Parallel
    test <@ cell.TryCompleted() |> ValueOption.isSome @>
    test <@ accessResult |> Array.forall ((=) 1) @> }

// Pinning the fact that the algorithm is not sensitive to the reuse of the initial value of a cache entry
let [<Fact>] ``AsyncLazy.Empty is a true singleton, does not allocate`` () =
    let i1 = AsyncLazy<int>.Empty
    let i2 = AsyncLazy<int>.Empty
    test <@ obj.ReferenceEquals(i1, i2) @>
