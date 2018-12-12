module Equinox.Cosmos.Integration.CacheCellTests

open Equinox.Store.Infrastructure
open Swensen.Unquote
open System.Threading
open Xunit

[<Fact>]
let ``AsyncLazy correctness`` () = async {
    // ensure that the encapsulated computation fires only once
    let count = ref 0
    let cell = AsyncLazy (async { return Interlocked.Increment count })
    let! accessResult = [|1 .. 100|] |> Array.map (fun i -> cell.AwaitValue ()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 1) @> }

[<Fact>]
let ``AsyncCacheCell correctness`` () = async {
    // ensure that the encapsulated computation fires only once
    // and that expiry functions as expected
    let state = ref 0
    let expectedValue = ref 1
    let cell = AsyncCacheCell (async { return Interlocked.Increment state }, fun value -> value <> !expectedValue)

    let! accessResult = [|1 .. 100|] |> Array.map (fun i -> cell.AwaitValue ()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 1) @>

    incr expectedValue

    let! accessResult = [|1 .. 100|] |> Array.map (fun i -> cell.AwaitValue ()) |> Async.Parallel
    test <@ accessResult |> Array.forall ((=) 2) @> }
