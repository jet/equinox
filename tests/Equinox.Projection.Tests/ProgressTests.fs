module ProgressTests

open Equinox.Projection
open Swensen.Unquote
open System.Collections.Generic
open Xunit

let mkDictionary xs = Dictionary<string,int64>(dict xs)

let [<Fact>] ``Empty has zero streams pending or progress to write`` () =
    let sut = Progress.State<_>()
    let queue = sut.InScheduledOrder(fun _ -> 0)
    test <@ Seq.isEmpty queue @>

let [<Fact>] ``Can add multiple batches with overlapping streams`` () =
    let sut = Progress.State<_>()
    let noBatchesComplete () = failwith "No bathes should complete"
    sut.AppendBatch(noBatchesComplete, mkDictionary ["a",1L; "b",2L])
    sut.AppendBatch(noBatchesComplete, mkDictionary ["b",2L; "c",3L])

let [<Fact>] ``Marking Progress removes batches and triggers the callbacks`` () =
    let sut = Progress.State<_>()
    let mutable callbacks = 0
    let complete () = callbacks <- callbacks + 1
    sut.AppendBatch(complete, mkDictionary ["a",1L; "b",2L])
    sut.MarkStreamProgress("a",1L) |> ignore
    sut.MarkStreamProgress("b",1L) |> ignore
    1 =! callbacks

let [<Fact>] ``Marking progress is not persistent`` () =
    let sut = Progress.State<_>()
    let mutable callbacks = 0
    let complete () = callbacks <- callbacks + 1
    sut.AppendBatch(complete, mkDictionary ["a",1L])
    sut.MarkStreamProgress("a",2L) |> ignore
    sut.AppendBatch(complete, mkDictionary ["a",1L; "b",2L])
    1 =! callbacks

// TODO: lots more coverage of newer functionality - the above were written very early into the exercise