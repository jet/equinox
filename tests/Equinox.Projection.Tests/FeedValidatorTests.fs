module Equinox.Projection.FeedValidator.Tests

open Equinox.Projection.Validation
open FsCheck.Xunit
open Swensen.Unquote
open Xunit

let [<Property>] ``Properties`` state index =
    let result,state' = StreamState.combine state index
    match state with
    | Some (Partial (_, pos)) when pos <= 1 -> ()
    | Some (Partial (min, pos)) when min > pos || min = 0 -> ()
    | None ->
        match state' with
        | All _ -> result =! New
        | Partial _ -> result =! Ok
    | Some (All x) ->
        match state',result with
        | All x' , Duplicate -> x' =! x
        | All x', New -> x' =! x+1
        | All x', Gap -> x' =! x 
        | x -> failwithf "Unexpected %A" x
    | Some (Partial (min, pos)) ->
        match state',result with
        | All 0,Duplicate when min=0 && index = 0 -> ()
        | All x,Duplicate when min=1 && pos = x && index = 0 -> ()
        | Partial (min', pos'), Duplicate -> min' =! max min' index; pos' =! pos
        | Partial (min', pos'), Ok
        | Partial (min', pos'), New -> min' =! min; pos' =! index
        | x -> failwithf "Unexpected %A" x

let [<Fact>] ``Zero on unknown stream is New`` () =
    let result,state = StreamState.combine None 0
    New =! result
    All 0 =! state

let [<Fact>] ``Non-zero on unknown stream is Ok`` () =
    let result,state = StreamState.combine None 1
    Ok =! result
    Partial (1,1) =! state

let [<Fact>] ``valid successor is New`` () =
    let result,state = StreamState.combine (All 0 |> Some) 1
    New =! result
    All 1 =! state

let [<Fact>] ``single immediate repeat is flagged`` () =
    let result,state = StreamState.combine (All 0 |> Some) 0 
    Duplicate =! result
    All 0 =! state

let [<Fact>] ``non-immediate repeat is flagged`` () =
    let result,state = StreamState.combine (All 1 |> Some) 0
    Duplicate =! result
    All 1 =! state

let [<Fact>] ``Gap is flagged`` () =
    let result,state = StreamState.combine (All 1 |> Some) 3
    Gap =! result
    All 1 =! state

let [<Fact>] ``Potential gaps are not flagged as such when we're processing a Partial`` () =
    let result,state = StreamState.combine (Some (Partial (1,1))) 3
    New =! result
    Partial (1,3) =! state

let [<Fact>] ``Earlier values widen the min when we're processing a Partial`` () =
    let result,state = StreamState.combine (Some (Partial (2,3))) 1
    Duplicate =! result
    Partial (1,3) =! state 