module Integration.CartIntegration

open Backend.Carts
open Domain
open Foldunk
open Serilog
open Swensen.Unquote
open System

let createLogger hookObservers =
    LoggerConfiguration()
        .WriteTo.Observers(Action<_> hookObservers)
        .CreateLogger()

type LogCapture() =
    let capture = ResizeArray()
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe capture.Add
    member __.Clear () = capture.Clear()
    member __.Items = capture.ToArray()

// Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
    let write logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer);
        writer |> string |> testOutput.WriteLine
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe write

type Tests(testOutputHelper) =
    let output = TestOutputAdapter testOutputHelper
    let subscribe = output.Subscribe >> ignore
    let createLoggerWithCapture () =
        let capture = LogCapture()
        let hook obs =
            obs |> subscribe
            obs |> subscribe
        createLogger hook, capture
    let createLogger () =
        createLogger subscribe

    let createServiceWithBatchSize batchSize =
        let store : IEventStream<_,_,_> = Stores.MemoryStreamStore() :> _
        CartService(store)

    [<AutoData>]
    let ``Basic Flow Execution`` cartId1 cartId2 ((_,skuId,quantity) as args) = Async.RunSynchronously <| async {
        let log, service = createLogger (), createServiceWithBatchSize 50 
        let decide (ctx: DecisionState<_,_>) = async {
            Cart.Commands.AddItem args |> Cart.Commands.interpret |> ctx.Execute
            return ctx.Complete ctx.State }
        let verify = function
            | { Cart.Folds.State.items = [ item ] } ->
                let expectedItem : Cart.Folds.ItemInfo = { skuId = skuId; quantity = quantity; returnsWaived = false }
                test <@ expectedItem = item @>
            | x -> x |> failwithf "Expected to find item, got %A"
        let actTrappingStateAsSaved cartId = service.Execute log cartId decide
        let actLoadingStateSeparately cartId = async {
            let! _ = service.Execute log cartId decide
            return! service.Load log cartId }
        let! expected = cartId1 |> actTrappingStateAsSaved
        let! actual = cartId2 |> actLoadingStateSeparately 
        test <@ expected = actual @>
        verify expected
        verify actual
    }

    [<AutoData>]
    let ``Can read in batches`` context cartId skuId = async {
        let log, capture = createLoggerWithCapture ()
        let service = createServiceWithBatchSize 3
        let decide (ctx : DecisionState<_,_>) = async {
            for _ in 1..5 do
                for c in [Cart.Commands.AddItem (context, skuId, 1); Cart.Commands.RemoveItem (context, skuId)] do
                    ctx.Execute <| Cart.Commands.interpret c
            return ctx.Complete () }
        do! service.Execute log cartId decide

        let load (ctx : DecisionState<_,_>) = async { return ctx.Complete ctx.State }
        
        let! state = service.Execute log cartId load
        test <@ Seq.isEmpty state.items @>

        let batchReads = capture.Items |> Seq.filter (fun e -> "ReadStreamEventBackwards" = string e.Properties.["Action"])
        test <@ 4 = Seq.length batchReads @> // Need to read 4 batches to read 10 events in batches of 3
    }