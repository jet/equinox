[<AutoOpen>]
module Samples.Store.Integration.Infrastructure

open Domain
open FsCheck
open System

type FsCheckGenerators =
    static member CartId = Arb.generate |> Gen.map CartId |> Arb.fromGen
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member RequestId = Arb.generate |> Gen.map RequestId |> Arb.fromGen
    static member ContactPreferencesId =
        Arb.generate<Guid>
        |> Gen.map (fun x -> sprintf "%s@test.com" (x.ToString("N")))
        |> Gen.map ContactPreferences.Id
        |> Arb.fromGen

type AutoDataAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(Arbitrary = [|typeof<FsCheckGenerators>|], MaxTest = 1, QuietOnSuccess = true)

// Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
// NB VS does not surface these atm, but other test runners / test reports do
type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
    let writeSeriLogEvent logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer)
        writer |> string |> testOutput.WriteLine
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe writeSeriLogEvent

[<AutoOpen>]
module SerilogHelpers =
    open Serilog
    let createLogger hookObservers =
        LoggerConfiguration()
            .WriteTo.Observers(System.Action<_> hookObservers)
            .Destructure.AsScalar<Foldunk.EventStore.Metrics.Metric>()
            .CreateLogger()

    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? Serilog.Events.ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|EsMetric|_|) (logEvent : Serilog.Events.LogEvent) : Foldunk.EventStore.Metrics.Metric option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | (SerilogScalar (:? Foldunk.EventStore.Metrics.Metric as x)) -> Some x
            | _ -> None)

    type LogCaptureBuffer() =
        let captured = ResizeArray()
        member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
            source.Subscribe (fun x -> x.RenderMessage () |> System.Diagnostics.Trace.Write; captured.Add x)
        member __.Clear () = captured.Clear()
        member __.Entries = captured.ToArray()
        member __.ExternalCalls =
            captured |> Seq.choose (function EsMetric metric -> Some metric.action | _ -> None) |> List.ofSeq

/// Needs an ES instance with default settings
/// TL;DR: At an elevated command prompt: choco install eventstore-oss; \ProgramData\chocolatey\bin\EventStore.ClusterNode.exe
let connectToLocalEventStoreNode () = async {
    let localhost = System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 1113)
    let conn = EventStore.ClientAPI.EventStoreConnection.Create(localhost)
    do! conn.ConnectAsync() |> Async.AwaitTask
    return conn }

let defaultBatchSize = 500

let private createGesGateway eventStoreConnection maxBatchSize =
    let connection = Foldunk.EventStore.GesConnection(eventStoreConnection)
    Foldunk.EventStore.GesGateway(connection, Foldunk.EventStore.GesStreamPolicy(maxBatchSize = maxBatchSize))

let createGesStream<'event, 'state> eventStoreConnection batchSize (codec : Foldunk.EventSum.IEventSumEncoder<'event,byte[]>) streamName : Foldunk.IStream<_,_> =
    let gateway = createGesGateway eventStoreConnection batchSize
    let streamState = Foldunk.EventStore.GesStreamState<'event, 'state>(gateway, codec)
    Foldunk.EventStore.GesStream<'event, 'state>(streamState, streamName) :> _

let createGesStreamWithCompactionEventTypeOption<'event, 'state> eventStoreConnection batchSize compactionEventTypeOption (codec : Foldunk.EventSum.IEventSumEncoder<'event,byte[]>) streamName
    : Foldunk.IStream<'event, 'state> =
    let gateway = createGesGateway eventStoreConnection batchSize
    let streamState = Foldunk.EventStore.GesStreamState<'event, 'state>(gateway, codec, ?compactionEventType = compactionEventTypeOption)
    Foldunk.EventStore.GesStream<'event, 'state>(streamState, streamName) :> _

let createGesStreamWithCompactionPredicate<'event, 'state> eventStoreConnection windowSize compactionPredicate (codec : Foldunk.EventSum.IEventSumEncoder<'event,byte[]>) streamName
    : Foldunk.IStream<'event, 'state> =
    let gateway = createGesGateway eventStoreConnection windowSize
    let streamState = Foldunk.EventStore.GesStreamState<'event, 'state>(gateway, codec, compactionPredicate = compactionPredicate)
    Foldunk.EventStore.GesStream<'event, 'state>(streamState, streamName) :> _

let inline createMemStore () =
    Foldunk.MemoryStore.MemoryStreamStore()
let createMemStream<'event, 'state> store streamName : Foldunk.IStream<'event, 'state> =
    Foldunk.MemoryStore.MemoryStream(store, streamName) :> _