[<AutoOpen>]
module Samples.Store.Integration.Infrastructure

open Domain
open FsCheck
open Serilog
open Serilog.Events
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
        formatter.Format(logEvent, writer);
        writer |> string |> testOutput.WriteLine
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe writeSeriLogEvent

let createLogger hookObservers =
    LoggerConfiguration()
        .WriteTo.Observers(Action<_> hookObservers)
        .CreateLogger()

type LogCaptureBuffer() =
    let captured = ResizeArray()
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe captured.Add
    member __.Clear () = captured.Clear()
    member __.Entries = captured.ToArray()
    member __.ExternalCalls =
        [ for i in captured do
            let hasProp name = i.Properties.ContainsKey name
            let prop name = (string i.Properties.[name]).Trim '"'
            if hasProp "ExternalCall" && prop "ExternalCall" = "True" then
                yield prop "Action" ]

type SerilogTracerAdapter(emit) =
    let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
    let writeSerilogEvent (logEvent : Serilog.Events.LogEvent) =
        try 
            let tgp name =
                match logEvent.Properties.TryGetValue name with
                | true, value -> Some value
                | false, _ -> None
            let level =
                match logEvent.Level with
                | Serilog.Events.LogEventLevel.Fatal -> "Fatal"
                | Serilog.Events.LogEventLevel.Error -> "Error"
                | Serilog.Events.LogEventLevel.Information -> "Info"
                | Serilog.Events.LogEventLevel.Verbose -> "Trace"
                | Serilog.Events.LogEventLevel.Debug -> "Debug"
                | Serilog.Events.LogEventLevel.Warning | _ -> "Warn"
            match tgp "ExternalCall", tgp "Action", tgp "Latency" with
            | Some (SerilogScalar (:? bool as externalCall)), Some (SerilogScalar (:? string as action)), Some (SerilogScalar (:? TimeSpan as latency))
                    when externalCall ->
                let others =
                    seq {
                        for x in logEvent.Properties do
                            match x.Key with
                            | "ExternalCall" | "Action" | "Latency" -> ()
                            | _ -> yield x
                    }
                let othersRendered = [ for x in others -> sprintf "%s=%O" x.Key x.Value] |> String.concat ", "
                sprintf "%s %s-Elapsed=%O Values: %s" level action latency othersRendered |> emit
            | _ ->
                use writer = new System.IO.StringWriter()
                formatter.Format(logEvent, writer)
                writer |> string |> emit
          with _ -> 
                use writer = new System.IO.StringWriter()
                formatter.Format(logEvent, writer);
                writer |> string |> sprintf "Cannnot log event: %s" |> emit
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe writeSerilogEvent

/// Needs an ES instance with default settings
/// TL;DR: At an elevated command prompt: choco install eventstore-oss; \ProgramData\chocolatey\bin\EventStore.ClusterNode.exe
let connectToLocalEventStoreNode () = async {
    let localhost = System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 1113)
    let conn = EventStore.ClientAPI.EventStoreConnection.Create(localhost)
    do! conn.ConnectAsync() |> Async.AwaitTask
    return conn }

let createGesGateway eventStoreConnection maxBatchSize =
    let connection = Foldunk.EventStore.GesConnection(eventStoreConnection)
    Foldunk.EventStore.GesGateway(connection, Foldunk.EventStore.GesStreamPolicy(maxBatchSize = maxBatchSize))

let createGesStream<'state,'event> gateway (codec : Foldunk.EventSum.IEventSumEncoder<'event,byte[]>) streamName : Foldunk.IStream<_,_> =
    let store = Foldunk.EventStore.GesStreamStore<'state, 'event>(gateway, codec)
    Foldunk.EventStore.GesStream<'state, 'event>(store, streamName) :> _

let inline createMemStore () =
    Foldunk.MemoryStore.MemoryStreamStore()
let inline createMemStream<'state,'event> store streamName : Foldunk.IStream<'state,'event> =
    Foldunk.MemoryStore.MemoryStream(store, streamName) :> _