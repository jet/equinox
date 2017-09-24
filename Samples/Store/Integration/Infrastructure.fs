[<AutoOpen>]
module Example.Integration.Infrastructure

open Domain
open FsCheck
open Serilog
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

/// Needs an ES instance with default settings
/// TL;DR: At an elevated command prompt: choco install eventstore-oss; \ProgramData\chocolatey\bin\EventStore.ClusterNode.exe
let connectToLocalEventStoreNode () = async {
    let localhost = System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 1113)
    let conn = EventStore.ClientAPI.EventStoreConnection.Create(localhost)
    do! conn.ConnectAsync() |> Async.AwaitTask
    return conn }

let createGesGateway maxBatchSize eventStoreConnection =
    let connection = Foldunk.Stores.EventStore.GesConnection(eventStoreConnection)
    Foldunk.Stores.EventStore.GesGateway(connection, Foldunk.Stores.EventStore.GesStreamPolicy(maxBatchSize = maxBatchSize))
let createGesStream<'state,'event> (codec : Foldunk.EventSum.IEventSumEncoder<'event,byte[]>) gateway =
    Foldunk.Stores.EventStore.GesEventStream<'state, 'event>(gateway, codec)
let inline createGesStreamer<'state,'event> eventStoreConnection batchSize (codec : Foldunk.EventSum.IEventSumEncoder<'event,byte[]>) : Foldunk.IEventStream<'state,'event> =
    createGesGateway batchSize eventStoreConnection |> createGesStream<'state, 'event> codec :> _

let inline createInMemoryStreamer<'state,'event> () : Foldunk.IEventStream<'state,'event> =
    Foldunk.Stores.InMemoryStore.MemoryStreamStore() :> _