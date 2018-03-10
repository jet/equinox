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
            .CreateLogger()

    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? Serilog.Events.ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|EsEvent|_|) (logEvent : Serilog.Events.LogEvent) : Foldunk.EventStore.Log.Event option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | (SerilogScalar (:? Foldunk.EventStore.Log.Event as x)) -> Some x
            | _ -> None)

    type LogCaptureBuffer() =
        let captured = ResizeArray()
        member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
            source.Subscribe (fun x -> x.RenderMessage () |> System.Diagnostics.Trace.Write; captured.Add x)
        member __.Clear () = captured.Clear()
        member __.Entries = captured.ToArray()
        member __.ExternalCalls =
            captured |> Seq.choose (|EsEvent|_|) |> List.ofSeq

open Foldunk.EventStore

/// To establish a local node to run the tests against:
/// PS> cinst eventstore-oss -y
/// PS> & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
// Connect with Gossip based cluster discovery in force, reconfiguring the external HTTP port to align with the port that the Commercial ES Cluster hosts its manager instances on
let connectToLocalEventStoreNode log =
    GesConnector("admin", "changeit", reqTimeout=TimeSpan.FromSeconds 1., reqRetries=3, requireMaster=true, log=Logger.SerilogVerbose log)
        .ConnectViaGossipAsync("localhost")
let defaultBatchSize = 500
let createGesGateway connection batchSize = GesGateway(connection, GesBatchingPolicy(maxBatchSize = batchSize))