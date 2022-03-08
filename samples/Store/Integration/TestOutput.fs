namespace global

open Serilog

module TestOutputRenderer =

    let render template =
        let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter(template, null)
        fun logEvent ->
            use writer = new System.IO.StringWriter()
            formatter.Format(logEvent, writer)
            writer.ToString()
    let full = render "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message} {Properties} {NewLine}{Exception}"

type TestOutputRendererSink(writeLine) =
    let renderer = TestOutputRenderer.full
    interface Serilog.Core.ILogEventSink with member _.Emit e = e |> renderer |> writeLine

type LogCaptureBuffer() =
    let captured = ResizeArray()
    interface Serilog.Core.ILogEventSink with member _.Emit logEvent = captured.Add logEvent
    member _.Clear () = captured.Clear()
    member _.ChooseCalls chooser = captured |> Seq.choose chooser |> List.ofSeq

type TestOutput(testOutput : Xunit.Abstractions.ITestOutputHelper) =

    let write (m : string) =
        m.TrimEnd '\n' |> testOutput.WriteLine
        m |> System.Diagnostics.Trace.WriteLine
    let testOutputAndTrace = TestOutputRendererSink(write)

    member _.CreateLogger(?sink : Serilog.Core.ILogEventSink) =
        LoggerConfiguration()
            .WriteTo.Sink(testOutputAndTrace)
            .WriteTo.Seq("http://localhost:5341")
            |> fun c -> match sink with Some s -> c.WriteTo.Sink(s) | None -> c
            |> fun c -> c.CreateLogger()
