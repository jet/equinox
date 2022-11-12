namespace OpenTelemetry

open System.Diagnostics
open Serilog
open OpenTelemetry

type SerilogExporter() =
  inherit BaseExporter<Activity>()

  let log = LoggerConfiguration()
                .WriteTo.Console(
                    outputTemplate = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {Properties}{NewLine}{Exception}",
                    theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code).CreateLogger() :> ILogger

  override this.Export(batch) =
      for span in batch do
          let mutable logger = log
          for tag in span.TagObjects do
              logger <- logger.ForContext(tag.Key, tag.Value)
          logger.Information("{Module}:{Operation}", span.Source.Name, span.DisplayName)
      ExportResult.Success



