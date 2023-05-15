[<AutoOpen>]
module internal Equinox.Core.Internal

module Log =

    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let withScalarProperty (key: string) (value: 'T) (log: Serilog.ILogger) =
        let enrich (e: Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(key, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = enrich evt })
    let [<return: Struct>] (|ScalarValue|_|): Serilog.Events.LogEventPropertyValue -> obj voption = function
        | :? Serilog.Events.ScalarValue as x -> ValueSome x.Value
        | _ -> ValueNone

#if !NO_TASK_SEQ
module TaskSeq =

    open FSharp.Control

    let takeWhile predicate (source: taskSeq<_>) = taskSeq {
        use e = source.GetAsyncEnumerator(System.Threading.CancellationToken())
        let! step = e.MoveNextAsync()
        let mutable go = step
        while go do
            let value = e.Current
            if predicate value then
                yield value
                let! more = e.MoveNextAsync()
                go <- more
            else
                go <- false
    }

    let takeWhileInclusive predicate (source: taskSeq<_>) = taskSeq {
        use e = source.GetAsyncEnumerator(System.Threading.CancellationToken())
        let! step = e.MoveNextAsync()
        let mutable go = step
        while go do
            let value = e.Current
            yield value
            if predicate value then
                let! more = e.MoveNextAsync()
                go <- more
            else
                go <- false
    }
#endif
