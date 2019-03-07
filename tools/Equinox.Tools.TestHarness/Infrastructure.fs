[<AutoOpen>]
module private Equinox.Tools.TestHarness.Prelude

open System
open System.Diagnostics
open System.Text
open System.Threading

/// Extracts the innermost exception if defined
let (|InnermostExn|) (exn : exn) =
    let rec aux (e : exn) =
        match e with
        | :? AggregateException as agg when agg.InnerExceptions.Count > 1 -> e
        | _ ->
            match e.InnerException with
            | null -> e
            | e0 -> aux e0

    if exn = null then nullArg "exn"
    aux exn

// https://gist.github.com/palladin/3761004/cefb9acbe5d03366b53c38c4b7afd7488b99134a
type internal SuccessException<'T>(value : 'T) =
    inherit Exception()
    member self.Value = value

type Async with
#if NET461
    static member Choice<'T>(workflows : seq<Async<'T option>>) : Async<'T option> = async {
        try
            let! _ =
                workflows
                |> Seq.map (fun workflow -> async {
                                                let! optionValue = workflow
                                                match optionValue with
                                                | None -> return None
                                                | Some v -> return raise <| new SuccessException<'T>(v)
                                           })
                |> Async.Parallel

            return None
        with
        | :? SuccessException<'T> as ex -> return Some ex.Value }
#endif
    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : System.Threading.Tasks.Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (sc,ec,_) ->
            task.ContinueWith(fun (t : System.Threading.Tasks.Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif t.IsCanceled then ec(new System.Threading.Tasks.TaskCanceledException())
                else sc t.Result)
            |> ignore)
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : System.Threading.Tasks.Task) : Async<unit> =
        Async.FromContinuations(fun (sc,ec,_) ->
            task.ContinueWith(fun (task : System.Threading.Tasks.Task) ->
                if task.IsFaulted then
                    let e = task.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif task.IsCanceled then
                    ec(System.Threading.Tasks.TaskCanceledException())
                else
                    sc ())
            |> ignore)

    static member map (f:'a -> 'b) (a:Async<'a>) : Async<'b> = async.Bind(a, f >> async.Return)
    static member choose a b = async {
        let! result = Async.Choice [|Async.map Some a ; Async.map Some b |]
        return Option.get result
    }

    /// Creates an async computation which runs the provided sequence of computations and completes
    /// when all computations in the sequence complete. Up to parallelism computations will
    /// be in-flight at any given point in time. Error or cancellation of any computation in
    /// the sequence causes the resulting computation to error or cancel, respectively.
    /// Like Async.Parallel but with support for throttling.
    /// Note that an array is allocated to contain the results of all computations.
    static member ParallelThrottled (parallelism:int) (tasks:seq<Async<'T>>) : Async<'T[]> = async {
        if parallelism < 1 then invalidArg "parallelism" "Must be positive number."
        use semaphore = new SemaphoreSlim(parallelism)
        let throttledWorker (task:Async<'T>) = async {
            let! ct = Async.CancellationToken
            do! semaphore.WaitAsync ct |> Async.AwaitTaskCorrect
            try return! task
            finally ignore(semaphore.Release())
        }

        return! tasks |> Seq.map throttledWorker |> Async.Parallel
    }

type StringBuilder with
    member sb.Appendf fmt = Printf.ksprintf (ignore << sb.Append) fmt
    member sb.Appendfn fmt = Printf.ksprintf (ignore << sb.AppendLine) fmt

    static member inline Build(builder : StringBuilder -> unit) =
        let instance = StringBuilder() // TOCONSIDER PooledStringBuilder.GetInstance()
        builder instance
        instance.ToString()