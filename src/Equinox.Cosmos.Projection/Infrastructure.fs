namespace Equinox.Cosmos.Projection

open System
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module Impl =
    type Async with
        /// Re-raise an exception so that the current stacktrace is preserved
        static member Raise(e : #exn) : Async<'T> = Async.FromContinuations (fun (_,ec,_) -> ec e)
        static member Sleep(t : TimeSpan) : Async<unit> = Async.Sleep(int t.TotalMilliseconds)

        /// eagerly cancels the parent workflow without waiting for cooperative cancellation by the child computation.
        static member TimeoutAfterEager (timeout : TimeSpan) (workflow : Async<'T>) : Async<'T> = async {
            let! ct = Async.CancellationToken
            return! Async.FromContinuations(fun (sk,ek,ck) ->
                let mutable latch = 0
                let inline protect k t = if Interlocked.Increment &latch = 1 then k t
                let cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
                do cts.CancelAfter timeout
                let _ = cts.Token.Register(fun () -> protect ek (TimeoutException("async workflow has timed out")))
                Async.StartWithContinuations(workflow, protect sk, protect ek, protect ck, cts.Token))
        }

[<AbstractClass>]
type AsyncBuilderAbstract() =
    member __.Zero() = async.Zero()
    member __.Return t = async.Return t
    member __.ReturnFrom t = async.ReturnFrom t
    member __.Bind(f,g) = async.Bind(f,g)
    member __.Combine(f,g) = async.Combine(f,g)
    member __.Delay f = async.Delay f
    member __.While(c,b) = async.While(c,b)
    member __.For(xs,b) = async.For(xs,b)
    member __.Using(d,b) = async.Using(d,b)
    member __.TryWith(b,e) = async.TryWith(b,e)
    member __.TryFinally(b,f) = async.TryFinally(b,f)

type TaskBuilder(?ct : CancellationToken) =
    inherit AsyncBuilderAbstract()
    member __.Run f : Task<'T> = Async.StartAsTask(f, ?cancellationToken = ct)

type UnitTaskBuilder(?ct : CancellationToken, ?timeout : TimeSpan) =
    inherit AsyncBuilderAbstract()
    
    member __.Run workflow : Task = 
        let wrapped = match timeout with None -> workflow | Some t -> Async.TimeoutAfterEager t workflow
        let task = Async.StartAsTask(wrapped, ?cancellationToken = ct)
        task :> _