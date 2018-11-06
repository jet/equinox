[<AutoOpen>]
module Infrastructure

open System.Threading
open System.Threading.Tasks

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
    member __.TryWith(b,e) = async.TryWith(b,e)

type TaskBuilder(?ct : CancellationToken) =
    inherit AsyncBuilderAbstract()
    member __.Run f : Task<'T> = Async.StartAsTask(f, ?cancellationToken = ct)

type UnitTaskBuilder() =
    inherit AsyncBuilderAbstract()
    member __.Run f : Task = Async.StartAsTask f :> _

/// Async builder variation that automatically runs top-level expression as task
let task = new TaskBuilder()

/// Async builder variation that automatically runs top-level expression as untyped task
let utask = new UnitTaskBuilder()

/// <summary>Async builder variation that automatically runs top-level expression as task</summary>
/// <example>
///     let ct = new CancellationToken()
///     let _ = ctask ct { return 42 }
/// </example>
let ctask ct = new TaskBuilder(ct)
