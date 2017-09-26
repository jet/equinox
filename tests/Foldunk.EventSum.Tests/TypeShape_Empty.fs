module internal TypeShape_Empty

// SOURCE: https://github.com/eiriktsarpalis/TypeShape/blob/master/samples/empty.fsx

// structural empty value generator

open System
open System.Collections.Generic
open TypeShape
open TypeShape_Utils

let rec mkEmptyFunc<'T> () : unit -> 'T =
    let mutable f = Unchecked.defaultof<unit -> 'T>
    if cache.TryGetValue(&f) then f
    else
        use mgr = cache.CreateRecTypeManager()
        mkEmptyFuncCached<'T> mgr

and private mkEmptyFuncCached<'T> (ctx : RecTypeManager) : unit -> 'T =
    match ctx.TryFind<unit -> 'T>() with
    | Some f -> f
    | None ->
        let _ = ctx.CreateUninitialized<unit -> 'T>(fun c () -> c.Value ())
        let f = mkEmptyFuncAux<'T> ctx
        ctx.Complete f

and private mkEmptyFuncAux<'T> (ctx : RecTypeManager) : unit -> 'T =
    let wrap (f : unit -> 'a) = unbox<unit -> 'T> f

    let mkMemberInitializer (shape : IShapeWriteMember<'DeclaringType>) =
        shape.Accept { new IWriteMemberVisitor<'DeclaringType, 'DeclaringType -> 'DeclaringType> with
            member __.Visit (shape : ShapeWriteMember<'DeclaringType, 'Field>) =
                let fe = mkEmptyFuncCached<'Field> ctx
                fun inst -> shape.Inject inst (fe ())
        }

    match shapeof<'T> with
    | Shape.Primitive -> fun () -> Unchecked.defaultof<'T>
    | Shape.Decimal -> wrap(fun () -> 0M)
    | Shape.BigInt -> wrap(fun () -> 0I)
    | Shape.Guid -> wrap(fun () -> Guid.Empty)
    | Shape.String -> wrap(fun () -> "")
    | Shape.TimeSpan -> wrap(fun () -> TimeSpan.Zero)
    | Shape.DateTime -> wrap(fun () -> DateTime.MinValue)
    | Shape.DateTimeOffset -> wrap(fun () -> DateTimeOffset.MinValue)
    | Shape.Unit -> wrap id
    | Shape.Enum s ->
        s.Accept { new IEnumVisitor<unit -> 'T> with
            member __.Visit<'Enum, 'Underlying when 'Enum : enum<'Underlying>>() =
                let ue = mkEmptyFuncCached<'Underlying> ctx
                wrap(fun () -> LanguagePrimitives.EnumOfValue<'Underlying,'Enum>(ue()))
        }

    | Shape.Nullable s ->
        s.Accept { new INullableVisitor<unit -> 'T> with
            member __.Visit<'t when 't : struct and 't :> ValueType and 't : (new : unit -> 't)>() = // 'T = 't
                wrap(fun () -> new Nullable<'t>())
        }

    | Shape.FSharpFunc s ->
        // empty<'T -> 'S> = fun (_ : 'T) -> empty<'S>
        s.Accept { new IFSharpFuncVisitor<unit -> 'T> with
            member __.Visit<'Dom, 'Cod> () = // 'T = 'Cod -> 'Dom
                let de = mkEmptyFuncAux<'Cod> ctx
                wrap(fun () -> fun (_ : 'Dom) -> de ()) }

    | Shape.DefaultConstructor s ->
        s.Accept { new IDefaultConstructorVisitor<unit -> 'T> with
            member __.Visit<'t when 't : (new : unit -> 't)>() = // 'T = 't
                wrap(fun () -> new 't ())
        }

    | Shape.FSharpOption s ->
        s.Accept { new IFSharpOptionVisitor<unit -> 'T> with
            member __.Visit<'t>() = // 'T = 't option
                wrap(fun () -> Option<'t>.None) }

    | Shape.KeyValuePair s ->
        s.Accept { new IKeyValuePairVisitor<unit -> 'T> with
            member __.Visit<'k,'v>() = // 'T = KeyValuePair<'k,'v>
                let ke,ve = mkEmptyFuncCached<'k> ctx, mkEmptyFuncCached<'v> ctx
                wrap(fun () -> new KeyValuePair<'k,'v>(ke(),ve())) }

    | Shape.FSharpList s ->
        s.Accept { new IFSharpListVisitor<unit -> 'T> with
            member __.Visit<'t>() = wrap(fun () -> List.empty<'t>) } // 'T = 't list

    | Shape.Array s ->
        s.Accept { new IArrayVisitor<unit -> 'T> with
            member __.Visit<'t> rank = 
                match rank with
                | 1 -> wrap(fun () -> Array.empty<'t>) // 'T = 't []
                | 2 -> wrap(fun () -> Array2D.zeroCreate<'t> 0 0) // 'T = 't [,]
                | 3 -> wrap(fun () -> Array3D.zeroCreate<'t> 0 0 0) // 'T = 't [,,]
                | 4 -> wrap(fun () -> Array4D.zeroCreate<'t> 0 0 0 0) // 'T = 't [,,,]
                | _ -> failwithf "Unsupported type %O" typeof<'T> }

    | Shape.FSharpSet s ->
        s.Accept { new IFSharpSetVisitor<unit -> 'T> with
            member __.Visit<'t when 't : comparison>() = wrap(fun () -> Set.empty<'t>) } // 'T = Set<'t>

    | Shape.FSharpMap s ->
        s.Accept { new IFSharpMapVisitor<unit -> 'T> with
            member __.Visit<'k, 'v when 'k : comparison>() = wrap(fun () -> Map.empty<'k,'v>) } // 'T = Map<'k,'v>

    | Shape.Tuple (:? ShapeTuple<'T> as shape) ->
        let elemInitializers = shape.Elements |> Array.map mkMemberInitializer
        fun () ->
            let mutable inst = shape.CreateUninitialized()
            for f in elemInitializers do inst <- f inst
            inst

    | Shape.FSharpRecord (:? ShapeFSharpRecord<'T> as shape) ->
        let fieldInitializers = shape.Fields |> Array.map mkMemberInitializer
        fun () ->
            let mutable inst = shape.CreateUninitialized()
            for f in fieldInitializers do inst <- f inst
            inst

    | Shape.FSharpUnion (:? ShapeFSharpUnion<'T> as shape) ->
        let defaultUnionCase = shape.UnionCases |> Array.minBy (fun c -> c.Fields.Length)
        let fieldInitializers = defaultUnionCase.Fields |> Array.map mkMemberInitializer
        fun () ->
            let mutable inst = defaultUnionCase.CreateUninitialized()
            for f in fieldInitializers do inst <- f inst
            inst

    | Shape.CliMutable (:? ShapeCliMutable<'T> as shape) ->
        fun () -> shape.CreateUninitialized()

    | _ -> failwithf "Type '%O' does not support empty values." typeof<'T>

and private cache : TypeCache = new TypeCache()

/// Generates a structural empty value for given type
let empty<'T> = mkEmptyFunc<'T> () ()