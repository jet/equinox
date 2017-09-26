module Foldunk.Tests.Infrastructure.Empty

open System
open System.Collections.Generic
open TypeShape
open TypeShape_Utils

let rec private mkEmptyFunc<'T> () : bool -> 'T =
    let mutable f = Unchecked.defaultof<bool -> 'T>
    if cache.TryGetValue(&f) then f
    else
        use mgr = cache.CreateRecTypeManager()
        mkEmptyFuncCached<'T> mgr

and private mkEmptyFuncCached<'T> (ctx : RecTypeManager) : bool -> 'T =
    match ctx.TryFind<bool -> 'T>() with
    | Some f -> f
    | None ->
        let _ = ctx.CreateUninitialized<bool -> 'T>(fun c m -> c.Value m)
        let f = mkEmptyFuncAux<'T> ctx
        ctx.Complete f

and private mkEmptyFuncAux<'T> (ctx : RecTypeManager) : bool -> 'T =
    let wrap (f : bool -> 'a) = unbox<bool -> 'T> f

    let mkMemberInitializer (shape : IShapeWriteMember<'DeclaringType>) =
        shape.Accept { new IWriteMemberVisitor<'DeclaringType, bool -> 'DeclaringType -> 'DeclaringType> with
            member __.Visit (shape : ShapeWriteMember<'DeclaringType, 'Field>) =
                let fe = mkEmptyFuncCached<'Field> ctx
                fun m inst -> shape.Inject inst (fe m)
        }

    match shapeof<'T> with
    | Shape.Primitive -> fun _ -> Unchecked.defaultof<'T>
    | Shape.Decimal -> wrap(fun _ -> 0M)
    | Shape.BigInt -> wrap(fun _ -> 0I)
    | Shape.Guid -> wrap(fun _ -> Guid.Empty)
    | Shape.String -> wrap(fun _ -> "")
    | Shape.TimeSpan -> wrap(fun _ -> TimeSpan.Zero)
    | Shape.DateTime -> wrap(fun _ -> DateTime.MinValue)
    | Shape.DateTimeOffset -> wrap(fun _ -> DateTimeOffset.MinValue)
    | Shape.Unit -> wrap (fun _ -> ())
    | Shape.Enum s ->
        s.Accept { new IEnumVisitor<bool -> 'T> with
            member __.Visit<'t, 'u when 't : enum<'u>>() = // 'T = 't
                let ue = mkEmptyFuncCached<'u> ctx
                wrap(fun m -> LanguagePrimitives.EnumOfValue<'u,'t>(ue m)) }

    | Shape.Nullable s ->
        s.Accept { new INullableVisitor<bool -> 'T> with
            member __.Visit<'t when 't : struct and 't :> ValueType and 't : (new : unit -> 't)>() = // 'T = 't
                let em = mkEmptyFuncCached<'t> ctx
                wrap(fun max -> if max then Nullable(em true) else Nullable()) }

    | Shape.FSharpFunc s ->
        // empty<'T -> 'S> = fun (_ : 'T) -> empty<'S>
        s.Accept { new IFSharpFuncVisitor<bool -> 'T> with
            member __.Visit<'Dom, 'Cod> () = // 'T = 'Cod -> 'Dom
                let de = mkEmptyFuncAux<'Cod> ctx
                wrap(fun max -> fun (_ : 'Dom) -> de max) }

    | Shape.FSharpOption s ->
        s.Accept { new IFSharpOptionVisitor<bool -> 'T> with
            member __.Visit<'t>() = // 'T = 't option
                let et = mkEmptyFuncCached<'t> ctx
                wrap(fun max -> if max then Some (et max) else None) }

    | Shape.KeyValuePair s ->
        s.Accept { new IKeyValuePairVisitor<bool -> 'T> with
            member __.Visit<'k,'v>() = // 'T = KeyValuePair<'k,'v>
                let ke,ve = mkEmptyFuncCached<'k> ctx, mkEmptyFuncCached<'v> ctx
                wrap(fun max -> new KeyValuePair<'k,'v>(ke max,ve max)) }

    | Shape.FSharpList s ->
        s.Accept { new IFSharpListVisitor<bool -> 'T> with
            member __.Visit<'t>() = // 'T = 't list
                let te = mkEmptyFuncCached<'t> ctx
                wrap(fun max -> if max then [te max] else []) }

    | Shape.Array s ->
        s.Accept { new IArrayVisitor<bool -> 'T> with
            member __.Visit<'t> rank =
                let te = mkEmptyFuncCached<'t> ctx
                let inline sz m = if m then 1 else 0
                match rank with
                | 1 -> wrap(fun m -> let s = sz m in Array.init s (fun _ -> te m)) // 'T = 't []
                | 2 -> wrap(fun m -> let s = sz m in Array2D.init s s (fun _ _ -> te m)) // 'T = 't [,]
                | 3 -> wrap(fun m -> let s = sz m in Array3D.init s s s (fun _ _ _ -> te m)) // 'T = 't [,,]
                | 4 -> wrap(fun m -> let s = sz m in Array4D.init s s s s (fun _ _ _ _ -> te m)) // 'T = 't [,,,]
                | _ -> failwithf "Unsupported type %O" typeof<'T> }

    | Shape.FSharpSet s ->
        s.Accept { new IFSharpSetVisitor<bool -> 'T> with
            member __.Visit<'t when 't : comparison>() = // 'T = Set<'t>
                let te = mkEmptyFuncCached<'t> ctx
                wrap(fun max -> if max then set [|te max|] else Set.empty) }

    | Shape.FSharpMap s ->
        s.Accept { new IFSharpMapVisitor<bool -> 'T> with
            member __.Visit<'k, 'v when 'k : comparison>() = // 'T = Map<'k,'v>
                let ke, ve = mkEmptyFuncCached<'k> ctx, mkEmptyFuncCached<'v> ctx
                wrap(fun max ->
                        if max then Map.ofArray [|(ke max, ve max)|]
                        else Map.empty) }

    | Shape.Tuple (:? ShapeTuple<'T> as shape) ->
        let elemInitializers = shape.Elements |> Array.map mkMemberInitializer
        fun max ->
            let mutable inst = shape.CreateUninitialized()
            for f in elemInitializers do inst <- f max inst
            inst

    | Shape.FSharpRecord (:? ShapeFSharpRecord<'T> as shape) ->
        let fieldInitializers = shape.Fields |> Array.map mkMemberInitializer
        fun max ->
            let mutable inst = shape.CreateUninitialized()
            for f in fieldInitializers do inst <- f max inst
            inst

    | Shape.FSharpUnion (:? ShapeFSharpUnion<'T> as shape) ->
        let defaultUnionCase = shape.UnionCases |> Array.minBy (fun c -> c.Fields.Length)
        let fieldInitializers = defaultUnionCase.Fields |> Array.map mkMemberInitializer
        fun max ->
            let mutable inst = defaultUnionCase.CreateUninitialized()
            for f in fieldInitializers do inst <- f max inst
            inst

    | Shape.CliMutable (:? ShapeCliMutable<'T> as shape) ->
        let propInitializers = shape.Properties |> Array.map mkMemberInitializer
        fun max ->
            let mutable inst = shape.CreateUninitialized()
            if max then
                for p in propInitializers do inst <- p max inst
            inst

    | _ ->
        failwithf "Type '%O' does not support empty values." typeof<'T>

and private cache : TypeCache = new TypeCache()

/// Generates a structural empty value for given type
let empty<'T> = mkEmptyFunc<'T> () false

/// Generates a structural empty value that populates
/// variadic types with singletons
let notEmpty<'T> = mkEmptyFunc<'T> () true