#nowarn "77"
namespace Equinox.Cosmos

open System
open System.Diagnostics

[<AutoOpen>]
module Prelude =

    /// Fixed point combinator.
    /// Passes a function to itself.
    let rec fix f a = f (fix f) a

    /// Transforms a function by flipping the order of its arguments.
    let flip f a b = f b a

    /// Transforms a function by flipping the order of its arguments.
    let flip3 f a b c = f c a b

    /// Transforms a function by flipping the order of its arguments.
    let flip4 f a b c d = f d a b c

    /// Transforms an uncurried function to a curried function.
    let curry f a b = f (a,b)

    /// Transforms an uncurried function to a triple-curried function.
    let curry3 f a b c = f (a, b, c)

    /// Transforms a curried function to a non-curried function.
    let uncurry f (a,b) = f a b

    /// Given a value, creates a function with one ignored argument which returns the value.
    let inline konst x _ = x

    /// Given a value, creates a function with two ignored arguments which returns the value.
    let inline konst2 x _ _ = x

    /// Creates a pair.
    let inline tuple2 a b = a,b

    /// Fail with an unexpected match failure.
    let failwithumf () =
        let stackTrace = StackTrace ()
        let frame = stackTrace.GetFrame 1
        let meth = frame.GetMethod ()
        let line = frame.GetFileLineNumber ()
        let fileName = frame.GetFileName ()
        failwithf "Unexpected match failure in '%s' on line %i in file %s." meth.Name line fileName

    /// Fails with a shrug.
    let inline undef<'T> : 'T = failwith "¯\_(ツ)_/¯"

    /// Shrug it off.
    let inline ``¯\_(ツ)_/¯`` x = ignore x

    /// Null-coalescing operator.
    let (|?) lhs rhs = match lhs with null -> rhs | _ -> lhs

    /// Implicit operator.
    let inline (!>) (x:^a) : ^b = ((^a or ^b) : (static member op_Implicit : ^a -> ^b) x)

    /// Explicit operator.
    let inline (!!) (a:^a) : ^b = ((^a or ^b) : (static member op_Explicit : ^a -> ^b) a)

    /// Tuple convenience operator.
    /// NOTE: I'm wondering if this is a waste of an operator since it's redundant with (,) and can be used for cooler
    /// things like metaprogramming? - BGE
    let inline (=>) a b = (a,b)

    /// Sequencing operator like Haskell's ($). Has better precedence than (<|) due to the
    /// first character used in the symbol.
    let (^) = (<|)

    let inline kvpair (a,b) = System.Collections.Generic.KeyValuePair<_,_>(a,b)

    /// Return the first argument after applying f to it. Useful for debugging chains of (|>)
    let inline tap f x = ignore (f x); x

    /// Split the input between the two argument functions and combine their output.
    /// See (***) in http://hackage.haskell.org/package/base-4.7.0.2/docs/Control-Arrow.html
    let inline fsplit (f:'a -> 'x) (g:'b -> 'y) : 'a * 'b -> 'x * 'y =
      fun (a,b) -> (f a),(g b)

    /// Split the input between the two argument functions and combine their output.
    let ( *** ) = fsplit

    /// Determines whether the argument is a null reference.
    let inline isNull a = obj.ReferenceEquals(null, a)

    /// Determines whether the argument is not a null reference.
    let inline isNotNull a = not <| obj.ReferenceEquals(null, a)

    /// defaultArg, but with the arguments in the correct order
    let inline defaultArgCorrect a b = defaultArg b a




    // ------------------------------------------------------------------------------------------------------------------
    // tuple

    module Tuple =

        let flattenL2 a = let (a,b),c = a in a,b,c
        let flattenL3 a = let ((a,b),c),d = a in a,b,c,d
        let flattenL4 a = let (((a,b),c),d),e = a in a,b,c,d,e
        let flattenL5 a = let ((((a,b),c),d),e),f = a in a,b,c,d,e,f
        let flattenL6 a = let (((((a,b),c),d),e),f),g = a in a,b,c,d,e,f,g
        let flattenL7 a = let ((((((a,b),c),d),e),f),g),h = a in a,b,c,d,e,f,g,h

    /// A curried, first-class form of (,).
    let inline pair a b = a,b

    /// A curried, first-class form of (,,).
    let inline triple a b c = a,b,c

    /// Duplicate a value into a pair (diagonal morphism).
    let inline diag a = a,a

    /// Maps over the first item of a tuple.
    let inline mapFst f (a,b) = (f a, b)

    /// Maps over the second item of a tuple.
    let inline mapSnd f (a,b) = (a, f b)

    /// Maps over individual items of a pair.
    let inline mapPair f g (a,b) = (f a, g b)

    /// Maps over the first item of a tuple.
    let inline mapFst3 f (a,b,c) = (f a, b, c)

    /// Maps over the first item of a tuple.
    let inline mapSnd3 f (a,b,c) = (a, f b, c)

    /// Maps over the first item of a tuple.
    let inline mapThd3 f (a,b,c) = (a, b, f c)

    /// Maps over the first item of a tuple.
    let inline mapTriple f g h (a,b,c) = (f a, g b, h c)

    // ------------------------------------------------------------------------------------------------------------------


    /// Helpers for converting functions to System.Func<T,..> delegates
    module ToDelegate =

        open System

        let inline f0 f = new Func<_>(f)
        let inline f1 f = new Func<_,_>(f)
        let inline f2 f = new Func<_,_,_>(f)
        let inline f3 f = new Func<_,_,_,_>(f)
        let inline f4 f = new Func<_,_,_,_,_>(f)

        let inline action a = new Action(a)
        let inline action1 a = new Action<_>(a)
        let inline action2 a = new Action<_,_>(a)
        let inline action3 a = new Action<_,_,_>(a)
        let inline action4 a = new Action<_,_,_,_>(a)
        let inline action5 a = new Action<_,_,_,_,_>(a)

    // ------------------------------------------------------------------------------------------------------------------
    // operations values that could be null

    module Null =

        /// Returns the same value if not null, or the default value otherwise
        let inline getValueOr (defaultValue : 'T) (input : 'T) =
            match input with
            | null -> defaultValue
            | _ -> input


    // ------------------------------------------------------------------------------------------------------------------
    // operations on the option type

    module Option =

        /// An empty nullable value of a type designated by use.
        let emptyNullable = new Nullable<_>()

        /// Converts an option to a corresponding nullable.
        let inline toNullable opt = Option.fold (fun _ v -> new Nullable<_>(v)) emptyNullable opt

        let inline asNullable value = new Nullable<_>(value)

        let ofNullable (value:Nullable<_>) =
            if value.HasValue then Some value.Value
            else None

        /// Maps null references returned as proper (non-nullable) F# values by Linq Query Expressions
        /// http://stackoverflow.com/a/26008852
        let ofNull (value:'a) =
            if obj.ReferenceEquals(value, null) then None
            else Some value

        [<Obsolete("Use Option.getValueOr")>]
        let inline isNull defaultValue = function Some v -> v | None -> defaultValue

        /// Given a default value and an option, returns the option value if there else the default value.
        let inline getValueOr defaultValue = function Some v -> v | None -> defaultValue

        /// Given a default value and an option, returns the option value if there else the default value.
        let inline isNullLazy defaultValue = function Some v -> v | None -> defaultValue()

        [<Obsolete("This function is unsafe for reference values. Use Option.valueOrDefault for value types and Option.getValueOr [defaultValue] for reference types")>]
        let inline getValueOrDefault (o:'a option) = getValueOr (Unchecked.defaultof<'a>) o

        /// Gets the Some value or Unchecked.defaultof<'a> for value types.
        let inline valueOrDefault (o:'a option when 'a : struct) = getValueOr Unchecked.defaultof<'a> o

        let inline (|?) value defaultValue = getValueOr value defaultValue

        type OptionBuilder() =
            member __.Return(value) = Some(value)
            member __.ReturnFrom(opt:'T option) = opt
            member __.Zero() = None
            member __.Delay(f:unit -> 'a option) = f()
            member __.Bind(opt, f) = Option.bind f opt
            member this.TryWith(opt, h) =
                try this.ReturnFrom(opt)
                with e -> h e
            member this.TryFinally(opt, compensate) =
                try this.ReturnFrom(opt)
                finally compensate()
            member this.Using(res:#IDisposable, body) =
                this.TryFinally(body res, fun () -> match res with null -> () | disp -> disp.Dispose())

        let inline join (a:'a option option) : 'a option = a |> Option.bind id

        /// Merges two options into one by returning the first if it is Some otherwise returning the second.
        /// This operation is not commutative but is associative.
        let merge (a:'a option) (b:'a option) : 'a option =
            match a with
            | Some _ -> a
            | None -> b

        /// If `a` is Some, returns `a`. Otherwise, returns `defaultValue`. This is like merge, but with parameters reversed
        let inline someOr defaultValue a =
          merge a defaultValue

        /// Folds an option by applying f to Some otherwise returning the default value.
        let inline foldOr (f:'a -> 'b) (defaultValue:'b) = function Some a -> f a | None -> defaultValue

        /// Folds an option by applying f to Some otherwise returning the default value.
        let inline foldOrLazy (f:'a -> 'b) (defaultValue:unit -> 'b) = function Some a -> f a | None -> defaultValue()

        /// Return Some (f ()) if f doesn't raise. None otherwise
        let inline tryWith (f : unit -> 'a) = try () |> f|> Some with _ -> None

        /// bind to an Async<option>
        let inline bindAsync (f:'a -> Async<'b option>) (a:'a option) : Async<'b option> =
            a |> foldOr f (async.Return None)

        /// map to an Async<option>
        let inline mapAsync (f: 'a -> Async<'b>) =
          function | Some a -> async.Bind(f a, Some >> async.Return) | None -> async.Return None

        /// Converts a pair of a value and an optional value into an optional value containing a pair.
        [<Obsolete("use liftFst instead")>]
        let strength = function
          | a, Some b -> Some (a,b)
          | a, None -> None

        /// Lifts the first element of a pair into an option based on the value of the second element option.
        let liftFst = function
          | a, Some b -> Some (a,b)
          | _, None -> None

        /// Lifts the second element of a pair into an option based on the value of the first element option.
        let liftSnd = function
          | Some a, b -> Some (a,b)
          | None, _ -> None

        /// Applies a function to values contained in both options when both are Some.
        let map2 f a b =
          match a, b with
          | Some a, Some b -> f a b |> Some
          | _ -> None

        /// When "Some a" returns "Choice1Of2 a" and "Choice2Of2 e" otherwise.
        let successOr (e:'e) (o:'a option) : Choice<'a, 'e> =
          match o with
          | Some a -> Choice1Of2 a
          | None -> Choice2Of2 e


    /// The maybe monad workflow builder.
    let maybe = new Option.OptionBuilder()

    // ------------------------------------------------------------------------------------------------------------------



    // ------------------------------------------------------------------------------------------------------------------

    /// Active pattern for matching a Choice<'a, 'b> with Choice1Of2 = Success and Choice2Of2 = Failure
    let inline (|Success|Failure|) c = c

    /// Indicates success as Choice1Of2
    let inline Success v = Choice1Of2 v

    /// Indicates failure as Choice2Of2
    let inline Failure e = Choice2Of2 e

    // ------------------------------------------------------------------------------------------------------------------

    type Result<'a, 'b> = Choice<'a, 'b>

    /// Active pattern for matching a Choice<'a, 'b> with Choice1Of2 = Ok and Choice2Of2 = Error
    let inline (|Ok|Error|) c = c

    /// Indicates ok as Choice1Of2
    let inline Ok v : Result<'a, 'e> = Choice1Of2 v

    /// Indicates error as Choice2Of2
    let inline Error e : Result<'a, 'e> = Choice2Of2 e


    // ------------------------------------------------------------------------

    type MergeChoice<'left, 'right> = Choice<'left * 'right, 'left, 'right>

    let inline (|MergeBoth|MergeLeft|MergeRight|) mc = mc

    let inline MergeBoth (left, right) = Choice1Of3(left, right)

    let inline MergeLeft left = Choice2Of3(left)

    let inline MergeRight right = Choice3Of3(right)


    // ------------------------------------------------------------------------------------------------------------------
    // string parsers

    open System.Globalization      

    let private tryParse parser input =
        let parsed,value = parser input
        if parsed then Some value
        else None

    type Int32 with static member parse = tryParse (fun x -> Int32.TryParse (x, NumberStyles.Any, CultureInfo.InvariantCulture))

    type Int64 with static member parse = tryParse (fun x -> Int64.TryParse (x, NumberStyles.Any, CultureInfo.InvariantCulture))

    type Boolean with static member parse = tryParse Boolean.TryParse

    type Decimal with

        static member parse = tryParse (fun x -> Decimal.TryParse (x, NumberStyles.Any, CultureInfo.InvariantCulture))

        static member parseMoney (s:string) =
            if isNull s then None
            else tryParse (fun x -> Decimal.TryParse (x, NumberStyles.Any, CultureInfo.InvariantCulture)) (s.TrimStart('$'))

    type Single with static member parse = tryParse (fun x -> Single.TryParse (x, NumberStyles.Any, CultureInfo.InvariantCulture))

    type DateTime with static member parse = tryParse (fun x -> DateTime.TryParse (x, DateTimeFormatInfo.InvariantInfo, DateTimeStyles.None))

    type DateTimeOffset with static member parse = tryParse (fun x -> DateTimeOffset.TryParse (x, DateTimeFormatInfo.InvariantInfo, DateTimeStyles.None))

    type Double with static member parse = tryParse (fun x -> Double.TryParse (x, NumberStyles.Any, CultureInfo.InvariantCulture))

    let (|Int32|_|) = Int32.parse

    let (|Boolean|_|) = Boolean.parse

    let (|Decimal|_|) = Decimal.parse

    let (|DateTime|_|) = DateTime.parse

    let (|DateTimeOffset|_|) = DateTimeOffset.parse

    // ------------------------------------------------------------------------------------------------------------------



    // ------------------------------------------------------------------------------------------------------------------
    // Dictionary<'a, 'b>

    open System.Collections.Generic

    /// Basic operations on dictionaries.
    module Dict =

        open System.Collections.Generic
        open System.Collections.Concurrent

        let private outToOption (has,value) = if has then Some value else None

        let tryGetValue key (dict:IDictionary<_,_>) = dict.TryGetValue(key) |> outToOption

        let put key value (dict:ConcurrentDictionary<_,_>) = dict.AddOrUpdate(key, value, fun _ _ -> value) |> ignore


    /// Creates a Dictionary<'a, 'b> with the specified comparer.
    let inline dictc (c:IEqualityComparer<'a>) (kvps:seq<'a * 'b>) =
      let d = new Dictionary<'a, 'b>(c)
      kvps |> Seq.iter (fun (k,v) -> d.[k] <- v)
      d

    /// Creates a Dictionary<string, 'a> with the StringComparer.OrdinalIgnoreCase comparer.
    let inline dictIgnoreCase kvps =
      dictc StringComparer.OrdinalIgnoreCase kvps

    // ------------------------------------------------------------------------------------------------------------------

    let cache (f:'a -> 'b) =
        let f = new Func<_,_>(f)
        let cache = new System.Collections.Concurrent.ConcurrentDictionary<'a, 'b>()
        fun k -> cache.GetOrAdd(k, f)


module Boolean =

    let map (f:bool -> 'a) theBool = f theBool

    let map2 (t:'a) (f:'a) theBool = if theBool then t else f



module Text =

    open System.Text

    let UTF8NoBOM = new UTF8Encoding(false, true)


// ------------------------------------------------------------------------------------------------------------------
/// Operations on array segments.
module ArraySegment =

    open System.IO

    let inline empty<'a> = new ArraySegment<'a>()

    let inline ofBytes (arr:byte[]) = new ArraySegment<_>(arr)

    /// Exposes the underlying buffer as an array segment sized to the appropriate length.
    /// Note if the memory stream is mutated after this operation, the underlying array will change.
    let inline ofMemoryStream (ms:MemoryStream) = new ArraySegment<_>(ms.GetBuffer(), 0, ms.Length |> int)

    /// Initializes a memory stream with the range represented by the array segment.
    let inline asMemoryStream (arr:ArraySegment<byte>) = new MemoryStream(arr.Array, arr.Offset, arr.Count)

    /// Widens the segment by the specified length.
    let inline widen length (arr:ArraySegment<_>) = new ArraySegment<_>(arr.Array, arr.Offset, arr.Count + length)

    /// Assigns a value to the position in the array immeditatly after the segment and expands the segment.
    /// Note that the underlying array has to have sufficient space.
    let inline add item (arr:ArraySegment<_>) =
        arr.Array.[arr.Count] <- item
        arr |> widen 1

    /// Copies the segment to a new array.
    let inline toArray (arr:ArraySegment<'a>) =
        let a : 'a[] = Array.zeroCreate arr.Count
        Array.Copy(arr.Array, arr.Offset, a, 0, arr.Count)
        a


[<AutoOpen>]
module ArraySegmentExtensions =

    open System.Text
    open System.IO

    type Encoding with
        member x.GetString(data:ArraySegment<byte>) = x.GetString(data.Array, data.Offset, data.Count)

// ------------------------------------------------------------------------------------------------------------------




// ------------------------------------------------------------------------------------------------------------------
// interlocked extensions

[<AutoOpen>]
module InterlockedExtensions =

    open System.Threading

    type Interlocked with

        /// Assigns the specified newValue to the location if it is greater than the exisiting value.
        /// Returns true if the new value was assigned and false otherwise.
        static member InterlockedExchangeIfGreaterThan (location:ref<int>, comparison:int, newValue:int) =
            let mutable initialValue = !location
            if initialValue >= comparison then false
            else
                let mutable flag = true
                while (Interlocked.CompareExchange(location, newValue, initialValue) <> initialValue && flag) do
                    initialValue <- !location
                    if initialValue >= comparison then
                        flag <- false
                flag

        /// Assigns the specified newValue to the location if it is less than the exisiting value.
        /// Returns true if the new value was assigned and false otherwise.
        static member InterlockedExchangeIfLessThan(location:ref<int>, comparison:int, newValue:int) =
            let mutable initialValue = !location
            if initialValue <= comparison then false
            else
                let mutable flag = true
                while (Interlocked.CompareExchange(location, newValue, initialValue) <> initialValue && flag) do
                    initialValue <- !location
                    if initialValue <= comparison then
                        flag <- false
                flag

        /// Assigns the specified newValue to the location if it is greater than the exisiting value.
        /// Returns true if the new value was assigned and false otherwise.
        static member InterlockedExchangeIfGreaterThan(location:ref<int64>, comparison:int64, newValue:int64) =
            let mutable initialValue = !location
            if initialValue >= comparison then false
            else
                let mutable flag = true
                while (Interlocked.CompareExchange(location, newValue, initialValue) <> initialValue && flag) do
                    initialValue <- !location
                    if initialValue >= comparison then
                        flag <- false
                flag

// ------------------------------------------------------------------------------------------------------------------


// ------------------------------------------------------------------------------------------------------------------

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Result =

    // BEGIN: The first three functions are from F# 4.1. Will need conditional compilation.

    [<CompiledName("Map")>]
    let map (mapping: 'a -> 'b) (result: Result<'a, 'e>) : Result<'b, 'e> =
        match result with
        | Ok x -> Ok (mapping x)
        | Error e -> Error e

    [<CompiledName("MapError")>]
    let mapError (mapping: 'e -> 'f) (result: Result<'a, 'e>) : Result<'a, 'f> =
        match result with
        | Ok x -> Ok x
        | Error e -> Error (mapping e)

    [<CompiledName("Bind")>]
    let bind (binder: 'a -> Result<'b, 'e>) (result: Result<'a, 'e>) : Result<'b, 'e> =
        match result with
        | Ok x -> binder x
        | Error e -> Error e

    // END: The first three functions are from F# 4.1. Will need conditional compilation.

    [<CompiledName("MapAsync")>]
    let mapAsync (mapping: 'a -> Async<'b>) (result: Result<'a, 'e>) : Async<Result<'b, 'e>> =
        match result with
        | Ok x -> async.Bind(mapping x, Ok >> async.Return)
        | Error e -> async.Return (Error e)

    [<CompiledName("BindAsync")>]
    let bindAsync (binder: 'a -> Async<Result<'b, 'e>>) (result: Result<'a, 'e>) : Async<Result<'b, 'e>> =
        match result with
        | Ok x -> binder x
        | Error e -> async.Return (Error e)

    [<CompiledName("ZipWith")>]
    let zipWith (f: 'e -> 'e -> 'e) (x: Result<'a, 'e>) (y: Result<'b, 'e>) : Result<('a * 'b), 'e> =
        match x, y with
        | Ok a, Ok b -> Ok (a, b)
        | Ok _, Error e -> Error e
        | Error e, Ok _ -> Error e
        | Error e1, Error e2 -> Error (f e1 e2)

    let fold (f:'a -> 'b) (g:'e -> 'b) (r:Result<'a, 'e>) : 'b =
      match r with
      | Ok a -> f a
      | Error e -> g e

    /// Returns a success value or throws an exception.
    let getOrThrow (r:Result<'a, 'e>) : 'a =
      match r with
      | Success a -> a
      | Failure e -> failwithf "%O" e

    /// Returns the success value, if success.
    let trySuccess (r:Result<'a, _>) : 'a option =
      match r with
      | Success a -> Some a
      | _ -> None

    /// Returns the failure value, if failure.    
    let tryFailure (r:Result<_, 'e>) : 'e option =
      match r with
      | Failure e -> Some e
      | _ -> None

// ------------------------------------------------------------------------------------------------------------------
/// Operations on the choice type.
module Choice =

    let returnM : 'a -> Choice<'a, 'b> = Choice1Of2

    /// Maps over the left result type.
    let mapl (f:'a -> 'b) = function
      | Choice1Of2 a -> f a |> Choice1Of2
      | Choice2Of2 e -> Choice2Of2 e

    /// Maps over the right result type.
    let mapr (f:'b -> 'c) = function
      | Choice1Of2 a -> Choice1Of2 a
      | Choice2Of2 e -> f e |> Choice2Of2

    /// Maps over the left result type.
    [<Obsolete("Use Result.map")>]
    let map (f:'a -> 'b) = function
      | Choice1Of2 a -> f a |> Choice1Of2
      | Choice2Of2 e -> Choice2Of2 e

    /// Maps over the left or the right result type.
    let bimap (f1:'a -> 'c) (f2:'b -> 'd) = function
      | Choice1Of2 x -> Choice1Of2 (f1 x)
      | Choice2Of2 x -> Choice2Of2 (f2 x)

    /// Folds a choice pair with functions for each case.
    let fold (f1:'a -> 'c) (f2:'b -> 'c) = function
      | Choice1Of2 x -> f1 x
      | Choice2Of2 x -> f2 x

    /// Extracts the value from a choice with the same type on the left as the right.
    /// (Also known as the codiagonal morphism).
    let codiag<'a> : Choice<'a, 'a> -> 'a =
      fold id id

    /// Binds a function to a choice where the right case represents failure to be propagated.
    [<Obsolete("Use Result.bind")>]
    let bind (f:'a -> Choice<'b, 'e>) = function
      | Choice1Of2 a -> f a
      | Choice2Of2 e -> Choice2Of2 e

    /// evaluate f () and return either the result of the evaluation or the exception
    let tryWith f = try Choice1Of2 (f ()) with exn -> Choice2Of2 exn

    /// Return Some v if is Success v. Otherwise return None.
    let toOption = function
        | Choice1Of2 a -> Some a
        | Choice2Of2 _ -> None

    /// Return Some v if is Failure v. Otherwise return None.
    let toOptionInverse = function
        | Choice1Of2 _ -> None
        | Choice2Of2 b -> Some b

     /// Return Some v if either is Success v. Otherwise return None.
    let isSuccess = function
        | Choice1Of2 a -> true
        | Choice2Of2 _ -> false



    let ofOption error = Option.foldOr Choice1Of2 (Choice2Of2 error)

    let filter (f: 'a -> bool) error = function
        | Choice1Of2 a ->
            if f a then Choice1Of2 a
            else Choice2Of2 error
        | Choice2Of2 error -> Choice2Of2 error

    /// Merges two choices which can potentially contain errors.
    /// When both choice values are errors, they are concatenated using ';'.
    let mergeErrs = function
      | Choice1Of2 (), Choice1Of2 () -> Choice1Of2 ()
      | Choice2Of2 e, Choice1Of2 _   -> Choice2Of2 e
      | Choice1Of2 _, Choice2Of2 e   -> Choice2Of2 e
      | Choice2Of2 e1, Choice2Of2 e2 -> Choice2Of2 (String.concat ";" [e1;e2])

    let errorsOfFirst (xs: Choice<unit, string> seq) =
        let x0: Choice<unit, string> = Success ()
        xs |> Seq.fold (fun x y ->
            match x, y with
            | Success x, Success y -> Success ()
            | Success x, Failure y -> Failure y
            | Failure x, Success y -> Failure x
            | Failure x, Failure y -> Failure x
        ) x0

    type ChoiceBuilder() =
        member __.Return(value) = returnM value
        member __.ReturnFrom(c:Choice<'a, 'e>) = c
        member __.Delay(f:unit -> Choice<'a, 'e>) = f()
        member __.Bind(c, f) = Result.bind f c
        member this.TryWith(opt, h) =
            try this.ReturnFrom(opt)
            with e -> h e
        member this.TryFinally(opt, compensate) =
            try this.ReturnFrom(opt)
            finally compensate()
        member this.Using(res:#IDisposable, body) =
            this.TryFinally(body res, fun () -> match res with null -> () | disp -> disp.Dispose())

[<AutoOpen>]
module ChoiceEx =

    let choice = new Choice.ChoiceBuilder()

// ------------------------------------------------------------------------------------------------------------------





// ------------------------------------------------------------------------------------------------------------------
/// A predicate.
type Pred<'a> = 'a -> bool

/// Operations on predicates.
module Pred =

  /// Always returns true.
  let True<'a> : Pred<'a> = konst true

  /// Always returns false.
  let False<'a> : Pred<'a> = konst false

  /// Creates a predicate which returns the logical AND of the argument predicates.
  let And (p1:Pred<'a>) (p2:Pred<'a>) : Pred<'a> =
    fun a -> p1 a && p2 a

  /// Creates a predicate which returns the logical OR of the argument predicates.
  let Or (p1:Pred<'a>) (p2:Pred<'a>) : Pred<'a> =
    fun a -> p1 a || p2 a

  /// Negates a predicate.
  let Not (p1:Pred<'a>) : Pred<'a> =
    p1 >> not

  /// Creates a predicate that checks all provided predicates.
  let All (ps:Pred<'a> seq) : Pred<'a> =
    fun a -> ps |> Seq.forall ((|>) a)

  /// Creates a predicate that checks whether any argument prediate returns true.
  let Any (ps:Pred<'a> seq) : Pred<'a> =
    fun a -> ps |> Seq.exists ((|>) a)

  module Infix =

    let inline (&&&) p1 p2 = And p1 p2

    let inline (|||) p1 p2 = Or p1 p2

// ------------------------------------------------------------------------------------------------------------------

[<Struct; NoComparison; NoEquality>]
type StructBox<'T when 'T : equality> (value:'T) =
  member x.Value = value
  static member Comparer =
    let gcomparer = HashIdentity.Structural<'T>
    {
      new System.Collections.Generic.IEqualityComparer<StructBox<'T>> with
        member __.GetHashCode(v) = gcomparer.GetHashCode(v.Value)
        member __.Equals(v1,v2) = gcomparer.Equals(v1.Value,v2.Value)
    }

// ------------------------------------------------------------------------------------------------------------------

[<AutoOpen>]
module GCExtensions =

    type GC with

        static member CollectionCounts () =
            String.Join(", ", seq { for i in 0 .. GC.MaxGeneration -> sprintf "gen%d: %d" i (GC.CollectionCount(i)) })

// ------------------------------------------------------------------------------------------------------------------

[<AutoOpen>]
module FSharpValueExtensions =

    open Microsoft.FSharp.Reflection

    type FSharpValue with

        static member MakeRecordWith (record: 'a, values: Map<string, obj>) =
            let recordType = typeof<'a>
            let boxedRecord = box record
            let values =
                FSharpType.GetRecordFields(recordType)
                |> Array.map (fun p ->
                    match Map.tryFind p.Name values with
                    | Some v ->
                        v
                    | None ->
                        p.GetValue(boxedRecord)
                )
            FSharpValue.MakeRecord(recordType, values) :?> 'a

// ------------------------------------------------------------------------------------------------------------------

[<AutoOpen>]
module DateTimeOffsetExtensions =

    let [<Literal>] private UnixEpochMilliseconds = 62135596800000L
    let [<Literal>] private UnixEpochSeconds = 62135596800L
    let [<Literal>] private UnixEpochTicks = 621355968000000000L

    type DateTimeOffset with

        // This is included in .NET 4.6 and above.
        static member FromUnixTimeMilliseconds milliseconds =
            if milliseconds < -62135596800000L || milliseconds > 253402300799999L then
                raise (new ArgumentOutOfRangeException("milliseconds", "Valid values are between -62135596800000 and 253402300799999, inclusive."))
            new DateTimeOffset(milliseconds * TimeSpan.TicksPerMillisecond + UnixEpochTicks, TimeSpan.Zero)

        // This is included in .NET 4.6 and above.
        static member FromUnixTimeSeconds seconds =
            if seconds < -62135596800L || seconds > 253402300799L then
                raise (new ArgumentOutOfRangeException("seconds", "Valid values are between -62135596800 and 253402300799, inclusive."))
            new DateTimeOffset(seconds * TimeSpan.TicksPerSecond + UnixEpochTicks, TimeSpan.Zero)

        // This is included in .NET 4.6 and above.
        member this.ToUnixTimeMilliseconds () =
            (this.UtcDateTime.Ticks / TimeSpan.TicksPerMillisecond) - UnixEpochMilliseconds

        // This is included in .NET 4.6 and above.
        member this.ToUnixTimeSeconds () =
            (this.UtcDateTime.Ticks / TimeSpan.TicksPerSecond) - UnixEpochSeconds

// ------------------------------------------------------------------------------------------------------------------

[<AutoOpen>]
module TaskExtensions =

    open System.Threading.Tasks

    type Task with

        static member ToGenericWithUnit (t: Task) =
            let unitFunc = Func<Task, unit>(konst ())
            t.ContinueWith unitFunc

// ------------------------------------------------------------------------------------------------------------------

[<AutoOpen>]
module IPExtensions =

    open System.Net
    open System.Net.Sockets

    type IPAddress with

(*
   Address Block             Present Use                       Reference
   ---------------------------------------------------------------------
   0.0.0.0/8            "This" Network                 [RFC1700, page 4]
   10.0.0.0/8           Private-Use Networks                   [RFC1918]
   14.0.0.0/8           Public-Data Networks         [RFC1700, page 181]
   24.0.0.0/8           Cable Television Networks                    --
   39.0.0.0/8           Reserved but subject
                           to allocation                       [RFC1797]
   127.0.0.0/8          Loopback                       [RFC1700, page 5]
   128.0.0.0/16         Reserved but subject
                           to allocation                             --
   169.254.0.0/16       Link Local                                   --
   172.16.0.0/12        Private-Use Networks                   [RFC1918]
   191.255.0.0/16       Reserved but subject
                           to allocation                             --
   192.0.0.0/24         Reserved but subject
                           to allocation                             --
   192.0.2.0/24         Test-Net
   192.88.99.0/24       6to4 Relay Anycast                     [RFC3068]
   192.168.0.0/16       Private-Use Networks                   [RFC1918]
   198.18.0.0/15        Network Interconnect
                           Device Benchmark Testing            [RFC2544]
   223.255.255.0/24     Reserved but subject
                           to allocation                             --
   224.0.0.0/4          Multicast                              [RFC3171]
   240.0.0.0/4          Reserved for Future Use        [RFC1700, page 4]
*)
        static member IsRfc3330SpecialUse (ip: IPAddress) =
            if isNull ip then
                false
            else
                match ip.AddressFamily with
                | AddressFamily.InterNetwork ->
                    let bytes = ip.GetAddressBytes()
                    let byte0 = bytes.[0]
                    let byte1 = bytes.[1]
                    let byte2 = bytes.[2]
                    byte0 = 0uy
                    || byte0 = 10uy
                    || byte0 = 14uy
                    || byte0 = 24uy
                    || byte0 = 39uy
                    || byte0 = 127uy
                    || (byte0 = 128uy && byte1 = 0uy)
                    || (byte0 = 169uy && byte1 = 254uy)
                    || (byte0 = 172uy && (byte1 >>> 4) = 1uy) // 1uy = 16uy >>> 4
                    || (byte0 = 191uy && byte1 = 255uy)
                    || (byte0 = 192uy && byte1 = 0uy && byte2 = 0uy)
                    || (byte0 = 192uy && byte1 = 0uy && byte2 = 2uy)
                    || (byte0 = 192uy && byte1 = 88uy && byte2 = 99uy)
                    || (byte0 = 192uy && byte1 = 168uy)
                    || (byte0 = 198uy && (byte1 >>> 1) = 9uy) // 9uy = 18uy >>> 1
                    || (byte0 = 223uy && byte1 = 255uy && byte2 = 255uy)
                    || (byte0 >>> 4) = 14uy // 14uy = 224uy >>> 4
                    || (byte0 >>> 4) = 15uy // 15uy = 240uy >>> 4
                | _ ->
                    false

        static member ParseWithPort (ipString: string) =
            // The reason for first parsing into Uri is to account for the case in which a port number is appended to rawIp for IPv4. See http://stackoverflow.com/questions/4968795/ipaddress-parse-using-port-on-ipv4
            let url = ref Unchecked.defaultof<Uri>
            let result = ref Unchecked.defaultof<IPAddress>
            let tryIp =
                match Uri.TryCreate(String.Concat("http://", ipString), UriKind.Absolute, url) with
                | true -> (!url).Host // IPv4?
                | false -> ipString // IPv6?
            match IPAddress.TryParse(tryIp, result) with
            | true -> !result
            | false -> null

// ------------------------------------------------------------------------------------------------------------------


[<AutoOpen>]
module ModuleAttributes =
  // Never type this out again! F# 4.1 will remove the need altogether.
  // Now just write [<Mod(Suffixed)>] instead and remove it when you
  // upgrade to 4.1
  type ModAttribute = CompilationRepresentationAttribute

  [<Literal>]
  let Suffixed = CompilationRepresentationFlags.ModuleSuffix


/// Operations on ExceptionDispatchInfo.
[<RequireQualifiedAccessAttribute>]
[<CompilationRepresentationAttribute(CompilationRepresentationFlags.ModuleSuffix)>]
module ExceptionDispatchInfo =

  open System.Runtime.ExceptionServices

  /// Throws the captured exception using ExceptionDispatchInfo.Throw.
  let inline throw (edi:ExceptionDispatchInfo) =
    edi.Throw ()
    failwith "unreachable"

  /// Captures and re-throws an exception using ExceptionDispatchInfo.
  let inline captureThrow (ex:exn) =
    let edi = ExceptionDispatchInfo.Capture ex in
    throw edi
