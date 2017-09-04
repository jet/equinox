﻿module Foldunk.Serialization

open FSharp.Reflection
open Newtonsoft.Json
open Newtonsoft.Json.Converters
open Newtonsoft.Json.Linq
open Newtonsoft.Json.Serialization
open System
open System.Reflection
open System.Runtime.InteropServices

[<AutoOpen>]
module Prelude =
    /// Provides a thread-safe memoization wrapper for supplied function
    let memoize : ('T -> 'S) -> 'T -> 'S =
        fun f ->
            let cache = new System.Collections.Concurrent.ConcurrentDictionary<'T, 'S>()
            fun t -> cache.GetOrAdd(t, f)

[<AbstractClass>]
type JsonPickler<'T>() =
    inherit JsonConverter()

    static let isMatchingType =
        let rec isMatching (ts : Type list) =
            match ts with
            | [] -> false
            | t :: _ when t = typeof<'T> -> true
            | t :: tl ->
                let tail =
                    [ match t.BaseType with null -> () | bt -> yield bt
                      yield! t.GetInterfaces()
                      yield! tl ]
                
                isMatching tail

        memoize (fun t -> isMatching [t])

    abstract Write : writer:JsonWriter * serializer:JsonSerializer * source:'T  -> unit
    abstract Read : reader:JsonReader * serializer:JsonSerializer -> 'T

    override __.CanConvert t = isMatchingType t

    override __.CanRead = true
    override __.CanWrite = true

    override __.WriteJson(writer, source : obj, serialize : JsonSerializer) =
        __.Write(writer, serialize, source :?> 'T)

    override __.ReadJson(reader : JsonReader, _, _, serializer : JsonSerializer) =
        __.Read(reader, serializer) :> obj

/// Json Converter that serializes based on an isomorphic type
[<AbstractClass>]
type JsonIsomorphism<'T, 'U>(?targetPickler : JsonPickler<'U>) =
    inherit JsonPickler<'T>()

    abstract Pickle   : 'T -> 'U
    abstract UnPickle : 'U -> 'T

    override __.Write(writer:JsonWriter, serializer:JsonSerializer, source:'T) =
        let target = __.Pickle source
        match targetPickler with
        | None -> serializer.Serialize(writer, target, typeof<'U>)
        | Some p -> p.Write(writer, serializer, target)

    override __.Read(reader:JsonReader, serializer:JsonSerializer) =
        let target =
            match targetPickler with
            | None -> serializer.Deserialize<'U>(reader)
            | Some p -> p.Read(reader, serializer)

        __.UnPickle target

/// Used for converting heterogenous data source
/// c.f. http://stackoverflow.com/a/18997172/1670977
type ObjectArrayConverter<'T>() =
    inherit JsonConverter()
    override __.CanConvert t = t = typeof<'T []>
    override __.CanWrite = false
    override __.ReadJson(reader : JsonReader, _ : Type, _ : obj, _ : JsonSerializer) =
        let token = JToken.Load reader
        match token.Type with
        | JTokenType.Array -> token.ToObject<'T []>()
        | _ -> [|token.ToObject<'T>()|]
        :> obj

    override __.WriteJson (_,_,_) = raise <| new NotImplementedException()

/// For Some 1 generates "1", for None generates "null"
type OptionConverter() =    
    inherit JsonConverter()

    let getAndCacheUnionCases = FSharpType.GetUnionCases |> memoize

    override x.CanConvert(typ) = typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<option<_>>

    override x.WriteJson(writer, value, serializer) =
        let value = 
            if value = null then null
            else 
                let _,fields = FSharpValue.GetUnionFields(value, value.GetType())
                fields.[0]
        serializer.Serialize(writer, value)

    override x.ReadJson(reader, typ, existingValue, serializer) =
        let innerType = 
            let innerType = typ.GetGenericArguments().[0]
            if innerType.IsValueType then typedefof<Nullable<_>>.MakeGenericType([|innerType|])
            else innerType

        let cases = getAndCacheUnionCases typ
        if reader.TokenType = JsonToken.Null then FSharpValue.MakeUnion(cases.[0], Array.empty)
        else
            let value = serializer.Deserialize(reader, innerType)
            if value = null then FSharpValue.MakeUnion(cases.[0], Array.empty)
            else FSharpValue.MakeUnion(cases.[1], [|value|]) 

type GuidConverter() =
    inherit JsonIsomorphism<Guid, string>()
    override __.Pickle g = g.ToString "N"
    override __.UnPickle g = Guid.Parse g

type Newtonsoft private () =
    static let settings = Newtonsoft.GetDefaultSettings(indent = true)
    static let noIndentSettings = Newtonsoft.GetDefaultSettings(indent = false)

    /// <summary>
    ///     Gets the default serializer settings used by Batman Json serialization
    /// </summary>
    /// <param name="useHyphenatedGuids">Use hyphenation when serializing guids. Defaults to true.</param>
    /// <param name="indent">Use multi-line, indented formatting when serializing json; defaults to true.</param>
    static member GetDefaultSettings
        (   [<Optional;DefaultParameterValue(null)>]?useHyphenatedGuids : bool,
            [<Optional;DefaultParameterValue(null)>]?indent : bool,
            [<Optional;DefaultParameterValue(null)>]?camelCase : bool) =
        let useHyphenatedGuids = defaultArg useHyphenatedGuids true
        let formatting = if defaultArg indent true then Formatting.Indented else Formatting.None
        let resolver : IContractResolver =
             if defaultArg camelCase true then CamelCasePropertyNamesContractResolver() :> _
             else DefaultContractResolver() :> _
        let settings =
            JsonSerializerSettings(
                ContractResolver = resolver,
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                DateFormatHandling = DateFormatHandling.IsoDateFormat,
                Formatting = formatting,
                NullValueHandling = NullValueHandling.Ignore)

        settings.Converters.Add(StringEnumConverter())
        if not useHyphenatedGuids then settings.Converters.Add(GuidConverter())
        settings.Converters.Add(OptionConverter())
        settings

    /// <summary>
    ///     Serializes given value to a json string.
    /// </summary>
    /// <param name="value">Value to serialize.</param>
    /// <param name="indent">Use indentation when serializing json. Defaults to true.</param>
    static member Serialize<'T>
        (   value : 'T,
            [<Optional;DefaultParameterValue(null)>]?indent : bool) : string =
        let settings = if defaultArg indent true then settings else noIndentSettings
        JsonConvert.SerializeObject(value, settings)

    /// <summary>
    ///     Deserializes value of given type from json string.
    /// </summary>
    /// <param name="json">Json string to deserialize.</param>
    static member Deserialize<'T> (json : string) : 'T =
        JsonConvert.DeserializeObject<'T>(json, settings)

[<NoComparison; NoEquality>]
type private Union = 
    {
        cases: UnionCaseInfo[]
        tagReader: obj -> int
        fieldReader: (obj -> obj[])[]
        caseConstructor: (obj[] -> obj)[]
    }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module private Union =
    let isUnion = memoize (fun t -> FSharpType.IsUnion(t, true))

    let createUnion t =
        let cases = FSharpType.GetUnionCases(t, true)
        {
            cases = cases
            tagReader = FSharpValue.PreComputeUnionTagReader(t, true)
            fieldReader = cases |> Array.map (fun c -> FSharpValue.PreComputeUnionReader(c, true))
            caseConstructor = cases |> Array.map (fun c -> FSharpValue.PreComputeUnionConstructor(c, true))
        }
    let getUnion = memoize createUnion

(* Utilities for working with DUs where none of the cases have a value *)
module TypeSafeEnum =
    let isDefined<'T> (str: string) =
        (Union.getUnion typeof<'T>).cases |> Array.exists (fun case -> case.Name = str)

    let parse<'T> (str: string) =
        let union = Union.getUnion typeof<'T>
        let tag = union.cases |> Array.findIndex (fun case -> case.Name = str)
        (union.caseConstructor.[tag] [||]) :?> 'T

    let tryParse<'T> (str: string) =
        let union = Union.getUnion typeof<'T>
        union.cases |> Array.tryFindIndex (fun case -> case.Name = str)
        |> Option.map (fun tag -> (union.caseConstructor.[tag] [||]) :?> 'T)

    let toString<'T> (x: 'T) =
        let union = Union.getUnion typeof<'T>
        let tag = union.tagReader (x :> obj)
        union.cases.[tag].Name

(* Serializes a discriminated union case with a single field that is a record by flattening the
   record fields to the same level as the discriminator *)
type UnionConverter(discriminator : string) =
    inherit JsonConverter()

    new() = UnionConverter("case")

    override __.CanConvert (t: Type) = Union.isUnion t

    override __.WriteJson(writer: JsonWriter, value: obj, jsonSerializer: JsonSerializer) =
        let union = Union.getUnion (value.GetType())
        let tag = union.tagReader value
        let case = union.cases.[tag]
        let fieldValues = union.fieldReader.[tag] value
        let fieldInfos = case.GetFields()

        writer.WriteStartObject()

        writer.WritePropertyName(discriminator)
        writer.WriteValue(case.Name)

        if fieldInfos.Length = 1 then
            let token = JToken.FromObject(fieldValues.[0], jsonSerializer)
            match token.Type with
            | JTokenType.Object ->
                // flatten the object properties into the same one as the discriminator
                token.Children() |> Seq.iter (fun prop -> prop.WriteTo writer)
            | _ ->
                writer.WritePropertyName(fieldInfos.[0].Name)
                token.WriteTo writer
        else
            Array.zip fieldInfos fieldValues
            |> Array.iter (fun (fieldInfo, fieldValue) ->
                writer.WritePropertyName(fieldInfo.Name)
                jsonSerializer.Serialize(writer, fieldValue))

        writer.WriteEndObject()

    override __.ReadJson(reader: JsonReader, t: Type, _: obj, jsonSerializer: JsonSerializer) =
        let union = Union.getUnion t
        let cases = union.cases

        let token = JToken.ReadFrom reader
        if token.Type <> JTokenType.Object then raise <| new FormatException(sprintf "Expected object reading JSON, got %O" token.Type)
        let obj = token :?> JObject

        let caseName = obj.Item(discriminator) |> string
        let tag = cases |> Array.findIndex (fun case -> case.Name = caseName)
        let case = cases.[tag]
        let fieldInfos = case.GetFields()

        let simpleFieldValue (fieldInfo: PropertyInfo) =
            obj.Item(fieldInfo.Name).ToObject(fieldInfo.PropertyType, jsonSerializer)

        let fieldValues =
            if fieldInfos.Length = 1 then
                let fieldInfo = fieldInfos.[0]
                try
                    // try a flattened record first, so strip out the discriminator property
                    let obj' =
                        obj.Children()
                        |> Seq.filter (function
                            | :? JProperty as prop when prop.Name = discriminator -> false
                            | _ -> true
                        )
                        |> Array.ofSeq
                        |> JObject
                    [| obj'.ToObject(fieldInfo.PropertyType, jsonSerializer) |]
                with _ ->
                    [| simpleFieldValue fieldInfo |]
            else
                fieldInfos |> Array.map simpleFieldValue

        union.caseConstructor.[tag] fieldValues