module Equinox.Cosmos.Integration.JsonConverterTests

open Equinox.Cosmos
open FsCheck.Xunit
open Newtonsoft.Json
open Swensen.Unquote
open System
open Xunit

type Embedded = { embed : string }
type Union =
    | A of Embedded
    | B of Embedded
    interface TypeShape.UnionContract.IUnionContract

let defaultSettings = JsonSerializerSettings()
let mkUnionEncoder () = Gardelloyd.NewtonsoftJson.Codec.Create<Union>(defaultSettings)

type EmbeddedString = { embed : string }
type EmbeddedDate = { embed : DateTime }
type EmbeddedDateTimeOffset = { embed : DateTimeOffset }
type U =
    | R of Embedded
    //| ED of EmbeddedDate // Not recommended; gets mangled by timezone adjustments
    //| S of string // Too messy/confusing to support
    //| DTO of DateTimeOffset // Have not delved into what the exact problem is; no point implementing if strings cant work
    //| DT of DateTime // Have not analyzed but seems to be same issue as DTO
    | EDTO of EmbeddedDateTimeOffset
    | ES of EmbeddedString
    //| I of int // works but removed as no other useful top level values work 
    | N 
    interface TypeShape.UnionContract.IUnionContract

type US =
    | SS of string
    interface TypeShape.UnionContract.IUnionContract

type VerbatimUtf8Tests() =
    let unionEncoder = mkUnionEncoder ()

    [<Fact>]
    let ``encodes correctly`` () =
        let encoded = unionEncoder.Encode(A { embed = "\"" })
        let e : Store.Batch =
            {   p = "streamName"; id = string 0; i = -1L; n = -1L; _etag = null
                e = [| { t = DateTimeOffset.MinValue; c = encoded.EventType; d = encoded.Data; m = null } |] }
        let res = JsonConvert.SerializeObject(e)
        test <@ res.Contains """"d":{"embed":"\""}""" @>

    let uEncoder = Gardelloyd.NewtonsoftJson.Codec.Create<U>(defaultSettings)

    let [<Property(MaxTest=100)>] ``roundtrips diverse bodies correctly`` (x: U) =
        let encoded = uEncoder.Encode x
        let e : Store.Batch =
            {   p = "streamName"; id = string 0; i = -1L; n = -1L; _etag = null
                e = [| { t = DateTimeOffset.MinValue; c = encoded.EventType; d = encoded.Data; m = null } |] }
        let ser = JsonConvert.SerializeObject(e, defaultSettings)
        let des = JsonConvert.DeserializeObject<Store.Batch>(ser, defaultSettings)
        let loaded = Gardelloyd.Core.EventData.Create(des.e.[0].c,des.e.[0].d)
        let decoded = uEncoder.TryDecode loaded |> Option.get
        x =! decoded

    // NB while this aspect works, we don't support it as it gets messy when you then use the VerbatimUtf8Converter
    // https://github.com/JamesNK/Newtonsoft.Json/issues/862 // doesnt apply to this case
    let [<Fact>] ``Gardelloyd.NewtonsoftJson.Codec does not fall prey to Date-strings being mutilated`` () =
        let x = ES { embed = "2016-03-31T07:02:00+07:00" }
        let encoded = uEncoder.Encode x
        let decoded = uEncoder.TryDecode encoded |> Option.get
        test <@ x = decoded @> 

    //// NB while this aspect works, we don't support it as it gets messy when you then use the VerbatimUtf8Converter
    //let sEncoder = Gardelloyd.NewtonsoftJson.Codec.Create<US>(defaultSettings)
    //let [<Theory; InlineData ""; InlineData null>] ``Gardelloyd.NewtonsoftJson.Codec can roundtrip strings`` (value: string)  =
    //    let x = SS value
    //    let encoded = sEncoder.Encode x
    //    let decoded = sEncoder.TryDecode encoded |> Option.get
    //    test <@ x = decoded @> 

module VerbatimeUtf8NullHandling =
    type [<NoEquality; NoComparison>] EventHolderWithAndWithoutRequired =
        {   /// Event body, as UTF-8 encoded json ready to be injected directly into the Json being rendered
            [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
            d: byte[] // required

            /// Optional metadata, as UTF-8 encoded json, ready to emit directly (entire field is not written if value is null)
            [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
            [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
            m: byte[] }

    let values : obj[][] =
        [|  [| null |]
            [| [||] |]
            [| System.Text.Encoding.UTF8.GetBytes "{}" |] |]

    [<Theory; MemberData "values">]
    let ``roundtrips nulls and empties consistently`` value =
        let e : EventHolderWithAndWithoutRequired = { d = value; m = value }
        let ser = JsonConvert.SerializeObject(e)
        let des = JsonConvert.DeserializeObject<EventHolderWithAndWithoutRequired>(ser)
        test <@ ((e.m = null || e.m.Length = 0) && (des.m = null)) || System.Linq.Enumerable.SequenceEqual(e.m, des.m) @>
        test <@ ((e.d = null || e.d.Length = 0) && (des.d = null)) || System.Linq.Enumerable.SequenceEqual(e.d, des.d) @>

type Base64ZipUtf8Tests() =
    let unionEncoder = mkUnionEncoder ()

    [<Fact>]
    let ``serializes, achieving compression`` () =
        let encoded = unionEncoder.Encode(A { embed = String('x',5000) })
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data
                m = null }
        let res = JsonConvert.SerializeObject e
        test <@ res.Contains("\"d\":\"") && res.Length < 100 @>

    [<Property>]
    let roundtrips value =
        let hasNulls =
            match value with
            | A x | B x when obj.ReferenceEquals(null, x) -> true
            | A { embed = x } | B { embed = x } -> obj.ReferenceEquals(null, x)
        if hasNulls then () else

        let encoded = unionEncoder.Encode value
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data
                m = null }
        let ser = JsonConvert.SerializeObject(e)
        test <@ ser.Contains("\"d\":\"") @>
        let des = JsonConvert.DeserializeObject<Store.Unfold>(ser)
        let d = Gardelloyd.Core.EventData.Create(des.c, des.d)
        let decoded = unionEncoder.TryDecode d |> Option.get
        test <@ value = decoded @>