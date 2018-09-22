module Foldunk.Serialization.Tests

open Foldunk.Serialization
open Newtonsoft.Json
open Swensen.Unquote.Assertions
open System
open System.IO
open System.Text.RegularExpressions
open Xunit

let normalizeJsonString (json : string) =
    let str1 = Regex.Replace(json, @"{\s*}", "{}")
    let str2 = Regex.Replace(str1, @"\[\s*\]", "[]")
    let str3 = Regex.Replace(str2, @"\.0+", "")
    str3

type TestRecordPayload =
    {
        test: string
    }

type TrickyRecordPayload =
    {
        Item: string
    }

[<JsonConverter(typeof<Converters.UnionConverter>)>]
type TestDU =
    | CaseA of TestRecordPayload
    | CaseB
    | CaseC of string
    | CaseD of a: string
    | CaseE of string * int
    | CaseF of a: string * b: int
    | CaseG of TrickyRecordPayload
    | CaseH of a: TestRecordPayload
    | CaseI of a: TestRecordPayload * b: string
    | CaseJ of a: Nullable<int>
    | CaseK of a: int * b: Nullable<int>
    | CaseL of a: Nullable<int> * b: Nullable<int>
    | CaseM of a: int option
    | CaseN of a: int * b: int option
    | CaseO of a: int option * b: int option

// no camel case, because I want to test "Item" as a record property
let settings = Settings.CreateDefault(camelCase = false)

[<Fact>]
let ``UnionConverter produces expected output`` () =
    let a = CaseA {test = "hi"}
    let aJson = JsonConvert.SerializeObject(a, settings)

    test <@ """{"case":"CaseA","test":"hi"}""" = aJson @>

    let b = CaseB
    let bJson = JsonConvert.SerializeObject(b, settings)

    test <@ """{"case":"CaseB"}""" = bJson @>

    let c = CaseC "hi"
    let cJson = JsonConvert.SerializeObject(c, settings)

    test <@ """{"case":"CaseC","Item":"hi"}""" = cJson @>

    let d = CaseD "hi"
    let dJson = JsonConvert.SerializeObject(d, settings)

    test <@ """{"case":"CaseD","a":"hi"}""" = dJson @>

    let e = CaseE ("hi", 0)
    let eJson = JsonConvert.SerializeObject(e, settings)

    test <@ """{"case":"CaseE","Item1":"hi","Item2":0}""" = eJson @>

    let f = CaseF ("hi", 0)
    let fJson = JsonConvert.SerializeObject(f, settings)

    test <@ """{"case":"CaseF","a":"hi","b":0}""" = fJson @>

    let g = CaseG {Item = "hi"}
    let gJson = JsonConvert.SerializeObject(g, settings)

    test <@ """{"case":"CaseG","Item":"hi"}""" = gJson @>

    // this may not be expected, but I don't itend changing it
    let h = CaseH {test = "hi"}
    let hJson = JsonConvert.SerializeObject(h, settings)

    test <@ """{"case":"CaseH","test":"hi"}""" = hJson @>

    let i = CaseI ({test = "hi"}, "bye")
    let iJson = JsonConvert.SerializeObject(i, settings)

    test <@ "{\"case\":\"CaseI\",\"a\":{\"test\":\"hi\"},\"b\":\"bye\"}" = iJson @>

let requiredSettingsToHandleOptionalFields =
    // NB this is me documenting current behavior - ideally optionality wou
    let s = Settings.CreateDefault(camelCase = false)
    s.Converters.Add(Converters.OptionConverter())
    s

[<Fact>]
let ``UnionConverter deserializes properly`` () =
    let aJson = """{"case":"CaseA","test":"hi"}"""
    let a = JsonConvert.DeserializeObject<TestDU>(aJson, settings)

    test <@ CaseA {test = "hi"} = a @>

    let bJson = """{"case":"CaseB"}"""
    let b = JsonConvert.DeserializeObject<TestDU>(bJson, settings)

    test <@ CaseB = b @>

    let cJson = """{"case":"CaseC","Item":"hi"}"""
    let c = JsonConvert.DeserializeObject<TestDU>(cJson, settings)

    test <@ CaseC "hi" = c @>

    let dJson = """{"case":"CaseD","a":"hi"}"""
    let d = JsonConvert.DeserializeObject<TestDU>(dJson, settings)

    test <@ CaseD "hi" = d @>

    let eJson = """{"case":"CaseE","Item1":"hi","Item2":0}"""
    let e = JsonConvert.DeserializeObject<TestDU>(eJson, settings)

    test <@ CaseE ("hi", 0) = e @>

    let fJson = """{"case":"CaseF","a":"hi","b":0}"""
    let f = JsonConvert.DeserializeObject<TestDU>(fJson, settings)

    test <@ CaseF ("hi", 0) = f @>

    let gJson = """{"case":"CaseG","Item":"hi"}"""
    let g = JsonConvert.DeserializeObject<TestDU>(gJson, settings)

    test <@ CaseG {Item = "hi"} = g @>

    let hJson = """{"case":"CaseH","test":"hi"}"""
    let h = JsonConvert.DeserializeObject<TestDU>(hJson, settings)

    test <@ CaseH {test = "hi"} = h @>

    let iJson = """{"case":"CaseI","a":{"test":"hi"},"b":"bye"}"""
    let i = JsonConvert.DeserializeObject<TestDU>(iJson, settings)

    test <@ CaseI ({test = "hi"}, "bye") = i @>

    test <@ CaseJ (Nullable 1) = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseJ","a":1}""", settings) @>
    test <@ CaseK (1, Nullable 2) = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseK", "a":1, "b":2 }""", settings) @>
    test <@ CaseL (Nullable 1, Nullable 2) = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseL", "a": 1, "b": 2 }""", settings) @>

    let deserialzeCustom s = JsonConvert.DeserializeObject<TestDU>(s, requiredSettingsToHandleOptionalFields)
    test <@ CaseM (Some 1) = deserialzeCustom """{"case":"CaseM","a":1}""" @>
    test <@ CaseN (1, Some 2) = deserialzeCustom """{"case":"CaseN", "a":1, "b":2 }""" @>
    test <@ CaseO (Some 1, Some 2) = deserialzeCustom """{"case":"CaseO", "a": 1, "b": 2 }""" @>

[<Fact>]
let ``UnionConverter handles missing fields`` () =
    test <@ CaseJ (Nullable<int>()) = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseJ"}""", settings) @>
    test <@ CaseK (1, (Nullable<int>())) = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseK","a":1}""", settings) @>
    test <@ CaseL ((Nullable<int>()), (Nullable<int>())) = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseL"}""", settings) @>

    test <@ CaseM None = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseM"}""", settings) @>
    test <@ CaseN (1, None) = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseN","a":1}""", settings) @>
    test <@ CaseO (None, None) = JsonConvert.DeserializeObject<TestDU>("""{"case":"CaseO"}""", settings) @>

let (|Q|) (s : string) = Newtonsoft.Json.JsonConvert.SerializeObject s

let render = function
    | CaseA { test = null } -> """{"case":"CaseA"}"""
    | CaseA { test = Q x} -> sprintf """{"case":"CaseA","test":%s}""" x
    | CaseB -> """{"case":"CaseB"}"""
    | CaseC null -> """{"case":"CaseC"}"""
    | CaseC (Q s) -> sprintf """{"case":"CaseC","Item":%s}""" s
    | CaseD null -> """{"case":"CaseD"}"""
    | CaseD (Q s) -> sprintf """{"case":"CaseD","a":%s}""" s
    | CaseE (null,y) -> sprintf """{"case":"CaseE","Item2":%d}""" y
    | CaseE (Q x,y) -> sprintf """{"case":"CaseE","Item1":%s,"Item2":%d}""" x y
    | CaseF (null,y) -> sprintf """{"case":"CaseF","b":%d}""" y
    | CaseF (Q x,y) -> sprintf """{"case":"CaseF","a":%s,"b":%d}""" x y
    | CaseG {Item = null} -> """{"case":"CaseG"}"""
    | CaseG {Item = Q s} -> sprintf """{"case":"CaseG","Item":%s}""" s
    | CaseH {test = null} -> """{"case":"CaseH"}"""
    | CaseH {test = Q s} -> sprintf """{"case":"CaseH","test":%s}""" s
    | CaseI ({test = null}, null) -> """{"case":"CaseI","a":{}}"""
    | CaseI ({test = null}, Q s) -> sprintf """{"case":"CaseI","a":{},"b":%s}""" s
    | CaseI ({test = Q s}, null) -> sprintf """{"case":"CaseI","a":{"test":%s}}""" s
    | CaseI ({test = Q s}, Q b) -> sprintf """{"case":"CaseI","a":{"test":%s},"b":%s}""" s b

    | CaseJ x when not x.HasValue -> """{"case":"CaseJ"}"""
    | CaseJ x -> sprintf """{"case":"CaseJ","a":%d}""" x.Value
    | CaseK (a,x) when not x.HasValue -> sprintf """{"case":"CaseK","a":%d}""" a
    | CaseK (a,x) -> sprintf """{"case":"CaseK","a":%d,"b":%d}""" a x.Value
    | CaseL (a,b) when not a.HasValue && not b.HasValue -> """{"case":"CaseL"}"""
    | CaseL (a,b) when not a.HasValue -> sprintf """{"case":"CaseL","b":%d}""" b.Value
    | CaseL (a,b) when not b.HasValue -> sprintf """{"case":"CaseL","a":%d}""" a.Value
    | CaseL (a,b) -> sprintf """{"case":"CaseL","a":%d,"b":%d}""" a.Value b.Value

    | CaseM None -> """{"case":"CaseM"}"""
    | CaseM (Some x) -> sprintf """{"case":"CaseM","a":%d}""" x
    | CaseN (a,None) -> sprintf """{"case":"CaseN","a":%d}""" a
    | CaseN (a,x) -> sprintf """{"case":"CaseN","a":%d,"b":%d}""" a x.Value
    | CaseO (None,None) -> """{"case":"CaseO"}"""
    | CaseO (None,b) -> sprintf """{"case":"CaseO","b":%d}""" b.Value
    | CaseO (a,None) -> sprintf """{"case":"CaseO","a":%d}""" a.Value
    | CaseO (Some a,Some b) -> sprintf """{"case":"CaseO","a":%d,"b":%d}""" a b

[<FsCheck.Xunit.Property(MaxTest=1000)>]
let ``UnionConverter roundtrip property test`` (x: TestDU) =
    let serialized = JsonConvert.SerializeObject(x, requiredSettingsToHandleOptionalFields)
    render x =! serialized
    let deserialized = JsonConvert.DeserializeObject<_>(serialized, requiredSettingsToHandleOptionalFields)
    deserialized =! x

[<Fact>]
let ``UnionConverter's exception catch doesn't make the model invalid`` () =
    let s = JsonSerializer.CreateDefault()
    let mutable gotError = false
    s.Error.Add(fun _ -> gotError <- true)

    let dJson = """{"case":"CaseD","a":"hi"}"""
    use dReader = new StringReader(dJson)
    use dJsonReader = new JsonTextReader(dReader)
    let d = s.Deserialize<TestDU>(dJsonReader)

    test <@ (CaseD "hi") = d @>
    test <@ false = gotError @>

[<Fact>]
let ``UnionConverter by default throws on unknown cases`` () =
    let aJson = """{"case":"CaseUnknown"}"""
    let act () = JsonConvert.DeserializeObject<TestDU>(aJson, settings)

    fun (e : System.InvalidOperationException) -> <@ -1 <> e.Message.IndexOf "No case defined for 'CaseUnknown', and no catchAllCase nominated" @>
    |> raisesWith <@ act() @>

[<JsonConverter(typeof<Converters.UnionConverter>, "case", "Catchall")>]
type DuWithCatchAll =
| Known
| Catchall

[<Fact>]
let ``UnionConverter supports a nominated catchall`` () =
    let aJson = """{"case":"CaseUnknown"}"""
    let a = JsonConvert.DeserializeObject<DuWithCatchAll>(aJson, settings)

    test <@ Catchall = a @>

[<JsonConverter(typeof<Converters.UnionConverter>, "case", "CatchAllThatCantBeFound")>]
type DuWithMissingCatchAll =
| Known

[<Fact>]
let ``UnionConverter explains if nominated catchAll not found`` () =
    let aJson = """{"case":"CaseUnknown"}"""
    let act () = JsonConvert.DeserializeObject<DuWithMissingCatchAll>(aJson, settings)

    fun (e : System.InvalidOperationException) -> <@ -1 <> e.Message.IndexOf "nominated catchAllCase: 'CatchAllThatCantBeFound' not found" @>
    |> raisesWith <@ act() @>