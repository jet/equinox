module Foldunk.Serialization.Tests

open Foldunk.Serialization
open Newtonsoft.Json
open Swensen.Unquote.Assertions
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
    | CaseJ of a: int option
    | CaseK of a: int * b: int option
    | CaseL of a: int option * b: int option

// no camel case, because I want to test "Item" as a record property
let settings = Settings.CreateDefault(camelCase = false)

[<Fact>]
let ``UnionConverter produces expected output`` () =
    let a = CaseA {test = "hi"}
    let aJson = JsonConvert.SerializeObject(a, settings)

    test <@ "{\"case\":\"CaseA\",\"test\":\"hi\"}" = aJson @>

    let b = CaseB
    let bJson = JsonConvert.SerializeObject(b, settings)

    test <@ "{\"case\":\"CaseB\"}" = bJson @>

    let c = CaseC "hi"
    let cJson = JsonConvert.SerializeObject(c, settings)

    test <@ "{\"case\":\"CaseC\",\"Item\":\"hi\"}" = cJson @>

    let d = CaseD "hi"
    let dJson = JsonConvert.SerializeObject(d, settings)

    test <@ "{\"case\":\"CaseD\",\"a\":\"hi\"}" = dJson @>

    let e = CaseE ("hi", 0)
    let eJson = JsonConvert.SerializeObject(e, settings)

    test <@ "{\"case\":\"CaseE\",\"Item1\":\"hi\",\"Item2\":0}" = eJson @>

    let f = CaseF ("hi", 0)
    let fJson = JsonConvert.SerializeObject(f, settings)

    test <@ "{\"case\":\"CaseF\",\"a\":\"hi\",\"b\":0}" = fJson @>

    let g = CaseG {Item = "hi"}
    let gJson = JsonConvert.SerializeObject(g, settings)

    test <@ "{\"case\":\"CaseG\",\"Item\":\"hi\"}" = gJson @>

    // this may not be expected, but I don't itend changing it
    let h = CaseH {test = "hi"}
    let hJson = JsonConvert.SerializeObject(h, settings)

    test <@ "{\"case\":\"CaseH\",\"test\":\"hi\"}" = hJson @>

    let i = CaseI ({test = "hi"}, "bye")
    let iJson = JsonConvert.SerializeObject(i, settings)

    test <@ "{\"case\":\"CaseI\",\"a\":{\"test\":\"hi\"},\"b\":\"bye\"}" = iJson @>

[<Fact>]
let ``UnionConverter deserializes properly`` () =
    let aJson = "{\"case\":\"CaseA\",\"test\":\"hi\"}"
    let a = JsonConvert.DeserializeObject<TestDU>(aJson, settings)

    test <@ CaseA {test = "hi"} = a @>

    let bJson = "{\"case\":\"CaseB\"}"
    let b = JsonConvert.DeserializeObject<TestDU>(bJson, settings)

    test <@ CaseB = b @>

    let cJson = "{\"case\":\"CaseC\",\"Item\":\"hi\"}"
    let c = JsonConvert.DeserializeObject<TestDU>(cJson, settings)

    test <@ CaseC "hi" = c @>

    let dJson = "{\"case\":\"CaseD\",\"a\":\"hi\"}"
    let d = JsonConvert.DeserializeObject<TestDU>(dJson, settings)

    test <@ CaseD "hi" = d @>

    let eJson = "{\"case\":\"CaseE\",\"Item1\":\"hi\",\"Item2\":0}"
    let e = JsonConvert.DeserializeObject<TestDU>(eJson, settings)

    test <@ CaseE ("hi", 0) = e @>

    let fJson = "{\"case\":\"CaseF\",\"a\":\"hi\",\"b\":0}"
    let f = JsonConvert.DeserializeObject<TestDU>(fJson, settings)

    test <@ CaseF ("hi", 0) = f @>

    let gJson = "{\"case\":\"CaseG\",\"Item\":\"hi\"}"
    let g = JsonConvert.DeserializeObject<TestDU>(gJson, settings)

    test <@ CaseG {Item = "hi"} = g @>

    let hJson = "{\"case\":\"CaseH\",\"test\":\"hi\"}"
    let h = JsonConvert.DeserializeObject<TestDU>(hJson, settings)

    test <@ CaseH {test = "hi"} = h @>

    let iJson = "{\"case\":\"CaseI\",\"a\":{\"test\":\"hi\"},\"b\":\"bye\"}"
    let i = JsonConvert.DeserializeObject<TestDU>(iJson, settings)

    test <@ CaseI ({test = "hi"}, "bye") = i @>

    let jJson = """{"case":"CaseJ","a":1}"""
    let j = JsonConvert.DeserializeObject<TestDU>(jJson, settings)

    test <@ CaseJ (Some 1) = j @>

    let kJson = """{"case":"CaseK", "a":1, "b":2 }"""
    let k = JsonConvert.DeserializeObject<TestDU>(kJson, settings)

    test <@ CaseK (1, Some 2) = k @>

    let lJson = """{"case":"CaseL", "a": 1, "b": 2 }"""
    let l = JsonConvert.DeserializeObject<TestDU>(lJson, settings)

    test <@ CaseL (Some 1, Some 2) = l @>

[<Fact>]
let ``UnionConverter handles missing fields`` () =
    let jJson = """{"case":"CaseJ"}"""
    let j = JsonConvert.DeserializeObject<TestDU>(jJson, settings)

    test <@ CaseJ None = j @>

    let kJson = """{"case":"CaseK","a":1}"""
    let k = JsonConvert.DeserializeObject<TestDU>(kJson, settings)

    test <@ CaseK (1, None) = k @>

    let lJson = """{"case":"CaseL"}"""
    let l = JsonConvert.DeserializeObject<TestDU>(lJson, settings)

    test <@ CaseL (None, None) = l @>

[<Fact>]
let ``UnionConverter's exception catch doesn't make the model invalid`` () =

    let s = JsonSerializer.CreateDefault()
    let mutable gotError = false
    s.Error.Add(fun _ -> gotError <- true)

    let dJson = "{\"case\":\"CaseD\",\"a\":\"hi\"}"
    use dReader = new StringReader(dJson)
    use dJsonReader = new JsonTextReader(dReader)
    let d = s.Deserialize<TestDU>(dJsonReader)

    test <@ (CaseD "hi") = d @>
    test <@ false = gotError @>

[<Fact>]
let ``UnionConverter by default throws on unknown cases`` () =
    let aJson = "{\"case\":\"CaseUnknown\"}"
    let act () = JsonConvert.DeserializeObject<TestDU>(aJson, settings)

    fun (e : System.InvalidOperationException) -> <@ -1 <> e.Message.IndexOf "No case defined for 'CaseUnknown', and no catchAllCase nominated" @>
    |> raisesWith <@ act() @>

[<JsonConverter(typeof<Converters.UnionConverter>, "case", "Catchall")>]
type DuWithCatchAll =
| Known
| Catchall

[<Fact>]
let ``UnionConverter supports a nominated catchall`` () =
    let aJson = "{\"case\":\"CaseUnknown\"}"
    let a = JsonConvert.DeserializeObject<DuWithCatchAll>(aJson, settings)

    test <@ Catchall = a @>

[<JsonConverter(typeof<Converters.UnionConverter>, "case", "CatchAllThatCantBeFound")>]
type DuWithMissingCatchAll =
| Known

[<Fact>]
let ``UnionConverter explains if nominated catchAll not found`` () =
    let aJson = "{\"case\":\"CaseUnknown\"}"
    let act () = JsonConvert.DeserializeObject<DuWithMissingCatchAll>(aJson, settings)

    fun (e : System.InvalidOperationException) -> <@ -1 <> e.Message.IndexOf "nominated catchAllCase: 'CatchAllThatCantBeFound' not found" @>
    |> raisesWith <@ act() @>