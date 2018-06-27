module Foldunk.Equinox.ByteArrayConverterTests

open Newtonsoft.Json
open Swensen.Unquote
open System
open Xunit

module Fixtures =
    type Embedded = { embed : string }

let serializer = new JsonSerializer()

let inline serialize (x:'t) =
    use sw = new System.IO.StringWriter()
    use w = new JsonTextWriter(sw)
    serializer.Serialize(w,x)
    sw.ToString()

[<Fact>]
let ``ByteArrayConverter serializes properly`` () =
    use sw = new System.IO.StringWriter()
    use w = new JsonTextWriter(sw)
    let blob = serialize ({ embed = "\"" } : Fixtures.Embedded) |> System.Text.Encoding.UTF8.GetBytes
    let e : Equinox.Cosmos.EquinoxEvent =
        {   id = null
            s = null
            k  = null
            ts = DateTimeOffset.MinValue
            sn = 0L
            et = null

            d = blob

            md = null }
    let res = serialize e
    test <@ res.Contains """"d":{"embed":"\""}""" @>