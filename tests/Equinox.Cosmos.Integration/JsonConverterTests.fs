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

let mkUnionEncoder () = Equinox.UnionCodec.JsonUtf8.Create<Union>(JsonSerializerSettings())

type VerbatimUtf8Tests() =
    let unionEncoder = mkUnionEncoder ()

    [<Fact>]
#if NET461
    [<Trait("KnownFailOn","Mono")>] // Likely due to net461 not having consistent json.net refs and no binding redirects
#endif
    let ``encodes correctly`` () =
        let encoded = unionEncoder.Encode(A { embed = "\"" })
        let e : Store.Batch =
            {   p = "streamName"; id = string 0; i = -1L; n = -1L; _etag = null
                e = [| { t = DateTimeOffset.MinValue; c = encoded.caseName; d = encoded.payload; m = null } |] }
        let res = JsonConvert.SerializeObject(e)
        test <@ res.Contains """"d":{"embed":"\""}""" @>

type Base64ZipUtf8Tests() =
    let unionEncoder = mkUnionEncoder ()

    [<Fact>]
#if NET461
    [<Trait("KnownFailOn","Mono")>] // Likely due to net461 not having consistent json.net refs and no binding redirects
#endif
    let ``serializes, achieving compression`` () =
        let encoded = unionEncoder.Encode(A { embed = String('x',5000) })
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.caseName
                d = encoded.payload
                m = null }
        let res = JsonConvert.SerializeObject e
        test <@ res.Contains("\"d\":\"") && res.Length < 100 @>

    [<Property>]
#if NET461
    [<Trait("KnownFailOn","Mono")>] // Likely due to net461 not having consistent json.net refs and no binding redirects
#endif
    let roundtrips value =
        let hasNulls =
            match value with
            | A x | B x when obj.ReferenceEquals(null, x) -> true
            | A { embed = x } | B { embed = x } -> obj.ReferenceEquals(null, x)
        if hasNulls then () else

        let encoded = unionEncoder.Encode value
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.caseName
                d = encoded.payload
                m = null }
        let ser = JsonConvert.SerializeObject(e)
        test <@ ser.Contains("\"d\":\"") @>
        let des = JsonConvert.DeserializeObject<Store.Unfold>(ser)
        let d : Equinox.UnionCodec.EncodedUnion<_> = { caseName = des.c; payload = des.d }
        let decoded = unionEncoder.TryDecode d |> Option.get
        test <@ value = decoded @>