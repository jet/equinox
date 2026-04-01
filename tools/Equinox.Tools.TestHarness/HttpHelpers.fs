module Equinox.Tools.TestHarness.HttpHelpers

open System
open System.Net
open System.Net.Http
open System.Runtime.Serialization

type HttpContent with
    member c.ReadAsString() = async {
        match c with
        | null -> return null
        | c -> return! c.ReadAsStringAsync() |> Async.AwaitTaskCorrect
    }

    // only intended for logging in HttpClient! Hence the esoteric name
    member internal c.ReadAsStringDiapered() = async {
        try return! c.ReadAsString()
        with :? ObjectDisposedException -> return "<HttpContent:ObjectDisposedException>"
    }

/// Exception indicating an unexpected response received by an Http Client
type InvalidHttpResponseException =
    inherit Exception

    // TODO: include headers
    val private userMessage: string
    val private requestMethod: string
    val RequestUri: Uri
    val RequestBody: string
    val StatusCode: HttpStatusCode
    val ReasonPhrase: string
    val ResponseBody: string

    member x.RequestMethod = HttpMethod(x.requestMethod)

    private new(userMessage: string, requestMethod: HttpMethod, requestUri: Uri, requestBody: string,
                   statusCode: HttpStatusCode, reasonPhrase: string, responseBody: string,
                   ?innerException: exn) =
        {
            inherit Exception(message = null, innerException = defaultArg innerException null) ; userMessage = userMessage ;
            requestMethod = string requestMethod ; RequestUri = requestUri ; RequestBody = requestBody ;
            StatusCode = statusCode ; ReasonPhrase = reasonPhrase ; ResponseBody = responseBody
        }

    override e.Message =
        System.Text.StringBuilder.Build(fun sb ->
            sb.Appendfn "%s %O RequestUri=%O HttpStatusCode=%O" e.userMessage e.RequestMethod e.RequestUri e.StatusCode
            let getBodyString str = if String.IsNullOrWhiteSpace str then "<null>" else str
            sb.Appendfn "RequestBody=%s" (getBodyString e.RequestBody)
            sb.Appendfn "ResponseBody=%s" (getBodyString e.ResponseBody))

    static member Create(userMessage: string, response: HttpResponseMessage, ?innerException: exn) = async {
        let request = response.RequestMessage
        let! responseBodyC = response.Content.ReadAsStringDiapered() |> Async.StartChild
        let! requestBody = request.Content.ReadAsStringDiapered()
        let! responseBody = responseBodyC
        return
            InvalidHttpResponseException(
                userMessage, request.Method, request.RequestUri, requestBody,
                response.StatusCode, response.ReasonPhrase, responseBody,
                ?innerException = innerException)
    }

    static member Create(response: HttpResponseMessage, ?innerException: exn) =
        InvalidHttpResponseException.Create("HTTP request yielded unexpected response.", response, ?innerException = innerException)
