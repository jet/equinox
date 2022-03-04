[<AutoOpen>]
module internal Equinox.Tool.Infrastructure.Prelude

open Equinox.Tools.TestHarness.HttpHelpers
open System
open System.Diagnostics
open System.Text

type Exception with
    // https://github.com/fsharp/fslang-suggestions/issues/660
    member this.Reraise() =
        (System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture this).Throw ()
        Unchecked.defaultof<_>

type Async with
    /// <summary>
    ///     Raises an exception using Async's continuation mechanism directly.
    /// </summary>
    /// <param name="exn">Exception to be raised.</param>
    static member Raise(exn : #exn) = Async.FromContinuations(fun (_,ec,_) -> ec exn)

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : System.Threading.Tasks.Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (sc,ec,_) ->
            task.ContinueWith(fun (t : System.Threading.Tasks.Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif t.IsCanceled then ec(System.Threading.Tasks.TaskCanceledException())
                else sc t.Result)
            |> ignore)
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : System.Threading.Tasks.Task) : Async<unit> =
        Async.FromContinuations(fun (sc,ec,_) ->
            task.ContinueWith(fun (task : System.Threading.Tasks.Task) ->
                if task.IsFaulted then
                    let e = task.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif task.IsCanceled then
                    ec(System.Threading.Tasks.TaskCanceledException())
                else
                    sc ())
            |> ignore)

type StringBuilder with
    member sb.Appendf fmt = Printf.ksprintf (ignore << sb.Append) fmt

    static member inline Build(builder : StringBuilder -> unit) =
        let instance = StringBuilder() // TOCONSIDER PooledStringBuilder.GetInstance()
        builder instance
        instance.ToString()

[<AutoOpen>]
module HttpHelpers =

    open System.Net
    open System.Net.Http

    /// Operations on System.Net.HttpRequestMessage
    module HttpReq =

        /// Creates an HTTP GET request.
        let inline create () = new HttpRequestMessage()

        /// Assigns a method to an HTTP request.
        let inline withMethod (m : HttpMethod) (req : HttpRequestMessage) =
            req.Method <- m
            req

        /// Creates an HTTP GET request.
        let inline get () = create ()

        /// Creates an HTTP POST request.
        let inline post () = create () |> withMethod HttpMethod.Post

        /// Creates an HTTP PATCH request.
        let inline patch () = create () |> withMethod (HttpMethod "PATCH")

        /// Creates an HTTP DELETE request.
        let inline delete () = create () |> withMethod HttpMethod.Delete

        /// Assigns a path to an HTTP request.
        let inline withUri (u : Uri) (req : HttpRequestMessage) =
            req.RequestUri <- u
            req

        /// Assigns a path to an HTTP request.
        let inline withPath (p : string) (req : HttpRequestMessage) =
            req |> withUri (Uri(p, UriKind.Relative))

        /// Assigns a path to a Http request using printf-like formatting.
        let inline withPathf fmt =
            Printf.ksprintf withPath fmt

        /// Uses supplied string as UTF8-encoded json request body
        let withJsonString (json : string) (request : HttpRequestMessage) =
            request.Content <- new StringContent(json, Encoding.UTF8, "application/json")
            request

        /// Use supplied serialize to convert the request to a json string, include that as a UTF-8 encoded body
        let withJson (serialize : 'Request -> string) (input : 'Request) (request : HttpRequestMessage) =
            request |> withJsonString (serialize input)

        /// Use standard Json.Net profile convert the request to a json rendering
        let withJsonNet<'Request> (input : 'Request) (request : HttpRequestMessage) =
            request |> withJson Newtonsoft.Json.JsonConvert.SerializeObject input

        /// Appends supplied header to request header
        let inline withHeader (name : string) (value : string) (request : HttpRequestMessage) =
            request.Headers.Add(name, value)
            request

    type HttpClient with
        /// <summary>
        ///     Drop-in replacement for HttpClient.SendAsync which addresses known timeout issues
        /// </summary>
        /// <param name="msg">HttpRequestMessage to be submitted.</param>
        member client.SendAsync2(msg : HttpRequestMessage) = async {
            let! ct = Async.CancellationToken
            try return! client.SendAsync(msg, ct) |> Async.AwaitTaskCorrect
            // address https://github.com/dotnet/corefx/issues/20296
            with :? System.Threading.Tasks.TaskCanceledException ->
                let message =
                    match client.BaseAddress with
                    | null -> "HTTP request timeout"
                    | baseAddr -> sprintf "HTTP request timeout [%O]" baseAddr

                return! Async.Raise(TimeoutException message)
        }

    type HttpResponseMessage with

        /// Raises an <c>InvalidHttpResponseException</c> if the response status code does not match expected value.
        member response.EnsureStatusCode(expectedStatusCode : HttpStatusCode) = async {
            if response.StatusCode <> expectedStatusCode then
                let! exn = InvalidHttpResponseException.Create("Http request yielded unanticipated HTTP Result.", response)
                do raise exn
        }

        /// <summary>Asynchronously deserializes the json response content using the supplied `deserializer`, without validating the `StatusCode`</summary>
        /// <param name="deserializer">The decoder routine to apply to the body content. Exceptions are wrapped in exceptions containing the offending content.</param>
        member response.InterpretContent<'Decoded>(deserializer : string -> 'Decoded) : Async<'Decoded> = async {
            let! content = response.Content.ReadAsString()
            try return deserializer content
            with e ->
                let! exn = InvalidHttpResponseException.Create("HTTP response could not be decoded.", response, e)
                return raise exn
        }

        /// <summary>Asynchronously deserializes the json response content using the supplied `deserializer`, validating the `StatusCode` is `expectedStatusCode`</summary>
        /// <param name="expectedStatusCode">check that status code matches supplied code or raise a <c>InvalidHttpResponseException</c> if it doesn't.</param>
        /// <param name="deserializer">The decoder routine to apply to the body content. Exceptions are wrapped in exceptions containing the offending content.</param>
        member response.Interpret<'Decoded>(expectedStatusCode : HttpStatusCode, deserializer : string -> 'Decoded) : Async<'Decoded> = async {
            do! response.EnsureStatusCode expectedStatusCode
            return! response.InterpretContent deserializer
        }

    module HttpRes =
        /// Deserialize body using default Json.Net profile - throw with content details if StatusCode is unexpected or decoding fails
        let deserializeExpectedJsonNet<'t> expectedStatusCode (res : HttpResponseMessage) =
            res.Interpret(expectedStatusCode, Newtonsoft.Json.JsonConvert.DeserializeObject<'t>)

        /// Deserialize body using default Json.Net profile - throw with content details if StatusCode is not OK or decoding fails
        let deserializeOkJsonNet<'t> =
            deserializeExpectedJsonNet<'t> HttpStatusCode.OK
