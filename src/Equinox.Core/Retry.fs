namespace Equinox.Core

/// Exception yielded by after `count` attempts to complete an operation have taken place
type OperationRetriesExceededException(count : int, innerException : exn) =
   inherit exn(sprintf "Retries failed; aborting after %i attempts." count, innerException)

/// Helper for defining backoffs within the definition of a retry policy for a store.
module Retry =
    /// Wraps an async computation in a retry loop, passing the (1-based) count into the computation and,
    ///   (until `attempts` exhausted) on an exception matching the `filter`, waiting for the timespan chosen by `backoff` before retrying
    let withBackoff (maxAttempts : int) (backoff : int -> System.TimeSpan option) (f : int -> Async<'a>) : Async<'a> =
        if maxAttempts < 1 then raise (invalidArg "maxAttempts" "Should be >= 1")
        let rec go attempt = async {
            try
                let! res = f attempt
                return res
            with ex ->
                if attempt = maxAttempts then return raise (OperationRetriesExceededException(maxAttempts, ex))
                else
                    match backoff attempt with
                    | Some timespan -> do! Async.Sleep (int timespan.TotalMilliseconds)
                    | None -> ()
                    return! go (attempt + 1) }
        go 1