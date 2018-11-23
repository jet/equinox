namespace Equinox.Cosmos

// NB this is a copy of the one in Backend - there is also one in Equinox/Infrastrcture.fs which this will be merged into

open System

/// Given a value, creates a function with one ignored argument which returns the value.

/// A backoff strategy.
/// Accepts the attempt number and returns an interval in milliseconds to wait.
/// If None then backoff should stop.
type Backoff = int -> int option

/// Operations on back off strategies represented as functions (int -> int option)
/// which take an attempt number and produce an interval.
module Backoff =

  let inline konst x _ = x
  let private checkOverflow x =
    if x = System.Int32.MinValue then 2000000000
    else x

  /// Stops immediately.
  let never : Backoff = konst None

  /// Always returns a fixed interval.
  let linear i : Backoff = konst (Some i)

  /// Modifies the interval.
  let bind (f:int -> int option) (b:Backoff) =
    fun i ->
      match b i with
      | Some x -> f x
      | None -> None

  /// Modifies the interval.
  let map (f:int -> int) (b:Backoff) : Backoff =
    fun i ->
      match b i with
      | Some x -> f x |> checkOverflow |> Some
      | None -> None

  /// Bounds the interval.
  let bound mx = map (min mx)

  /// Creates a back-off strategy which increases the interval exponentially.
  let exp (initialIntervalMs:int) (multiplier:float) : Backoff =
    fun i -> (float initialIntervalMs) * (pown multiplier i) |> int |> checkOverflow |> Some

  /// Randomizes the output produced by a back-off strategy:
  /// randomizedInterval = retryInterval * (random in range [1 - randomizationFactor, 1 + randomizationFactor])
  let rand (randomizationFactor:float) =
    let rand = new System.Random()
    let maxRand,minRand = (1.0 + randomizationFactor), (1.0 - randomizationFactor)
    map (fun x -> (float x) * (rand.NextDouble() * (maxRand - minRand) + minRand) |> int)

  /// Uses a fibonacci sequence to genereate timeout intervals starting from the specified initial interval.
  let fib (initialIntervalMs:int) : Backoff =
    let rec fib n =
      if n < 2 then initialIntervalMs
      else fib (n - 1) + fib (n - 2)
    fib >> checkOverflow >> Some

  /// Creates a stateful back-off strategy which keeps track of the number of attempts,
  /// and a reset function which resets attempts to zero.
  let keepCount (b:Backoff) : (unit -> int option) * (unit -> unit) =
    let i = ref -1
    (fun () -> System.Threading.Interlocked.Increment i |> b),
    (fun () -> i := -1)

  /// Bounds a backoff strategy to a specified maximum number of attempts.
  let maxAttempts (max:int) (b:Backoff) : Backoff =
    fun n -> if n > max then None else b n


  // ------------------------------------------------------------------------------------------------------------------------
  // defaults

  /// 500ms
  let [<Literal>] DefaultInitialIntervalMs = 500

  /// 60000ms
  let [<Literal>] DefaultMaxIntervalMs = 60000

  /// 0.5
  let [<Literal>] DefaultRandomizationFactor = 0.5

  /// 1.5
  let [<Literal>] DefaultMultiplier = 1.5

  /// The default exponential and randomized back-off strategy with a provided initial interval.
  /// DefaultMaxIntervalMs = 60,000
  /// DefaultRandomizationFactor = 0.5
  /// DefaultMultiplier = 1.5
  let DefaultExponentialBoundedRandomizedOf initialInternal =
    exp initialInternal DefaultMultiplier
    |> rand DefaultRandomizationFactor
    |> bound DefaultMaxIntervalMs

  /// The default exponential and randomized back-off strategy.
  /// DefaultInitialIntervalMs = 500
  /// DefaultMaxIntervalMs = 60,000
  /// DefaultRandomizationFactor = 0.5
  /// DefaultMultiplier = 1.5
  let DefaultExponentialBoundedRandomized = DefaultExponentialBoundedRandomizedOf DefaultInitialIntervalMs