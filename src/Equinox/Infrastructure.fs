namespace Equinox

/// A backoff strategy.
/// Accepts the attempt number and returns an interval in milliseconds to wait.
/// If None then backoff should stop.
type Backoff = int -> int option

module private Backoff =

  let private checkOverflow x =
    if x = System.Int32.MinValue then 2000000000
    else x

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
  let defaultExponentialBoundedRandomizedOf initialInternal =
    exp initialInternal DefaultMultiplier
    |> rand DefaultRandomizationFactor
    |> bound DefaultMaxIntervalMs

  /// The default exponential and randomized back-off strategy.
  /// DefaultInitialIntervalMs = 500
  /// DefaultMaxIntervalMs = 60,000
  /// DefaultRandomizationFactor = 0.5
  /// DefaultMultiplier = 1.5
  let defaultExponentialBoundedRandomized = defaultExponentialBoundedRandomizedOf DefaultInitialIntervalMs