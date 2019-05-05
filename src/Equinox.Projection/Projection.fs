namespace Equinox.Projection

/// Item from a reader as supplied to the `IIngester`
type [<NoComparison>] StreamItem = { stream: string; index: int64; event: Equinox.Codec.IEvent<byte[]> }