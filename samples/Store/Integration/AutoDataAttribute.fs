namespace global

open System

type AutoDataAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(Arbitrary = [|typeof<FsCheckGenerators>|], MaxTest = 1, QuietOnSuccess = true)

    member val SkipIfRequestedViaEnvironmentVariable: string = null with get, set

    member x.SkipRequested =
        match Option.ofObj x.SkipIfRequestedViaEnvironmentVariable |> Option.map Environment.GetEnvironmentVariable |> Option.bind Option.ofObj with
        | Some value when value.Equals(bool.TrueString, StringComparison.OrdinalIgnoreCase) -> true
        | _ -> false
    override x.Skip = if x.SkipRequested then $"Skipped as requested via %s{x.SkipIfRequestedViaEnvironmentVariable}" else null
