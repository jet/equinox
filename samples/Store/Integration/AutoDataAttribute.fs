namespace global

open System

type AutoDataAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(Arbitrary = [| typeof<FsCheckGenerators> |], MaxTest = 1, QuietOnSuccess = true)

    let mutable skipEnvVar: string = null

    member x.SkipIfRequestedViaEnvironmentVariable
        with get () = skipEnvVar
        and set value =
            skipEnvVar <- value
            match Option.ofObj value |> Option.map Environment.GetEnvironmentVariable |> Option.bind Option.ofObj with
            | Some v when v.Equals(bool.TrueString, StringComparison.OrdinalIgnoreCase) ->
                x.Skip <- $"Skipped as requested via %s{value}"
            | _ -> ()
