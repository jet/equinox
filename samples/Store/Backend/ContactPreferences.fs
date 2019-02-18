module Backend.ContactPreferences

open Domain.ContactPreferences

type Service(log, resolveStream, ?maxAttempts) =
    let (|AggregateId|) (email: string) = Equinox.AggregateId ("ContactPreferences", email) // TODO hash >> base64
    let (|Stream|) (AggregateId id) = Equinox.Stream(log, resolveStream id, defaultArg maxAttempts 3)
    let update (Stream stream as email) value : Async<unit> =
        let command = Update { email = email; preferences = value }
        stream.Transact(Commands.interpret command)

    member __.Update email value =
        update email value

    member __.Read(Stream stream) =
        stream.Query id