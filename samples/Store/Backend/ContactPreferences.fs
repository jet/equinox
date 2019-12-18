module Backend.ContactPreferences

open Domain.ContactPreferences

type Service(log, resolve, ?maxAttempts) =

    let (|Stream|) (Events.ForClientId streamId) = Equinox.Stream(log, resolve streamId, defaultArg maxAttempts 3)

    let update (Stream stream as email) value : Async<unit> =
        let command = Update { email = email; preferences = value }
        stream.Transact(Commands.interpret command)

    member __.Update email value =
        update email value

    member __.Read(Stream stream) =
        stream.Query id