module Backend.ContactPreferences

open Domain.ContactPreferences

type Service(log, resolve, ?maxAttempts) =

    let resolve (Events.ForClientId streamId) = Equinox.Stream(log, resolve streamId, defaultArg maxAttempts 3)

    let update email value : Async<unit> =
        let stream = resolve email
        let command = Update { email = email; preferences = value }
        stream.Transact(Commands.interpret command)

    member __.Update email value =
        update email value

    member __.Read(email) =
        let stream = resolve email
        stream.Query id