module Backend.ContactPreferences

open Domain.ContactPreferences

type Service internal (resolve : Id -> Equinox.Stream<Events.Event, Fold.State>) =

    let update email value : Async<unit> =
        let stream = resolve email
        let command = let (Id email) = email in Update { email = email; preferences = value }
        stream.Transact(interpret command)

    member __.Update(email, value) =
        update email value

    member __.Read(email) =
        let stream = resolve email
        stream.Query id

let create log resolve =
    let resolve id = Equinox.Stream(log, resolve (streamName id), maxAttempts  = 3)
    Service(resolve)
