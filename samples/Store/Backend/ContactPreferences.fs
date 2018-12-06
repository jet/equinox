module Backend.ContactPreferences

type Service(log, resolveStream) =
    let (|CatId|) (email: string) = Equinox.CatId ("ContactPreferences", email) // TODO hash >> base64
    let (|Stream|) (CatId id) = Domain.ContactPreferences.Handler(log, resolveStream id)

    member __.Update (Stream stream as email) value =
        stream.Update email value

    member __.Read(Stream stream) =
        stream.Read