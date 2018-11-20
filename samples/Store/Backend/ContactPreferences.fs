module Backend.ContactPreferences

type Service(log, resolveStream) =
    let (|Stream|) email =
        let streamName = sprintf "ContactPreferences-%s" email // TODO hash >> base64
        Domain.ContactPreferences.Handler(log, resolveStream streamName)

    member __.Update (Stream stream as email) value =
        stream.Update email value

    member __.Read(Stream stream) =
        stream.Read