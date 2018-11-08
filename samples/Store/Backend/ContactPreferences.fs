module Backend.ContactPreferences

type Service(log, resolveStream) =
    let (|ContactPreferences|) email =
        let streamName = sprintf "ContactPreferences-%s" email // TODO hash >> base64
        Domain.ContactPreferences.Handler(log, resolveStream 1 (fun (_eventType : string) -> true) streamName)

    member __.Update (ContactPreferences handler as email) value =
        handler.Update email value

    member __.Read(ContactPreferences handler) =
        handler.Read