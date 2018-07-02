module Backend.ContactPreferences

open Domain

type Service(log, resolveStream) =
    let stream (ContactPreferences.Id email) =
        sprintf "ContactPreferences-%s" email // TODO hash >> base64
        |> resolveStream 1 (fun (_eventType : string) -> true)
    let (|ContactPreferences|) email =
        ContactPreferences.Handler(log, stream (Domain.ContactPreferences.Id email))

    member __.Update (ContactPreferences handler as email) value =
        handler.Update email value

    member __.Read(ContactPreferences handler) =
        handler.Read