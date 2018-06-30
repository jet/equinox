module Backend.ContactPreferences

open Domain

type Service(resolveStream) =
    let stream (ContactPreferences.Id email) =
        sprintf "ContactPreferences-%s" email // TODO hash >> base64
        |> resolveStream 1 (fun (_eventType : string) -> true)
    let go log email =
        ContactPreferences.Handler(log, stream (Domain.ContactPreferences.Id email))

    member __.Update log email value =
        let handler = go log email
        handler.Update email value

    member __.Read log email =
        let handler = go log email
        handler.Read