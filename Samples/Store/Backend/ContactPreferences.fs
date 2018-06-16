module Backend.ContactPreferences

open Domain

type Service(createStream) =
    let stream (ContactPreferences.Id email) =
        sprintf "ContactPreferences-%s" email // TODO hash >> base64
        |> createStream 1 (fun (_eventType : string) -> true)
    let handler log email =
        ContactPreferences.Handler(log, stream (Domain.ContactPreferences.Id email))

    member __.Update log email value =
        let handler = handler log email
        handler.Update email value

    member __.Read log email =
        let handler = handler log email
        handler.Read