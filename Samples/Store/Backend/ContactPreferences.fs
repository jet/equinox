module Backend.ContactPreferences

open Domain

type Service(createStream) =
    static let codec = Foldunk.EventSum.generateJsonUtf8SumEncoder<ContactPreferences.Events.Event>
    let streamName (ContactPreferences.Id email) = sprintf "ContactPreferences-%s" email // TODO hash >> base64
    let handler id = ContactPreferences.Handler(streamName id |> createStream codec)

    member __.Update (log : Serilog.ILogger) email value =
        let handler = handler (Domain.ContactPreferences.Id email)
        handler.Update log email value

    member __.Load (log : Serilog.ILogger) email =
        let handler = handler (Domain.ContactPreferences.Id email)
        handler.Load log