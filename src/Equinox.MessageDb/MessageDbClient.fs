namespace Equinox.MessageDb

open System
open System.Text.Json
open System.Threading.Tasks
open FsCodec
open FsCodec.Core
open Npgsql
open NpgsqlTypes

exception WrongExpectedVersion

type MessageDbClient(source: unit -> Task<NpgsqlConnection>) =
    member __.ReadStream(streamName: string, fromPosition: int64, batchSize: int64, ct) =
        task {
            use! conn = source()
            use cmd = conn.CreateCommand()

            cmd.CommandText <-
                "select
                   position, type, data, metadata, id::uuid,
                   (metadata::jsonb->>'$correlationId')::text,
                   (metadata::jsonb->>'$causationId')::text,
                   time
                 from get_stream_messages(@StreamName, @FromPosition, @BatchSize)"

            cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName)
            |> ignore

            cmd.Parameters.AddWithValue("FromPosition", NpgsqlDbType.Bigint, fromPosition)
            |> ignore

            cmd.Parameters.AddWithValue("BatchSize", NpgsqlDbType.Bigint, batchSize)
            |> ignore

            use reader = cmd.ExecuteReader()

            let! hasNext = reader.ReadAsync(ct)
            let events = ResizeArray()
            let mutable hasNext = hasNext

            while hasNext do
                let meta =
                    if reader.IsDBNull(3) then
                        "null"
                    else
                        reader.GetString(3)

                let time =
                    DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)

                let timestamp = DateTimeOffset(time)

                let timelineEvent =
                    TimelineEvent.Create(
                        index = reader.GetInt64(0),
                        eventType = reader.GetString(1),
                        data = JsonSerializer.Deserialize<JsonElement>(reader.GetString(2)),
                        meta = JsonSerializer.Deserialize<JsonElement>(meta),
                        eventId = reader.GetGuid(4),
                        ?correlationId =
                            (if reader.IsDBNull(5) then
                                 None
                             else
                                 Some(reader.GetString(5))),
                        ?causationId =
                            (if reader.IsDBNull(6) then
                                 None
                             else
                                 Some(reader.GetString(6))),
                        timestamp = timestamp
                    )

                events.Add(timelineEvent)
                let! next = reader.ReadAsync(ct)
                hasNext <- next

            return events.ToArray()
        }

    member private _.PrepareWriteCommand
        (cmd: NpgsqlBatchCommand)
        (streamName: string)
        version
        (message: IEventData<JsonElement>)
        =
        cmd.CommandText <- "select 1 from write_message(@Id::text, @StreamName, @EventType, @Data, @Meta, @ExpectedVersion)"

        cmd.Parameters.AddWithValue("Id", NpgsqlDbType.Uuid, message.EventId) |> ignore
        cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue("EventType", NpgsqlDbType.Text, message.EventType) |> ignore
        let data, meta = message.Data, message.Meta
        cmd.Parameters.AddWithValue("Data", NpgsqlDbType.Jsonb, data.GetRawText()) |> ignore
        let meta =
            match meta.ValueKind with
            | JsonValueKind.Null | JsonValueKind.Undefined -> "null"
            | _ -> meta.GetRawText()
        cmd.Parameters.AddWithValue("Meta", NpgsqlDbType.Jsonb, meta) |> ignore
        cmd.Parameters.AddWithValue("ExpectedVersion", NpgsqlDbType.Bigint, version) |> ignore

        cmd

    member __.WriteMessages(streamName, events, version: int64, ct) =
        task {
            try
                use! conn = source ()
                use transaction = conn.BeginTransaction()
                use batch = new NpgsqlBatch(conn, transaction)

                let mutable expectedVersion = version

                for event in events do
                    batch.BatchCommands.Add(
                        __.PrepareWriteCommand (NpgsqlBatchCommand()) streamName expectedVersion event
                    )

                    expectedVersion <- expectedVersion + 1L

                do! batch.ExecuteNonQueryAsync(ct) :> Task
                do! transaction.CommitAsync(ct)
            with :? PostgresException as ex ->
                if ex.Message.Contains("Wrong expected version") then
                    raise WrongExpectedVersion
                else
                    raise ex
        }
