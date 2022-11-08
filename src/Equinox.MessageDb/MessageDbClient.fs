namespace Equinox.MessageDb

open System
open System.Data.Common
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsCodec
open FsCodec.Core
open Npgsql
open NpgsqlTypes

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type MdbSyncResult = Written of int64 | ConflictUnknown

type MessageDbClient(source: CancellationToken -> Task<NpgsqlConnection>) =
    let readRow (reader: DbDataReader) =
        let meta =
            if reader.IsDBNull(3) then
                "null"
            else
                reader.GetString(3)

        let time =
            DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)

        let timestamp = DateTimeOffset(time)

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
    member _.ReadLastEvent(streamName : string, ct) = task {
        use! conn = source ct
        use cmd = conn.CreateCommand()
        cmd.CommandText <-
            "select
               position, type, data, metadata, id::uuid,
               (metadata::jsonb->>'$correlationId')::text,
               (metadata::jsonb->>'$causationId')::text,
               time
             from get_last_stream_message(@StreamName);"
        cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
        use reader = cmd.ExecuteReader()

        let! hasRow = reader.ReadAsync(ct)
        if hasRow then
            return ValueSome(readRow reader)
        else
            return ValueNone
    }
    member _.ReadStream(streamName : string, fromPosition : int64, batchSize : int64, ct) = task {
        use! conn = source ct
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
            events.Add(readRow reader)
            let! next = reader.ReadAsync(ct)
            hasNext <- next

        return events.ToArray() }

    member private _.PrepareWriteCommand(streamName : string, version, message : IEventData<JsonElement>) =
        let cmd = NpgsqlBatchCommand()
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

    member __.WriteMessages(streamName, events : _ array, version : int64, ct) = task {
        try
            use! conn = source ct
            use transaction = conn.BeginTransaction()
            use batch = new NpgsqlBatch(conn, transaction)

            let prep i event = batch.BatchCommands.Add(__.PrepareWriteCommand(streamName, version + int64 i, event))
            events |> Array.iteri prep

            do! batch.ExecuteNonQueryAsync(ct) :> Task
            do! transaction.CommitAsync(ct)
            return MdbSyncResult.Written(version + int64 events.Length)
        with :? PostgresException as ex when ex.Message.Contains("Wrong expected version") ->
            return MdbSyncResult.ConflictUnknown }
