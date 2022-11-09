namespace Equinox.MessageDb.Core

open FsCodec
open FsCodec.Core
open Npgsql
open NpgsqlTypes
open System
open System.Data.Common
open System.Text.Json
open System.Threading
open System.Threading.Tasks

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type MdbSyncResult = Written of int64 | ConflictUnknown
type private Format = ReadOnlyMemory<byte>

type MessageDbClient(createConnection: CancellationToken -> Task<NpgsqlConnection>) =
    let readonly (bytes: byte array) = ReadOnlyMemory.op_Implicit(bytes)
    let readRow (reader: DbDataReader) =
        let readNullableString idx = if reader.IsDBNull(idx) then None else Some (reader.GetString idx)
        let time = DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)
        let data = reader.GetFieldValue<byte array>(2) |> readonly
        let meta = reader.GetFieldValue<byte array>(3) |> readonly

        let timestamp = DateTimeOffset(time)

        TimelineEvent.Create(
            index = reader.GetInt64(0),
            eventType = reader.GetString(1),
            data = data,
            meta = meta,
            eventId = reader.GetGuid(4),
            ?correlationId = readNullableString 5,
            ?causationId = readNullableString 6,
            timestamp = timestamp)
    let jsonNull = JsonSerializer.SerializeToUtf8Bytes(null)
    member _.ReadLastEvent(streamName : string, ct) = task {
        use! conn = createConnection ct
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
        use! conn = createConnection ct
        use cmd = conn.CreateCommand()

        cmd.CommandText <-
            "select
               position, type, data, metadata, id::uuid,
               (metadata::jsonb->>'$correlationId')::text,
               (metadata::jsonb->>'$causationId')::text,
               time
             from get_stream_messages(@StreamName, @FromPosition, @BatchSize)"

        cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue("FromPosition", NpgsqlDbType.Bigint, fromPosition) |> ignore
        cmd.Parameters.AddWithValue("BatchSize", NpgsqlDbType.Bigint, batchSize) |> ignore

        use reader = cmd.ExecuteReader()

        let events = ResizeArray()

        let! hasNext = reader.ReadAsync(ct)
        let mutable hasNext = hasNext
        while hasNext do
            events.Add(readRow reader)
            let! next = reader.ReadAsync(ct)
            hasNext <- next

        return events.ToArray() }

    member private _.PrepareWriteCommand(streamName : string, version, message : IEventData<Format>) =
        let cmd = NpgsqlBatchCommand()
        cmd.CommandText <- "select 1 from write_message(@Id::text, @StreamName, @EventType, @Data, @Meta, @ExpectedVersion)"

        // Npgsql does not support ReadOnlyMemory<byte>
        // as a json property. It must be a byte[]
        let meta = match message.Meta with m when m.IsEmpty -> jsonNull | m -> m.ToArray()
        let data = message.Data.ToArray()

        cmd.Parameters.AddWithValue("Id", NpgsqlDbType.Uuid, message.EventId) |> ignore
        cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue("EventType", NpgsqlDbType.Text, message.EventType) |> ignore
        cmd.Parameters.AddWithValue("Data", NpgsqlDbType.Jsonb, data) |> ignore
        cmd.Parameters.AddWithValue("Meta", NpgsqlDbType.Jsonb, meta) |> ignore
        cmd.Parameters.AddWithValue("ExpectedVersion", NpgsqlDbType.Bigint, version) |> ignore

        cmd

    member client.WriteMessages(streamName, events : _ array, version : int64, ct) = task {
        try use! conn = createConnection ct
            use transaction = conn.BeginTransaction()
            use batch = new NpgsqlBatch(conn, transaction)

            let addCommand i event =
                client.PrepareWriteCommand(streamName, version + int64 i, event)
                |> batch.BatchCommands.Add
            events |> Array.iteri addCommand

            do! batch.ExecuteNonQueryAsync(ct) :> Task
            do! transaction.CommitAsync(ct)
            return MdbSyncResult.Written (version + int64 events.Length)
        with :? PostgresException as ex when ex.Message.Contains("Wrong expected version") ->
            return MdbSyncResult.ConflictUnknown }
