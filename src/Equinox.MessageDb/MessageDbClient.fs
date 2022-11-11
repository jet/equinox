namespace Equinox.MessageDb.Core

open FsCodec
open FsCodec.Core
open Npgsql
open NpgsqlTypes
open System
open System.Data.Common
open System.Text.Json
open System.Threading.Tasks

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type MdbSyncResult = Written of int64 | ConflictUnknown
type private Format = ReadOnlyMemory<byte>

module private Json =
  let jsonNull = JsonSerializer.SerializeToUtf8Bytes(null)
  let toArray (m: Format) = match m.IsEmpty with true -> jsonNull | false -> m.ToArray()

type MessageDbWriter(connectionString: string) =
    member private _.PrepareWriteCommand(streamName : string, version, message : IEventData<Format>) =
            let cmd = NpgsqlBatchCommand()
            cmd.CommandText <- "select 1 from write_message(@Id::text, @StreamName, @EventType, @Data, @Meta, @ExpectedVersion)"

            cmd.Parameters.AddWithValue("Id", NpgsqlDbType.Uuid, message.EventId) |> ignore
            cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
            cmd.Parameters.AddWithValue("EventType", NpgsqlDbType.Text, message.EventType) |> ignore
            cmd.Parameters.AddWithValue("Data", NpgsqlDbType.Jsonb, Json.toArray message.Data) |> ignore
            cmd.Parameters.AddWithValue("Meta", NpgsqlDbType.Jsonb, Json.toArray message.Meta) |> ignore
            cmd.Parameters.AddWithValue("ExpectedVersion", NpgsqlDbType.Bigint, version) |> ignore

            cmd

        member client.WriteMessages(streamName, events : _ array, version : int64, ct) = task {
            try use conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync(ct)
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

type MessageDbReader internal (connectionString: string, leaderConnectionString: string) =
    let readonly (bytes: byte array) = ReadOnlyMemory.op_Implicit(bytes)
    let readRow (reader: DbDataReader) =
        let readNullableString idx = if reader.IsDBNull(idx) then None else Some (reader.GetString idx)
        let timestamp = DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc))

        TimelineEvent.Create(
            index = reader.GetInt64(0),
            eventType = reader.GetString(1),
            data = (reader.GetFieldValue<byte array>(2) |> readonly),
            meta = (reader.GetFieldValue<byte array>(3) |> readonly),
            eventId = reader.GetGuid(4),
            ?correlationId = readNullableString 5,
            ?causationId = readNullableString 6,
            timestamp = timestamp)

    member _.ReadLastEvent(streamName : string, requiresLeader, ct) = task {
        use conn = new NpgsqlConnection(if requiresLeader then leaderConnectionString else connectionString)
        do! conn.OpenAsync(ct)
        use cmd = conn.CreateCommand()
        cmd.CommandText <-
            "select
               position, type, data, metadata, id::uuid,
               (metadata::jsonb->>'$correlationId')::text,
               (metadata::jsonb->>'$causationId')::text,
               time
             from get_last_stream_message(@StreamName);"
        cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
        use! reader = cmd.ExecuteReaderAsync(ct)
        let! hasRow = reader.ReadAsync(ct)
        if hasRow then
            return [| readRow reader |]
        else
            return Array.empty }

    member _.ReadStream(streamName : string, fromPosition : int64, batchSize : int64, requiresLeader, ct) = task {
        use conn = new NpgsqlConnection(if requiresLeader then leaderConnectionString else connectionString)
        do! conn.OpenAsync(ct)
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

        use! reader = cmd.ExecuteReaderAsync(ct)

        let events = ResizeArray()
        let! hasRow = reader.ReadAsync(ct)
        let mutable hasRow = hasRow
        while hasRow do
            events.Add(readRow reader)
            let! nextHasRow = reader.ReadAsync(ct)
            hasRow <- nextHasRow

        return events.ToArray() }

