namespace Equinox.MessageDb.Core

open FsCodec
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

    let private jsonNull = ReadOnlyMemory(JsonSerializer.SerializeToUtf8Bytes(null))

    let fromReader idx (reader: DbDataReader) =
        if reader.IsDBNull(idx) then jsonNull
        else reader.GetString(idx) |> Text.Encoding.UTF8.GetBytes |> ReadOnlyMemory

    let addParameter (name: string) (value: Format) (p: NpgsqlParameterCollection) =
        if value.Length = 0 then p.AddWithValue(name, NpgsqlDbType.Jsonb, DBNull.Value) |> ignore
        else p.AddWithValue(name, NpgsqlDbType.Jsonb, value.ToArray()) |> ignore

module private Npgsql =

    let connect connectionString ct = task {
        let conn = new NpgsqlConnection(connectionString)
        do! conn.OpenAsync(ct)
        return conn }

type MessageDbWriter(connectionString : string) =

    let prepareAppend (streamName : string) (expectedVersion : int64) (e : IEventData<Format>) =
        let cmd = NpgsqlBatchCommand(CommandText = "select 1 from write_message(@Id::text, @StreamName, @EventType, @Data, @Meta, @ExpectedVersion)")

        cmd.Parameters.AddWithValue("Id", NpgsqlDbType.Uuid, e.EventId) |> ignore
        cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue("EventType", NpgsqlDbType.Text, e.EventType) |> ignore
        cmd.Parameters |> Json.addParameter "Data" e.Data
        cmd.Parameters |> Json.addParameter "Meta" e.Meta
        cmd.Parameters.AddWithValue("ExpectedVersion", NpgsqlDbType.Bigint, expectedVersion) |> ignore

        cmd

    member _.WriteMessages(streamName, events : _ array, version : int64, ct) = task {
        use! conn = Npgsql.connect connectionString ct
        use transaction = conn.BeginTransaction()
        use batch = new NpgsqlBatch(conn, transaction)
        let toAppendCall i e =
            let expectedVersion = version + int64 i
            prepareAppend streamName expectedVersion e
        events |> Seq.mapi toAppendCall |> Seq.iter batch.BatchCommands.Add
        try do! batch.ExecuteNonQueryAsync(ct) :> Task
            do! transaction.CommitAsync(ct)
            return MdbSyncResult.Written (version + int64 events.Length)
        with :? PostgresException as ex when ex.Message.Contains("Wrong expected version") ->
            return MdbSyncResult.ConflictUnknown }

type MessageDbReader internal (connectionString : string, leaderConnectionString : string) =

    let connect requiresLeader = Npgsql.connect (if requiresLeader then leaderConnectionString else connectionString)

    let parseRow (reader : DbDataReader) : ITimelineEvent<Format> =
        let inline readNullableString idx = if reader.IsDBNull(idx) then None else Some (reader.GetString idx)
        let et, data, meta = reader.GetString(1), reader |> Json.fromReader 2, reader |> Json.fromReader 3
        FsCodec.Core.TimelineEvent.Create(
            index = reader.GetInt64(0),
            eventType = et, data = data, meta = meta, eventId = reader.GetGuid(4),
            ?correlationId = readNullableString 5, ?causationId = readNullableString 6,
            timestamp = DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)),
            size = et.Length + data.Length + meta.Length)

    member _.ReadLastEvent(streamName : string, requiresLeader, ct) = task {
        use! conn = connect requiresLeader ct
        use cmd = conn.CreateCommand(CommandText =
            "select
               position, type, data, metadata, id::uuid,
               (metadata::jsonb->>'$correlationId')::text,
               (metadata::jsonb->>'$causationId')::text,
               time
             from get_last_stream_message(@StreamName)")
        cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
        use! reader = cmd.ExecuteReaderAsync(ct)

        if reader.Read() then return [| parseRow reader |]
        else return Array.empty }

    member _.ReadStream(streamName : string, fromPosition : int64, batchSize : int64, requiresLeader, ct) = task {
        use! conn = connect requiresLeader ct
        use cmd = conn.CreateCommand(CommandText =
            "select
               position, type, data, metadata, id::uuid,
               (metadata::jsonb->>'$correlationId')::text,
               (metadata::jsonb->>'$causationId')::text,
               time
             from get_stream_messages(@StreamName, @FromPosition, @BatchSize)")
        cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue("FromPosition", NpgsqlDbType.Bigint, fromPosition) |> ignore
        cmd.Parameters.AddWithValue("BatchSize", NpgsqlDbType.Bigint, batchSize) |> ignore
        use! reader = cmd.ExecuteReaderAsync(ct)

        return [| while reader.Read() do yield parseRow reader |] }
