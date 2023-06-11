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
[<Struct>]
type ExpectedVersion = Any | StreamVersion of int64

[<AutoOpen>]
module private Sql =
    type private NpgsqlParameterCollection with
        member this.AddNullableString(value: string option) =
            match value with
            | Some value -> this.AddWithValue(NpgsqlDbType.Text, value)
            | None       -> this.AddWithValue(NpgsqlDbType.Text, DBNull.Value)
        member this.AddExpectedVersion(value: ExpectedVersion) =
            match value with
            | StreamVersion value -> this.AddWithValue(NpgsqlDbType.Bigint, value)
            | Any                 -> this.AddWithValue(NpgsqlDbType.Bigint, DBNull.Value)

        member this.AddJson(value: Format) =
            if value.Length = 0 then this.AddWithValue(NpgsqlDbType.Jsonb, DBNull.Value)
            else this.AddWithValue(NpgsqlDbType.Jsonb, value.ToArray())


module private WriteMessage =
    [<Literal>]
    let writeMessage = "select * from write_message($1::text, $2, $3, $4, $5, $6)"
    let prepareCommand (streamName: string) (expectedVersion: ExpectedVersion) (e: IEventData<Format>) =
        let cmd = NpgsqlBatchCommand(CommandText = writeMessage)

        cmd.Parameters.AddWithValue(NpgsqlDbType.Uuid, e.EventId) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, e.EventType) |> ignore
        cmd.Parameters.AddJson(e.Data) |> ignore
        cmd.Parameters.AddJson(e.Meta) |> ignore
        cmd.Parameters.AddExpectedVersion(expectedVersion) |> ignore

        cmd

module private ReadLast =
    [<Literal>]
    let readLast = "select position, type, data, metadata, id::uuid, time from get_last_stream_message($1, $2);"
    let prepareCommand (streamName: string) (eventType: string option) =
        let cmd = new NpgsqlCommand(CommandText = readLast)
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddNullableString(eventType) |> ignore
        cmd
module private ReadStream =
    [<Literal>]
    let readStream = "select position, type, data, metadata, id::uuid, time from get_stream_messages($1, $2, $3);"
    let prepareCommand (streamName: string) (fromPosition: int64) (batchSize: int64) =
        use cmd = new NpgsqlCommand(CommandText = readStream)
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Bigint, fromPosition) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Bigint, batchSize) |> ignore
        cmd

module private Json =

    let private jsonNull = ReadOnlyMemory(JsonSerializer.SerializeToUtf8Bytes(null))

    let fromReader idx (reader: DbDataReader) =
        if reader.IsDBNull(idx) then jsonNull
        else reader.GetString(idx) |> Text.Encoding.UTF8.GetBytes |> ReadOnlyMemory

module private Npgsql =

    let connect connectionString ct = task {
        let conn = new NpgsqlConnection(connectionString)
        do! conn.OpenAsync(ct)
        return conn }

type internal MessageDbWriter(connectionString: string) =

    member _.WriteMessages(streamName, events: _[], version, ct) = task {
        use! conn = Npgsql.connect connectionString ct
        use transaction = conn.BeginTransaction()
        use batch = new NpgsqlBatch(conn, transaction)
        let toAppendCall i e =
            let expectedVersion = match version with Any -> Any | StreamVersion version -> StreamVersion (version + int64 i)
            WriteMessage.prepareCommand streamName expectedVersion e
        events |> Seq.mapi toAppendCall |> Seq.iter batch.BatchCommands.Add
        try do! batch.ExecuteNonQueryAsync(ct) :> Task
            do! transaction.CommitAsync(ct)
            match version with
            | Any -> return MdbSyncResult.Written(-1L)
            | StreamVersion version -> return MdbSyncResult.Written (version + int64 events.Length)
        with :? PostgresException as ex when ex.Message.Contains("Wrong expected version") ->
            return MdbSyncResult.ConflictUnknown }

type internal MessageDbReader (connectionString: string, leaderConnectionString: string) =

    let connect requiresLeader = Npgsql.connect (if requiresLeader then leaderConnectionString else connectionString)

    let parseRow (reader: DbDataReader): ITimelineEvent<Format> =
        let et, data, meta = reader.GetString(1), reader |> Json.fromReader 2, reader |> Json.fromReader 3
        FsCodec.Core.TimelineEvent.Create(
            index = reader.GetInt64(0),
            eventType = et, data = data, meta = meta, eventId = reader.GetGuid(4),
            timestamp = DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc)),
            size = et.Length + data.Length + meta.Length)

    member _.ReadLastEvent(streamName: string, requiresLeader, ct, ?eventType) = task {
        use! conn = connect requiresLeader ct
        use cmd = ReadLast.prepareCommand streamName eventType
        cmd.Connection <- conn
        use! reader = cmd.ExecuteReaderAsync(ct)

        if reader.Read() then return [| parseRow reader |]
        else return Array.empty }

    member _.ReadStream(streamName: string, fromPosition: int64, batchSize: int64, requiresLeader, ct) = task {
        use! conn = connect requiresLeader ct
        use cmd = ReadStream.prepareCommand streamName fromPosition batchSize
        cmd.Connection <- conn
        use! reader = cmd.ExecuteReaderAsync(ct)

        return [| while reader.Read() do yield parseRow reader |] }
