namespace Equinox.MessageDb.Core

open FsCodec
open Npgsql
open NpgsqlTypes
open System
open System.Data.Common
open System.Text.Json
open System.Threading.Tasks

type private Format = ReadOnlyMemory<byte>

[<Struct>]
type ExpectedVersion = Any | StreamVersion of int64

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type MdbSyncResult = Written of int64 | ConflictUnknown

[<AutoOpen>]
module private NpgsqlHelpers =

    let createConnectionAndOpen connectionString ct = task {
        let conn = new NpgsqlConnection(connectionString)
        do! conn.OpenAsync(ct)
        return conn }

    type NpgsqlParameterCollection with
        member p.AddParameter<'T>(parameterType: NpgsqlDbType, value: 'T voption) =
            p.AddWithValue(parameterType, match value with ValueSome v -> box v | ValueNone -> DBNull.Value) |> ignore
        member p.AddNullableString(value: string option) =
            p.AddParameter(NpgsqlDbType.Text, match value with Some x -> ValueSome x | None -> ValueNone)
        member p.AddExpectedVersion(value: ExpectedVersion) =
            p.AddParameter(NpgsqlDbType.Bigint, match value with StreamVersion v -> ValueSome v | Any -> ValueNone)
        member p.AddJson(value: Format) =
            p.AddParameter(NpgsqlDbType.Jsonb, if value.Length <> 0 then value.ToArray() |> ValueSome else ValueNone)

    let private jsonNull = ReadOnlyMemory(JsonSerializer.SerializeToUtf8Bytes(null))

    type DbDataReader with
        member reader.GetJson idx =
            if reader.IsDBNull(idx) then jsonNull
            else reader.GetString(idx) |> Text.Encoding.UTF8.GetBytes |> ReadOnlyMemory

module private WriteMessage =
    [<Literal>]
    let writeMessage = "select * from write_message($1::text, $2, $3, $4, $5, $6)"
    let prepareCommand (streamName: string) (expectedVersion: ExpectedVersion) (e: IEventData<Format>) =
        let cmd = NpgsqlBatchCommand(writeMessage)
        cmd.Parameters.AddWithValue(NpgsqlDbType.Uuid, e.EventId) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, e.EventType) |> ignore
        cmd.Parameters.AddJson(e.Data)
        cmd.Parameters.AddJson(e.Meta)
        cmd.Parameters.AddExpectedVersion(expectedVersion)
        cmd

type internal MessageDbWriter(connectionString: string) =

    member _.WriteMessages(streamName, events: _[], version, onSync, ct) = task {
        use! conn = createConnectionAndOpen connectionString ct
        use transaction = conn.BeginTransaction()
        use batch = new NpgsqlBatch(conn, transaction)
        let toAppendCall i e =
            let expectedVersion = match version with Any -> Any | StreamVersion version -> StreamVersion (version + int64 i)
            WriteMessage.prepareCommand streamName expectedVersion e
        events |> Seq.mapi toAppendCall |> Seq.iter batch.BatchCommands.Add
        try do! batch.ExecuteNonQueryAsync(ct) :> Task
            match onSync with
            | Some fn -> do! fn conn
            | None -> ()
            do! transaction.CommitAsync(ct)
            match version with
            | Any -> return MdbSyncResult.Written(-1L)
            | StreamVersion version -> return MdbSyncResult.Written (version + int64 events.Length)
        with :? PostgresException as ex when ex.Message.Contains("Wrong expected version") ->
            return MdbSyncResult.ConflictUnknown }

module private ReadLast =
    [<Literal>]
    let readLast = "select position, type, data, metadata, id::uuid, time from get_last_stream_message($1, $2);"
    let prepareCommand connection (streamName: string) (eventType: string option) =
        let cmd = new NpgsqlCommand(readLast, connection)
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddNullableString(eventType)
        cmd

module private ReadStream =
    [<Literal>]
    let readStream = "select position, type, data, metadata, id::uuid, time from get_stream_messages($1, $2, $3);"
    let prepareCommand connection (streamName: string) (fromPosition: int64) (batchSize: int64) =
        let cmd = new NpgsqlCommand(readStream, connection)
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, streamName) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Bigint, fromPosition) |> ignore
        cmd.Parameters.AddWithValue(NpgsqlDbType.Bigint, batchSize) |> ignore
        cmd

type internal MessageDbReader (connectionString: string, leaderConnectionString: string) =

    let connect requiresLeader = createConnectionAndOpen (if requiresLeader then leaderConnectionString else connectionString)

    let parseRow (reader: DbDataReader): ITimelineEvent<Format> =
        let et, data, meta = reader.GetString(1), reader.GetJson(2), reader.GetJson(3)
        FsCodec.Core.TimelineEvent.Create(
            index = reader.GetInt64(0),
            eventType = et, data = data, meta = meta, eventId = reader.GetGuid(4),
            timestamp = DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc)),
            size = et.Length + data.Length + meta.Length)

    member _.ReadLastEvent(streamName: string, requiresLeader, ct, ?eventType) = task {
        use! conn = connect requiresLeader ct
        use cmd = ReadLast.prepareCommand conn streamName eventType
        use! reader = cmd.ExecuteReaderAsync(ct)

        if reader.Read() then return [| parseRow reader |]
        else return Array.empty }

    member _.ReadStream(streamName: string, fromPosition: int64, batchSize: int64, requiresLeader, ct) = task {
        use! conn = connect requiresLeader ct
        use cmd = ReadStream.prepareCommand conn streamName fromPosition batchSize
        use! reader = cmd.ExecuteReaderAsync(ct)

        return [| while reader.Read() do parseRow reader |] }
