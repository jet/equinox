namespace Equinox.MessageDb

open System
open System.Data
open System.Text.Json
open System.Threading.Tasks
open Equinox.Core
open FSharp.Control
open FsCodec
open FsCodec.Core
open Npgsql
open NpgsqlTypes

module Token =
    let create streamVersion : StreamToken =
        { value = box streamVersion
          version = streamVersion
          streamBytes = -1 }

exception WrongExpectedVersion

type MessageDbStore(conn: NpgsqlConnection) =
    let BATCH_SIZE = 1000L
    member private __.ReadStreamBatched(streamName: string, fromPosition: int64) =
        asyncSeq {
                  use cmd = conn.CreateCommand()
                  cmd.CommandText <- "select
                        position, type, data, metadata, id,
                        (metadata->>'$correlationId')::text,
                        (metadata->>'$causationId')::text,
                        time
                    from get_stream_messages(@StreamName, @FromPosition, @BatchSize)"
                  cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
                  cmd.Parameters.AddWithValue("FromPosition", NpgsqlDbType.Bigint, fromPosition) |> ignore
                  cmd.Parameters.AddWithValue("BatchSize", NpgsqlDbType.Bigint, fromPosition) |> ignore
                  use reader = cmd.ExecuteReader()

                  let! hasNext = reader.ReadAsync() |> Async.AwaitTaskCorrect
                  let mutable hasNext = hasNext
                  let mutable count = 0L
                  while hasNext do
                      let timelineEvent = TimelineEvent.Create(
                          index = reader.GetInt64(0),
                          eventType = reader.GetString(1),
                          data = JsonSerializer.Deserialize<JsonElement>(reader.GetString(2)),
                          meta = JsonSerializer.Deserialize<JsonElement>(reader.GetString(3)),
                          eventId = reader.GetGuid(4),
                          correlationId = reader.GetString(5),
                          causationId = reader.GetString(6),
                          timestamp = reader.GetFieldValue<DateTimeOffset>(7).ToUniversalTime()
                          )
                      yield timelineEvent
                      let! next= reader.ReadAsync() |> Async.AwaitTaskCorrect
                      hasNext <- next
                      count <- count + 1L
                  if count = BATCH_SIZE then
                      yield! __.ReadStreamBatched(streamName, fromPosition + count)
                }

    member __.ReadStream(streamName: string, fromPosition: int64) =
      async {
        let! events = __.ReadStreamBatched(streamName, fromPosition) |> AsyncSeq.toArrayAsync
        let version =
          match Array.tryLast events with
          | Some ev -> ev.Index
          | None -> -1L

        return struct(version, events)
      }

    member private _.PrepareWriteCommand (streamName: string) version index (message: IEventData<JsonElement>) =
                    let cmd = conn.CreateCommand()
                    cmd.CommandText <- "select 1 from write_message(@Id, @StreamName, @EventType, @Data, @Meta, @ExpectedVersion)"
                    cmd.Parameters.AddWithValue("Id", NpgsqlDbType.Uuid, message.EventId) |> ignore
                    cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
                    cmd.Parameters.AddWithValue("EventType", NpgsqlDbType.Text, message.EventType) |> ignore
                    cmd.Parameters.AddWithValue("Data", NpgsqlDbType.Jsonb, message.Data) |> ignore
                    cmd.Parameters.AddWithValue("Meta", NpgsqlDbType.Jsonb, message.Meta) |> ignore
                    cmd.Parameters.AddWithValue("ExpectedVersion", NpgsqlDbType.Jsonb, version + int64 index) |> ignore
                    cmd

    member __.WriteMessage(streamName: string, message: IEventData<JsonElement>, version) =
        async {
            use cmd = __.PrepareWriteCommand streamName version 0 message

            try
              do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTaskCorrect |> Async.Ignore
            with :? Npgsql.PostgresException as e ->
              if e.MessageText.ToLower().Contains("wrong expected version") then
                raise WrongExpectedVersion
              else
                raise e
        }

    member __.WriteMessages(streamName, events, version: int64, ct) =
        task {
            let commands = Array.mapi (__.PrepareWriteCommand streamName version) events
            use! trx = conn.BeginTransactionAsync(ct)
            use batch = conn.CreateBatch()
            for command in commands do
                let c = NpgsqlBatchCommand()
                c.CommandText <- command.CommandText
                c.Parameters.AddRange(command.Parameters.ToArray())

                batch.BatchCommands.Add(c)
            do! batch.ExecuteNonQueryAsync(ct) :> Task
            do! trx.CommitAsync(ct)
        }


type Category<'event, 'state, 'context>(client: MessageDbStore, codec : FsCodec.IEventCodec<_, JsonElement, 'context>, fold, initial) =
    interface ICategory<'event, 'state, 'context> with
        member this.Load(_log, _categoryName, _streamId, streamName, _allowStale, ct) =
            let computation =
                async {
                  let! version, events = client.ReadStream(streamName, 0L)
                  let events = Array.chooseV codec.TryDecode events
                  let token = Token.create version
                  let state = fold initial events
                  return struct(token, state)
                }
            Async.StartAsTask(computation, cancellationToken = ct)

        member this.TrySync(log, categoryName, streamId, streamName, ctx, maybeInit, token, state, events, ct) =
                task {
                    let messsages = events |> Array.mapi (fun i ev -> codec.Encode(ctx, ev))
                    try
                        do! client.WriteMessages(streamName, messsages, token.version, ct)
                        let newToken = Token.create (token.version + int64 events.Length)
                        return SyncResult.Written(struct(newToken, fold state events))
                    with
                    | WrongExpectedVersion ->
                        let resync ct =
                            task {
                                let! version, newEvents =
                                    Async.StartAsTask(client.ReadStream(streamName, token.version + 1L), cancellationToken = ct)
                                let decoded = newEvents |> Array.chooseV codec.TryDecode
                                return struct(Token.create(max token.version version), fold state decoded)
                            }

                        return SyncResult.Conflict(resync)
                }



type MessageDbCategory<'event, 'state, 'context>(resolveInner, empty) =
    inherit Equinox.Category<'event, 'state, 'context>(resolveInner, empty)
    new (store : MessageDbStore, codec : FsCodec.IEventCodec<'event, _, 'context>, fold, initial) =
        let impl = Category<'event, 'state, 'context>(store, codec, fold, initial)
        let resolveInner streamIds = struct (impl :> ICategory<_, _, _>, FsCodec.StreamName.Internal.ofCategoryAndStreamId streamIds, ValueNone)
        let empty = struct (Token.create -1L, initial)
        MessageDbCategory(resolveInner, empty)
