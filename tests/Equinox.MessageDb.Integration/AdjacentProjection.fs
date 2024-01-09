module Equinox.MessageDb.Integration.AdjacentProjection

open Domain
open Equinox.MessageDb
open Dapper
open Xunit
open Swensen.Unquote

let defaultBatchSize = 500
let connectionString = "Host=localhost; Username=postgres; Password=; Database=message_store; Port=5432; Maximum Pool Size=10; Search Path=message_store, public"
let connectToLocalStore _ =
  MessageDbClient(connectionString)
type Context = MessageDbContext
type Category<'event, 'state, 'req> = MessageDbCategory<'event, 'state, 'req>

let createContext connection batchSize = Context(connection, batchSize = batchSize)


module Projection =
    [<CLIMutable>]
    type ContactPreferences = { id: string; many_promotions: bool; little_promotions: bool; product_review: bool; quick_surveys: bool }
    let createTable (conn: Npgsql.NpgsqlConnection) =
        conn.Execute("create table if not exists public.contact_preferences (
            id text not null primary key,
            many_promotions bool not null default false,
            little_promotions bool not null default false,
            product_review bool not null default false,
            quick_surveys bool not null default false
        )") |> ignore

    let project (conn: Npgsql.NpgsqlConnection) streamId (state: ContactPreferences.Fold.State) = task {
        let! _ = conn.ExecuteAsync(
            "insert into public.contact_preferences (id, many_promotions, little_promotions, product_review, quick_surveys)
             values (@id, @many_promotions, @little_promotions, @product_review, @quick_surveys)",
            { id = FsCodec.StreamId.toString streamId; many_promotions = state.manyPromotions; little_promotions = state.littlePromotions; product_review = state.productReview; quick_surveys = state.quickSurveys })
        return () }
    let readOne id (conn: Npgsql.NpgsqlConnection) =
        conn.QuerySingleAsync<ContactPreferences>("select * from public.contact_preferences where id = @id", {| id = id |})

module ContactPreferences =
    let fold, initial = ContactPreferences.Fold.fold, ContactPreferences.Fold.initial
    let codec = ContactPreferences.Events.codec
    let create log connection =
        let access = AccessStrategy.AdjacentProjection(Projection.project, AccessStrategy.LatestKnownEvent)
        let caching = Equinox.CachingStrategy.NoCaching
        Category(createContext connection 1, ContactPreferences.Stream.Category, codec, fold, initial, access, caching)
        |> Equinox.Decider.forStream log
        |> ContactPreferences.create


module Tests =
    [<Fact>]
    let ``The projection is updated immediately`` () = async {
        use conn = new Npgsql.NpgsqlConnection(connectionString)
        Projection.createTable conn
        let connection = connectToLocalStore()
        let service = ContactPreferences.create Serilog.Log.Logger connection
        let id = System.Guid.NewGuid().ToString("N")
        let clientId = ContactPreferences.ClientId id
        do! service.Update(clientId, { littlePromotions = true; manyPromotions = false; productReview = true; quickSurveys = false })
        let! result = conn |> Projection.readOne id |> Async.AwaitTask
        test <@ result = { Projection.id = id; little_promotions = true; many_promotions = false; product_review = true; quick_surveys = false } @>
    }
