module Equinox.MessageDb.Integration.OnSync

open Dapper
open Domain
open Equinox.MessageDb
open Swensen.Unquote
open Xunit

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

    let project streamId (state: ContactPreferences.Fold.State) (conn: Npgsql.NpgsqlConnection) = task {
        let! _ = conn.ExecuteAsync(
            "insert into public.contact_preferences (id, many_promotions, little_promotions, product_review, quick_surveys)
             values (@id, @many_promotions, @little_promotions, @product_review, @quick_surveys)
             on conflict (id) do update
             set many_promotions = EXCLUDED.many_promotions,
                 little_promotions = EXCLUDED.little_promotions,
                 product_review = EXCLUDED.product_review,
                 quick_surveys = EXCLUDED.quick_surveys",
            { id = FsCodec.StreamId.toString streamId; many_promotions = state.manyPromotions; little_promotions = state.littlePromotions; product_review = state.productReview; quick_surveys = state.quickSurveys })
        return () }
    let readOne id (conn: Npgsql.NpgsqlConnection) = task {
        let! result = conn.QueryFirstOrDefaultAsync<ContactPreferences>("select * from public.contact_preferences where id = @id", {| id = id |})
        return if obj.ReferenceEquals(result, null)
            then None
            else Some result

    }

module ContactPreferences =
    let fold, initial = ContactPreferences.Fold.fold, ContactPreferences.Fold.initial
    let codec = ContactPreferences.Events.codec
    let createWithOnSync log connection onSync =
        let access = AccessStrategy.LatestKnownEvent
        let caching = Equinox.CachingStrategy.NoCaching
        Category(createContext connection 1, ContactPreferences.Stream.Category, codec, fold, initial, access, caching, onSync)
        |> Equinox.Decider.forStream log
        |> ContactPreferences.create

    let create log connection = createWithOnSync log connection Projection.project



module Tests =
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    [<Fact>]
    let ``The projection is updated immediately`` () = async {
        use conn = new Npgsql.NpgsqlConnection(connectionString)
        Projection.createTable conn
        let connection = connectToLocalStore()
        let service = ContactPreferences.create Serilog.Log.Logger connection
        let id = System.Guid.NewGuid() |> Guid.toStringN
        let clientId = ContactPreferences.ClientId id
        do! service.Update(clientId, { littlePromotions = true; manyPromotions = false; productReview = true; quickSurveys = false })
        let! result = conn |> Projection.readOne id |> Async.AwaitTask
        test <@ result = Some { Projection.id = id; little_promotions = true; many_promotions = false; product_review = true; quick_surveys = false } @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    [<Fact>]
    let ``The projection is not updated when no events are written`` () = async {
        use conn = new Npgsql.NpgsqlConnection(connectionString)
        Projection.createTable conn
        let connection = connectToLocalStore()
        let service = ContactPreferences.create Serilog.Log.Logger connection
        let id = System.Guid.NewGuid() |> Guid.toStringN
        let clientId = ContactPreferences.ClientId id
        // this is the initial state
        do! service.Update(clientId, ContactPreferences.Fold.initial)
        let! result = conn |> Projection.readOne id |> Async.AwaitTask
        test <@ result = None @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    [<Fact>]
    let ``When the projection throws the events do not get written`` () = async {
        use conn = new Npgsql.NpgsqlConnection(connectionString)
        Projection.createTable conn
        let connection = connectToLocalStore()
        let service = ContactPreferences.createWithOnSync Serilog.Log.Logger connection (fun _ _ _ -> task {
            failwith "Test error"
        })
        let id = System.Guid.NewGuid() |> Guid.toStringN
        let clientId = ContactPreferences.ClientId id
        try
            do! service.Update(clientId, { littlePromotions = true; manyPromotions = false; productReview = true; quickSurveys = false })
            failwith "Unexpected success"
        with exn -> test <@  exn.Message = "Test error" @>
        let! result = conn |> Projection.readOne id |> Async.AwaitTask
        test <@ result = None @>
        let! result = service.Read(clientId)
        let! version = service.ReadVersion(clientId)
        test <@ result = ContactPreferences.Fold.initial @>
        test <@ version = 0L @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    [<Fact>]
    let ``The projection is updated every time`` () = async {
        use conn = new Npgsql.NpgsqlConnection(connectionString)
        Projection.createTable conn
        let connection = connectToLocalStore()
        let service = ContactPreferences.create Serilog.Log.Logger connection
        let id = System.Guid.NewGuid() |> Guid.toStringN
        let clientId = ContactPreferences.ClientId id
        do! service.Update(clientId, { littlePromotions = true; manyPromotions = false; productReview = true; quickSurveys = false })
        do! service.Update(clientId, { littlePromotions = true; manyPromotions = true; productReview = true; quickSurveys = false })
        let! result = conn |> Projection.readOne id |> Async.AwaitTask
        test <@ result = Some { Projection.id = id; little_promotions = true; many_promotions = true; product_review = true; quick_surveys = false } @>
    }
