namespace Web

open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Serilog
open System

open global.Store.CompositionRoot

type Startup(_configuration: IConfiguration) =
    let configureMvc (s : IServiceCollection) =
        s.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1) |> ignore
    let configureApp (services : IServiceCollection) =
        let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

        let store = failwith "TODO parse cmdline"
        services.AddSingleton<Store>() |> ignore

        let useCache, useUnfolds = false, false
        services.AddSingleton(Builder(store, useCache, useUnfolds)) |> ignore

        let mkFavorites (log: ILogger, builder: Builder) =
            let fold, initial, snapshot = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial, Domain.Favorites.Folds.snapshot
            let codec = genCodec<Domain.Favorites.Events.Event>()
            Backend.Favorites.Service(log, builder.ResolveStream(codec,fold,initial,snapshot))
        regF <| fun sp -> mkFavorites (sp.GetService(), sp.GetService())

        let mkSaves (log: ILogger, builder: Builder) =
            let fold, initial, snapshot = Domain.SavedForLater.Folds.fold, Domain.SavedForLater.Folds.initial, Domain.SavedForLater.Folds.snapshot
            let codec = genCodec<Domain.SavedForLater.Events.Event>()
            Backend.SavedForLater.Service(log, builder.ResolveStream(codec,fold,initial,snapshot), maxSavedItems=50, maxAttempts=3)
        regF <| fun sp -> mkSaves (sp.GetService(), sp.GetService())

    //member val Configuration : IConfiguration = null with get, set

    // This method gets called by the runtime. Use this method to add services to the container.
    member __.ConfigureServices(services: IServiceCollection) =
        configureMvc services
        configureApp services

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    member __.Configure(app: IApplicationBuilder, env: IHostingEnvironment) =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection()
            .UseMvc() |> ignore