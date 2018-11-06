namespace Web

open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Serilog
open System

type Store() =
    member __.GetStream() = failwith "TODO"

type Startup(configuration: IConfiguration) =
    let configureMvc (s : IServiceCollection) =
        s.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1) |> ignore
    let configureApp (services : IServiceCollection) =
        let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

        services.AddSingleton<Store>() |> ignore

        regF <| fun sp -> Backend.Favorites.Service(sp.GetService<ILogger>(), sp.GetService<Store>().GetStream())

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