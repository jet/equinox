#r "paket:
nuget Fake.Core.Target
nuget Fake.IO.FileSystem
nuget Fake.DotNet.MSBuild //"
#load "./.fake/build.fsx/intellisense.fsx"

open Fake.Core
open Fake.DotNet
open Fake.IO
open Fake.IO.Globbing.Operators

let buildDir = "./.build/"

Target.create "Clean" (fun _ ->
    Shell.cleanDir buildDir
)

Target.create "Build" (fun _ ->
    !! "*.sln"
    |> MSBuild.runRelease id buildDir "Build"
    |> Trace.logItems "Build-Output: "
)

Target.runOrDefault "Build"