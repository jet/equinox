#r @"packages/FAKE/tools/FakeLib.dll"

open Fake.Core
open Fake.IO
open Fake.IO.Globbing.Operators


let buildDir = "./.build/"

Target.create "Clean" (fun _ ->
    Shell.cleanDir buildDir
)

Target.create "Build" (fun _ ->
    !! "*.sln"
    |> Fake.DotNet.MSBuild.runRelease id buildDir "Build"
    |> Trace.logItems "Build-Output: "
)

Target.runOrDefault "Build"