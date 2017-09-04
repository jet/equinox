#r "packages/build/FAKE/tools/FakeLib.dll"
open Fake

let buildDir = "./.build/"

Target "Clean" (fun _ ->
    CleanDir buildDir
)

Target "Build" (fun _ ->
    !! "*.sln"
    |> MSBuildRelease buildDir "Build"
    |> Log "Build-Output: "
)

RunTargetOrDefault "Build"