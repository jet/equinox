dotnet build build.proj
dotnet pack build.proj /p:BN=%1 /p:PR=%2
dotnet test build.proj -v n