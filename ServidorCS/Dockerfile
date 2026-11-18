FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["ServidorCS/ServidorCS.csproj", "ServidorCS/"]
RUN dotnet restore "./ServidorCS/ServidorCS.csproj"
COPY ServidorCS/ ServidorCS/
WORKDIR "/src/ServidorCS"
RUN dotnet publish "./ServidorCS.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "ServidorCS.dll"]
