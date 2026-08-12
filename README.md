# SimpleEventStore for .NET

`SimpleEventStore` is a small, asynchronous event store for .NET 10. It keeps the stream and PostgreSQL behavior of the Java `simple-event-store` 2.1.1 project while exposing cancellation-aware C# APIs, `System.Text.Json` serialization, an in-memory adapter, an Npgsql adapter, and Microsoft dependency-injection registration in one package.

## Semantics

- A new stream is appended with `StreamVersion.Undefined` (`-1`) and starts at version `0`.
- Existing streams use optimistic concurrency: the supplied version must be the current version.
- Stream/global query starts are exclusive and ends are inclusive.
- Empty stream-name or event-type filters are wildcards.
- Global record IDs start at `1`; projection checkpoints start at `0`.
- An aggregate starts at stream version `-1`. A projection starts at record ID `0` and `DateTimeOffset.UnixEpoch`.
- Query results are ordered and include the latest unfiltered record as checkpoint metadata, even when filters return no records.
- `Records` publishes only after persistence, event application, and state advancement succeed. It replays the latest successful save to late subscribers.

## Define events and entities

Handlers must be public instance methods named `Apply`. They can accept an event alone or an event plus `RecordDetails`. Inherited handlers and interface/base-event handlers are supported. When several handlers match, the most specific event parameter wins, then the one-argument overload.

```csharp
using SimpleEventStore;

public sealed record AccountOpened(string Owner) : IEvent;
public sealed record MoneyDeposited(decimal Amount) : IEvent;

public sealed class Account(StreamName id) : AggregateBase
{
    public override StreamName StreamName { get; } = id;
    public string? Owner { get; private set; }
    public decimal Balance { get; private set; }

    public void Apply(AccountOpened e) => Owner = e.Owner;
    public void Apply(MoneyDeposited e) => Balance += e.Amount;
}

public sealed class AccountTotals : ProjectionBase
{
    public decimal Deposited { get; private set; }

    public void Apply(MoneyDeposited e, RecordDetails details) =>
        Deposited += e.Amount;
}
```

## Standalone construction

Event registration is explicit. Persisted names are stable contracts and do not have to match CLR type names.

```csharp
using var store = new SimpleEventStoreBuilder()
    .RegisterEvent<AccountOpened>("account-opened")
    .RegisterEvent<MoneyDeposited>("money-deposited")
    .ConfigureJson(options => options.WriteIndented = false)
    .UseInMemoryStorage()
    .Build();
```

You can also register all concrete `IEvent` types in an assembly. Their simple type names are used by default, and duplicate names are rejected. Supply a naming function when fully-qualified or versioned names are required.

```csharp
var store = new SimpleEventStoreBuilder()
    .RegisterEventsFromAssembly(typeof(AccountOpened).Assembly, type => $"v1:{type.Name}")
    .Build();
```

## Save and rehydrate

```csharp
var account = new Account("account-42");
await store.SaveAsync(new AccountOpened("Ada"), account, cancellationToken);
await store.SaveAsync(new MoneyDeposited(25m), account, cancellationToken);

var rehydrated = await store.EnrichAsync(
    new Account("account-42"),
    cancellationToken);
```

`SaveAsync` translates a duplicate stream or stale expected version into `StaleAggregateStateException`. Rehydrating an unknown aggregate stream throws `StreamNotFoundException`.

## Catch up a projection

```csharp
var totals = await store.EnrichAsync(new AccountTotals(), cancellationToken);

// A second call reads strictly after this checkpoint.
Console.WriteLine(totals.LastRecordId);
Console.WriteLine(totals.LastUpdatedOn);
```

Override `ProjectionBase.StreamNames` to limit a projection to selected streams. Event-type filtering is inferred from its public `Apply` methods. A filtered projection still moves to the latest global checkpoint, preventing it from repeatedly scanning excluded records.

## Subscribe to saved records

```csharp
using var subscription = store.Records.Subscribe(record =>
    Console.WriteLine($"{record.Id}: {record.StreamName}/{record.Version}"));
```

The observable is synchronized for concurrent producers and keeps a one-item replay buffer. A subscriber that
throws from a notification is detached without failing the save, blocking other subscribers, or receiving later
records.

## Dependency injection

```csharp
using Microsoft.Extensions.DependencyInjection;

services.AddSimpleEventStore(options => options
    .RegisterEvent<AccountOpened>("account-opened")
    .RegisterEvent<MoneyDeposited>("money-deposited")
    .UseInMemoryStorage());

// EventStore and all its adapters are registered once (singleton lifetime).
```

For PostgreSQL:

```csharp
services.AddSimpleEventStore(options => options
    .RegisterEventsFromAssembly(typeof(AccountOpened).Assembly)
    .UsePostgreSql(configuration.GetConnectionString("EventStore")!));
```

DI registration never changes the database. Initialize it explicitly during deployment or application startup:

```csharp
var storage = serviceProvider.GetRequiredService<PostgreSqlEventStorage>();
await storage.InitializeSchemaAsync(cancellationToken);
```

Standalone PostgreSQL composition accepts an existing `NpgsqlDataSource`, so its owner controls connection pooling and disposal:

```csharp
await using var dataSource = Npgsql.NpgsqlDataSource.Create(connectionString);
var storage = new PostgreSqlEventStorage(dataSource);
await storage.InitializeSchemaAsync(cancellationToken);

using var store = new SimpleEventStoreBuilder()
    .RegisterEvent<AccountOpened>("account-opened")
    .UseStorage(storage)
    .Build();
```

The embedded, idempotent schema uses `eventsource.events` with the Java-compatible columns, JSONB payload, `(stream_name, version)` uniqueness, and latest-event views. Concurrent writers are serialized per stream inside a transaction before version validation, so losing optimistic-concurrency contenders do not consume record IDs.

## Build and test

The repository pins dependencies centrally and commits NuGet lock files. Docker must be running for full verification.

```shell
# Fast suite: unit tests plus the in-memory storage contract
dotnet test tests/SimpleEventStore.Tests/SimpleEventStore.Tests.csproj

# PostgreSQL 17 Testcontainers contract (requires Docker)
dotnet test tests/SimpleEventStore.PostgreSql.Tests/SimpleEventStore.PostgreSql.Tests.csproj

# Full solution, including PostgreSQL integration tests
dotnet test

# Produce the NuGet package
dotnet pack --configuration Release
```

All projects enable nullable reference types, deterministic builds, recommended analyzers, and warnings-as-errors.

## Scope

This package intentionally excludes snapshots, upcasting, batch appends, deletion, custom metadata, durable external subscriptions, and service hosting.
