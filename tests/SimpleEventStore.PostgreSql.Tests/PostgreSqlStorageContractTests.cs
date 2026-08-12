using System.Collections;
using Npgsql;
using SimpleEventStore.ContractTests;
using Testcontainers.PostgreSql;
using Xunit;

namespace SimpleEventStore.PostgreSql.Tests;

[Collection(PostgreSqlTestGroup.Name)]
public sealed class PostgreSqlStorageContractTests(PostgreSqlFixture fixture) : StorageContractTests
{
    protected override IEventStorage Storage => fixture.Storage;

    public override async ValueTask InitializeAsync()
    {
        await fixture.Storage.InitializeSchemaAsync(TestContext.Current.CancellationToken);
        await using var command = fixture.DataSource.CreateCommand(
            "TRUNCATE TABLE eventsource.events RESTART IDENTITY;");
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Schema_initialization_is_explicit_and_idempotent()
    {
        await fixture.Storage.InitializeSchemaAsync(TestContext.Current.CancellationToken);
        await fixture.Storage.InitializeSchemaAsync(TestContext.Current.CancellationToken);

        await using var command = fixture.DataSource.CreateCommand(
            "SELECT to_regclass('eventsource.events')::text;");
        var table = await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);

        Assert.Equal("eventsource.events", table);
    }

    [Fact]
    public async Task Stream_records_do_not_advance_past_the_captured_latest_record()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var first = await fixture.Storage.AppendAsync(
            "orders-1",
            StreamVersion.Undefined,
            "placed",
            "{}",
            cancellationToken);
        using var eventTypes = new BlockingReadOnlyCollection<string>(cancellationToken);

        var retrieval = fixture.Storage.RetrieveAsync(
            "orders-1",
            eventTypes,
            StreamVersion.Undefined,
            StreamVersion.Maximum,
            cancellationToken);
        await eventTypes.EnumerationStarted.WaitAsync(cancellationToken);
        StoredRecord second;
        try
        {
            second = await fixture.Storage.AppendAsync(
                "orders-1",
                first.Version,
                "renamed",
                "{}",
                cancellationToken);
        }
        finally
        {
            eventTypes.Release();
        }

        var result = await retrieval;

        Assert.Equal(first, result.LatestRecord);
        Assert.Equal([first], result.Records);
        Assert.DoesNotContain(second, result.Records);
    }

    [Fact]
    public async Task Global_records_do_not_advance_past_the_captured_latest_record()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var first = await fixture.Storage.AppendAsync(
            "orders-1",
            StreamVersion.Undefined,
            "placed",
            "{}",
            cancellationToken);
        using var streamNames = new BlockingReadOnlyCollection<StreamName>(cancellationToken);

        var retrieval = fixture.Storage.RetrieveAsync(
            RecordId.Undefined,
            RecordId.Maximum,
            streamNames,
            Array.Empty<string>(),
            cancellationToken);
        await streamNames.EnumerationStarted.WaitAsync(cancellationToken);
        StoredRecord second;
        try
        {
            second = await fixture.Storage.AppendAsync(
                "orders-2",
                StreamVersion.Undefined,
                "placed",
                "{}",
                cancellationToken);
        }
        finally
        {
            streamNames.Release();
        }

        var result = await retrieval;

        Assert.Equal(first, result.LatestRecord);
        Assert.Equal([first], result.Records);
        Assert.DoesNotContain(second, result.Records);
    }
}

internal sealed class BlockingReadOnlyCollection<T>(CancellationToken cancellationToken)
    : IReadOnlyCollection<T>, IDisposable
{
    private readonly CancellationToken _cancellationToken = cancellationToken;
    private readonly ManualResetEventSlim _release = new();
    private readonly TaskCompletionSource<bool> _enumerationStarted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Count => 0;

    public Task EnumerationStarted => _enumerationStarted.Task;

    public void Release() => _release.Set();

    public IEnumerator<T> GetEnumerator()
    {
        _enumerationStarted.TrySetResult(true);
        _release.Wait(_cancellationToken);
        return Enumerable.Empty<T>().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Dispose() => _release.Dispose();
}

[CollectionDefinition(Name)]
public sealed class PostgreSqlTestGroup : ICollectionFixture<PostgreSqlFixture>
{
    public const string Name = "PostgreSQL integration";
}

public sealed class PostgreSqlFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public PostgreSqlEventStorage Storage { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync(TestContext.Current.CancellationToken);
        DataSource = NpgsqlDataSource.Create(_container.GetConnectionString());
        Storage = new PostgreSqlEventStorage(DataSource);
        await Storage.InitializeSchemaAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (DataSource is not null)
        {
            await DataSource.DisposeAsync();
        }

        await _container.DisposeAsync();
    }
}
