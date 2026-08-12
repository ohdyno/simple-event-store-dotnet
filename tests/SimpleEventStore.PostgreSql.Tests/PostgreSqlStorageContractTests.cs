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
        await fixture.Storage.InitializeSchemaAsync();
        await using var command = fixture.DataSource.CreateCommand(
            "TRUNCATE TABLE eventsource.events RESTART IDENTITY;");
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Schema_initialization_is_explicit_and_idempotent()
    {
        await fixture.Storage.InitializeSchemaAsync();
        await fixture.Storage.InitializeSchemaAsync();

        await using var command = fixture.DataSource.CreateCommand(
            "SELECT to_regclass('eventsource.events')::text;");
        var table = await command.ExecuteScalarAsync();

        Assert.Equal("eventsource.events", table);
    }
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
        await _container.StartAsync();
        DataSource = NpgsqlDataSource.Create(_container.GetConnectionString());
        Storage = new PostgreSqlEventStorage(DataSource);
        await Storage.InitializeSchemaAsync();
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
