using SimpleEventStore;
using Xunit;

namespace SimpleEventStore.ContractTests;

public abstract class StorageContractTests : IAsyncLifetime
{
    protected abstract IEventStorage Storage { get; }

    public virtual ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public async Task Creates_a_new_stream_at_version_zero()
    {
        var record = await Storage.AppendAsync(
            "orders-1",
            StreamVersion.Undefined,
            "placed",
            "{\"id\": 1}",
            TestContext.Current.CancellationToken);

        Assert.Equal(RecordId.First, record.Id);
        Assert.Equal(StreamVersion.First, record.Version);
        Assert.Equal(new StreamName("orders-1"), record.StreamName);
        Assert.Equal("placed", record.EventType);
        Assert.Equal("{\"id\": 1}", record.EventContent);
        Assert.True(record.InsertedOn > DateTimeOffset.UnixEpoch);
    }

    [Fact]
    public async Task Rejects_duplicate_missing_and_stale_stream_operations()
    {
        await Assert.ThrowsAsync<StreamNotFoundException>(() =>
            Storage.AppendAsync(
                "missing",
                StreamVersion.First,
                "event",
                "{}",
                TestContext.Current.CancellationToken));

        await Storage.AppendAsync(
            "orders-1",
            StreamVersion.Undefined,
            "placed",
            "{}",
            TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<DuplicateStreamException>(() =>
            Storage.AppendAsync(
                "orders-1",
                StreamVersion.Undefined,
                "event",
                "{}",
                TestContext.Current.CancellationToken));
        await Storage.AppendAsync(
            "orders-1",
            StreamVersion.First,
            "event",
            "{}",
            TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<StaleStreamVersionException>(() =>
            Storage.AppendAsync(
                "orders-1",
                StreamVersion.First,
                "event",
                "{}",
                TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<StreamNotFoundException>(() =>
            Storage.RetrieveAsync(
                "missing",
                Array.Empty<string>(),
                StreamVersion.Undefined,
                StreamVersion.Maximum,
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Empty_global_query_has_undefined_latest_metadata()
    {
        var result = await Storage.RetrieveAsync(
            RecordId.Undefined,
            RecordId.Maximum,
            Array.Empty<StreamName>(),
            Array.Empty<string>(),
            TestContext.Current.CancellationToken);

        Assert.Empty(result.Records);
        Assert.Equal(StoredRecord.Empty, result.LatestRecord);
    }

    [Fact]
    public async Task Stream_ranges_are_exclusive_at_start_inclusive_at_end_and_ordered()
    {
        var records = await SeedOneStreamAsync();

        var result = await Storage.RetrieveAsync(
            "orders-1",
            Array.Empty<string>(),
            records[0].Version,
            records[1].Version,
            TestContext.Current.CancellationToken);

        Assert.Equal([records[1]], result.Records);
        Assert.Equal(records[^1], result.LatestRecord);

        var edge = await Storage.RetrieveAsync(
            "orders-1",
            Array.Empty<string>(),
            records[1].Version,
            records[0].Version,
            TestContext.Current.CancellationToken);
        Assert.Empty(edge.Records);
        Assert.Equal(records[^1], edge.LatestRecord);
    }

    [Fact]
    public async Task Stream_type_filters_do_not_change_latest_metadata()
    {
        var records = await SeedOneStreamAsync();

        var selected = await Storage.RetrieveAsync(
            "orders-1",
            ["renamed"],
            StreamVersion.Undefined,
            StreamVersion.Maximum,
            TestContext.Current.CancellationToken);
        var excluded = await Storage.RetrieveAsync(
            "orders-1",
            ["unknown"],
            StreamVersion.Undefined,
            StreamVersion.Maximum,
            TestContext.Current.CancellationToken);

        Assert.Equal([records[1]], selected.Records);
        Assert.Empty(excluded.Records);
        Assert.Equal(records[^1], excluded.LatestRecord);
    }

    [Fact]
    public async Task Global_ranges_filters_and_order_match_the_java_contract()
    {
        var records = await SeedTwoStreamsAsync();

        var all = await Storage.RetrieveAsync(
            RecordId.Undefined,
            RecordId.Maximum,
            Array.Empty<StreamName>(),
            Array.Empty<string>(),
            TestContext.Current.CancellationToken);
        var combined = await Storage.RetrieveAsync(
            records[0].Id,
            records[4].Id,
            ["orders-1"],
            ["renamed"],
            TestContext.Current.CancellationToken);

        Assert.Equal(records, all.Records);
        Assert.Equal(records[^1], all.LatestRecord);
        Assert.Equal(
            records.Where(record =>
                record.Id > records[0].Id
                && record.Id <= records[4].Id
                && record.StreamName == new StreamName("orders-1")
                && record.EventType == "renamed"),
            combined.Records);
        Assert.Equal(records[^1], combined.LatestRecord);
    }

    [Fact]
    public async Task Empty_filters_are_wildcards_and_equal_or_reversed_ranges_are_empty()
    {
        var records = await SeedTwoStreamsAsync();

        var oneStream = await Storage.RetrieveAsync(
            RecordId.Undefined,
            RecordId.Maximum,
            ["orders-2"],
            Array.Empty<string>(),
            TestContext.Current.CancellationToken);
        var equal = await Storage.RetrieveAsync(
            records[1].Id,
            records[1].Id,
            Array.Empty<StreamName>(),
            Array.Empty<string>(),
            TestContext.Current.CancellationToken);
        var reversed = await Storage.RetrieveAsync(
            records[2].Id,
            records[1].Id,
            Array.Empty<StreamName>(),
            Array.Empty<string>(),
            TestContext.Current.CancellationToken);

        Assert.Equal(records.Where(record => record.StreamName == new StreamName("orders-2")), oneStream.Records);
        Assert.Empty(equal.Records);
        Assert.Empty(reversed.Records);
        Assert.Equal(records[^1], equal.LatestRecord);
        Assert.Equal(records[^1], reversed.LatestRecord);
    }

    [Fact]
    public async Task Exactly_one_parallel_append_wins_without_versions_or_id_gaps()
    {
        await Storage.AppendAsync(
            "orders-1",
            StreamVersion.Undefined,
            "placed",
            "{}",
            TestContext.Current.CancellationToken);

        var contenders = Enumerable.Range(0, 24).Select(async index =>
        {
            try
            {
                return (Record: await Storage.AppendAsync(
                    "orders-1",
                    StreamVersion.First,
                    "renamed",
                    $"{{\"index\":{index}}}",
                    TestContext.Current.CancellationToken), Exception: (Exception?)null);
            }
            catch (Exception exception)
            {
                return (Record: (StoredRecord?)null, Exception: exception);
            }
        });

        var results = await Task.WhenAll(contenders);
        Assert.Single(results, static result => result.Record is not null);
        Assert.Equal(23, results.Count(static result => result.Exception is StaleStreamVersionException));

        var stored = await Storage.RetrieveAsync(
            "orders-1",
            Array.Empty<string>(),
            StreamVersion.Undefined,
            StreamVersion.Maximum,
            TestContext.Current.CancellationToken);
        Assert.Equal([0L, 1L], stored.Records.Select(static record => record.Version.Value));
        Assert.Equal([1L, 2L], stored.Records.Select(static record => record.Id.Value));
    }

    [Fact]
    public async Task Honors_pre_cancelled_tokens()
    {
        using var source = new CancellationTokenSource();
        await source.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            Storage.AppendAsync("orders-1", StreamVersion.Undefined, "placed", "{}", source.Token));
    }

    private async Task<StoredRecord[]> SeedOneStreamAsync()
    {
        var first = await Storage.AppendAsync(
            "orders-1",
            StreamVersion.Undefined,
            "placed",
            "{}",
            TestContext.Current.CancellationToken);
        var second = await Storage.AppendAsync(
            "orders-1",
            first.Version,
            "renamed",
            "{}",
            TestContext.Current.CancellationToken);
        var third = await Storage.AppendAsync(
            "orders-1",
            second.Version,
            "placed",
            "{}",
            TestContext.Current.CancellationToken);
        return [first, second, third];
    }

    private async Task<StoredRecord[]> SeedTwoStreamsAsync()
    {
        var records = new List<StoredRecord>();
        var versions = new Dictionary<StreamName, StreamVersion>();
        for (var version = 0; version < 3; version++)
        {
            foreach (StreamName stream in new[] { new StreamName("orders-1"), new StreamName("orders-2") })
            {
                var current = versions.GetValueOrDefault(stream, StreamVersion.Undefined);
                var type = version == 1 ? "renamed" : "placed";
                var record = await Storage.AppendAsync(
                    stream,
                    current,
                    type,
                    "{}",
                    TestContext.Current.CancellationToken);
                versions[stream] = record.Version;
                records.Add(record);
            }
        }

        return records.ToArray();
    }
}
