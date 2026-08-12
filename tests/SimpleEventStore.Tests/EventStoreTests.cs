using System.Reactive.Linq;
using Xunit;

namespace SimpleEventStore.Tests;

public sealed class EventStoreTests
{
    [Fact]
    public async Task Save_applies_advances_and_then_publishes()
    {
        using var store = CreateStore();
        var aggregate = new OrderAggregate("orders-1");
        var observed = new List<StoredRecord>();
        using var subscription = store.Records.Subscribe(record =>
        {
            Assert.Equal(record.Version, aggregate.Version);
            Assert.True(aggregate.IsEnriched);
            Assert.Equal("first", aggregate.Name);
            observed.Add(record);
        });

        var result = await store.SaveAsync(
            new OrderPlaced("first"),
            aggregate,
            TestContext.Current.CancellationToken);

        Assert.Same(aggregate, result);
        Assert.Equal(StreamVersion.First, aggregate.Version);
        Assert.Single(observed);
    }

    [Fact]
    public async Task Records_replays_only_the_latest_saved_record_to_late_subscribers()
    {
        using var store = CreateStore();
        var aggregate = new OrderAggregate("orders-1");
        await store.SaveAsync(new OrderPlaced("first"), aggregate, TestContext.Current.CancellationToken);
        await store.SaveAsync(new OrderRenamed("second"), aggregate, TestContext.Current.CancellationToken);

        StoredRecord? replayed = null;
        using var subscription = store.Records.Take(1).Subscribe(record => replayed = record);

        Assert.NotNull(replayed);
        Assert.Equal(new StreamVersion(1), replayed.Version);
    }

    [Fact]
    public async Task Failed_save_is_translated_and_not_published()
    {
        using var store = CreateStore();
        await store.SaveAsync(
            new OrderPlaced("first"),
            new OrderAggregate("orders-1"),
            TestContext.Current.CancellationToken);
        var observed = new List<StoredRecord>();
        using var subscription = store.Records.Subscribe(observed.Add);

        var exception = await Assert.ThrowsAsync<StaleAggregateStateException>(() =>
            store.SaveAsync(
                new OrderPlaced("duplicate"),
                new OrderAggregate("orders-1"),
                TestContext.Current.CancellationToken));

        Assert.IsType<DuplicateStreamException>(exception.InnerException);
        Assert.Single(observed);
    }

    [Fact]
    public async Task Handler_failure_does_not_publish_the_appended_record()
    {
        using var store = CreateStore();
        var observed = new List<StoredRecord>();
        using var subscription = store.Records.Subscribe(observed.Add);

        await Assert.ThrowsAsync<DomainFailureException>(() =>
            store.SaveAsync(
                new OrderPlaced("first"),
                new ThrowingAggregate("orders-1"),
                TestContext.Current.CancellationToken));

        Assert.Empty(observed);
    }

    [Fact]
    public async Task Aggregate_rehydrates_only_after_its_checkpoint_and_tracks_unfiltered_latest_version()
    {
        using var store = CreateStore();
        var saved = new OrderAggregate("orders-1");
        await store.SaveAsync(new OrderPlaced("first"), saved, TestContext.Current.CancellationToken);
        await store.SaveAsync(new OrderRenamed("second"), saved, TestContext.Current.CancellationToken);

        var aggregate = new RenameOnlyAggregate("orders-1");
        var result = await store.EnrichAsync(aggregate, TestContext.Current.CancellationToken);

        Assert.Same(aggregate, result);
        Assert.True(aggregate.IsEnriched);
        Assert.Equal("second", aggregate.Name);
        Assert.Equal(new StreamVersion(1), aggregate.Version);
    }

    [Fact]
    public async Task Filtered_aggregate_advances_to_latest_without_becoming_enriched()
    {
        using var store = CreateStore();
        await store.SaveAsync(
            new OrderPlaced("first"),
            new OrderAggregate("orders-1"),
            TestContext.Current.CancellationToken);

        var aggregate = await store.EnrichAsync(
            new RenameOnlyAggregate("orders-1"),
            TestContext.Current.CancellationToken);

        Assert.False(aggregate.IsEnriched);
        Assert.Equal(StreamVersion.First, aggregate.Version);
    }

    [Fact]
    public async Task Missing_aggregate_stream_throws_domain_exception()
    {
        using var store = CreateStore();

        var exception = await Assert.ThrowsAsync<StreamNotFoundException>(() =>
            store.EnrichAsync(new OrderAggregate("missing"), TestContext.Current.CancellationToken));

        Assert.Equal(new StreamName("missing"), exception.StreamName);
        Assert.IsType<StreamNotFoundException>(exception.InnerException);
    }

    [Fact]
    public async Task Projection_catches_up_with_details_and_updates_global_checkpoint()
    {
        using var store = CreateStore();
        var aggregate = new OrderAggregate("orders-1");
        await store.SaveAsync(new OrderPlaced("first"), aggregate, TestContext.Current.CancellationToken);
        await store.SaveAsync(new OrderRenamed("second"), aggregate, TestContext.Current.CancellationToken);

        var projection = await store.EnrichAsync(new OrderProjection(), TestContext.Current.CancellationToken);

        Assert.True(projection.IsEnriched);
        Assert.Equal(["first", "second"], projection.Names);
        Assert.Equal(new RecordId(2), projection.LastRecordId);
        Assert.Equal(projection.LastRecordId, projection.Details[^1].RecordId);
        Assert.True(projection.LastUpdatedOn > DateTimeOffset.UnixEpoch);
    }

    [Fact]
    public async Task Projection_filters_by_stream_and_advances_past_excluded_records()
    {
        using var store = CreateStore();
        await store.SaveAsync(
            new OrderPlaced("one"),
            new OrderAggregate("orders-1"),
            TestContext.Current.CancellationToken);
        await store.SaveAsync(
            new OrderPlaced("two"),
            new OrderAggregate("orders-2"),
            TestContext.Current.CancellationToken);

        var projection = await store.EnrichAsync(
            new OneStreamProjection("orders-2"),
            TestContext.Current.CancellationToken);

        Assert.Equal(["two"], projection.Names);
        Assert.Equal(new RecordId(2), projection.LastRecordId);
    }

    [Fact]
    public async Task Projection_with_no_matching_type_still_advances_checkpoint()
    {
        using var store = CreateStore();
        await store.SaveAsync(
            new OrderPlaced("one"),
            new OrderAggregate("orders-1"),
            TestContext.Current.CancellationToken);

        var projection = await store.EnrichAsync(new RenameProjection(), TestContext.Current.CancellationToken);

        Assert.False(projection.IsEnriched);
        Assert.Equal(RecordId.First, projection.LastRecordId);
        Assert.True(projection.LastUpdatedOn > DateTimeOffset.UnixEpoch);
    }

    [Fact]
    public async Task Cancellation_is_forwarded_without_mutating_aggregate_or_publication()
    {
        using var store = CreateStore();
        using var source = new CancellationTokenSource();
        await source.CancelAsync();
        var aggregate = new OrderAggregate("orders-1");
        var observed = new List<StoredRecord>();
        using var subscription = store.Records.Subscribe(observed.Add);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            store.SaveAsync(new OrderPlaced("first"), aggregate, source.Token));

        Assert.Equal(StreamVersion.Undefined, aggregate.Version);
        Assert.False(aggregate.IsEnriched);
        Assert.Empty(observed);
    }

    private static EventStore CreateStore() => new SimpleEventStoreBuilder()
        .RegisterEvent<OrderPlaced>("order-placed")
        .RegisterEvent<OrderRenamed>("order-renamed")
        .Build();
}
public sealed record OrderPlaced(string Name) : IEvent;
public sealed record OrderRenamed(string Name) : IEvent;

public class OrderAggregate(StreamName streamName) : AggregateBase
{
    public override StreamName StreamName { get; } = streamName;

    public string? Name { get; private set; }

    public void Apply(OrderPlaced @event) => Name = @event.Name;

    public void Apply(OrderRenamed @event) => Name = @event.Name;
}

public sealed class RenameOnlyAggregate(StreamName streamName) : AggregateBase
{
    public override StreamName StreamName { get; } = streamName;

    public string? Name { get; private set; }

    public void Apply(OrderRenamed @event) => Name = @event.Name;
}

public sealed class ThrowingAggregate(StreamName streamName) : AggregateBase
{
    public override StreamName StreamName { get; } = streamName;

    public void Apply(OrderPlaced _) => throw new DomainFailureException();
}

public sealed class DomainFailureException : Exception;

public class OrderProjection : ProjectionBase
{
    public List<string> Names { get; } = [];
    public List<RecordDetails> Details { get; } = [];

    public void Apply(OrderPlaced @event, RecordDetails details)
    {
        Names.Add(@event.Name);
        Details.Add(details);
    }

    public void Apply(OrderRenamed @event, RecordDetails details)
    {
        Names.Add(@event.Name);
        Details.Add(details);
    }
}

public sealed class OneStreamProjection(StreamName streamName) : OrderProjection
{
    public override IReadOnlyCollection<StreamName> StreamNames { get; } = [streamName];
}

public sealed class RenameProjection : ProjectionBase
{
    public void Apply(OrderRenamed _) { }
}
