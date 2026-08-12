using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace SimpleEventStore.Tests;

public sealed class SerializationAndCompositionTests
{
    [Fact]
    public void Json_round_trip_uses_camel_case_defaults()
    {
        var converter = new MapBackedEventTypeConverter(new Dictionary<string, Type>
        {
            ["sample"] = typeof(SampleEvent),
        });
        var serializer = new JsonEventSerializer(converter);

        var serialized = serializer.Serialize(new SampleEvent("value", 42));
        var roundTrip = serializer.Deserialize(serialized.EventType, serialized.EventJson);

        Assert.Equal("sample", serialized.EventType);
        Assert.Contains("\"someValue\"", serialized.EventJson, StringComparison.Ordinal);
        Assert.Equal(new SampleEvent("value", 42), roundTrip);
    }

    [Fact]
    public void Unknown_names_and_types_have_typed_failures()
    {
        var converter = new MapBackedEventTypeConverter(new Dictionary<string, Type>
        {
            ["sample"] = typeof(SampleEvent),
        });

        Assert.Throws<UnknownEventTypeException>(() => converter.GetEventName(typeof(UnregisteredEvent)));
        Assert.Throws<UnknownEventTypeException>(() => converter.GetEventType("unknown"));
    }

    [Fact]
    public void Explicit_mapping_rejects_one_type_under_multiple_names()
    {
        Assert.Throws<ArgumentException>(() => new MapBackedEventTypeConverter(new Dictionary<string, Type>
        {
            ["one"] = typeof(SampleEvent),
            ["two"] = typeof(SampleEvent),
        }));
    }

    [Fact]
    public void Assembly_scan_registers_events_and_rejects_duplicate_simple_names()
    {
        var assembly = Assembly.GetExecutingAssembly();

        var fullyQualified = MapBackedEventTypeConverter.Scan(assembly, static type => type.FullName!);

        Assert.Contains(typeof(SampleEvent).FullName!, fullyQualified.Keys);
        Assert.Throws<ArgumentException>(() => MapBackedEventTypeConverter.Scan(assembly));
    }

    [Fact]
    public async Task Standalone_builder_defaults_to_memory_and_honors_json_configuration()
    {
        using var store = new SimpleEventStoreBuilder()
            .RegisterEvent<SampleEvent>("sample")
            .ConfigureJson(static options => options.PropertyNamingPolicy = null)
            .Build();
        StoredRecord? stored = null;
        using var subscription = store.Records.Subscribe(new CallbackObserver<StoredRecord>(record => stored = record));

        await store.SaveAsync(
            new SampleEvent("value", 42),
            new SampleAggregate("sample-1"),
            TestContext.Current.CancellationToken);

        Assert.NotNull(stored);
        Assert.Contains("\"SomeValue\"", stored.EventContent, StringComparison.Ordinal);
    }

    [Fact]
    public void Dependency_injection_registers_one_default_graph()
    {
        var services = new ServiceCollection();
        services.AddSimpleEventStore(options => options.RegisterEvent<SampleEvent>("sample"));
        using var provider = services.BuildServiceProvider();

        Assert.Same(provider.GetRequiredService<EventStore>(), provider.GetRequiredService<EventStore>());
        Assert.IsType<InMemoryEventStorage>(provider.GetRequiredService<IEventStorage>());
        Assert.IsType<JsonEventSerializer>(provider.GetRequiredService<IEventSerializer>());
        Assert.IsType<MapBackedEventTypeConverter>(provider.GetRequiredService<IEventTypeConverter>());
        Assert.IsType<ReflectionEventApplier>(provider.GetRequiredService<IEventApplier>());
    }

    [Fact]
    public async Task Interface_apply_method_filters_to_registered_implementations()
    {
        using var store = new SimpleEventStoreBuilder()
            .RegisterEvent<InterfaceEvent>("interface-event")
            .Build();
        await store.SaveAsync(
            new InterfaceEvent("handled"),
            new InterfaceAggregate("stream"),
            TestContext.Current.CancellationToken);

        var projection = await store.EnrichAsync(
            new InterfaceProjection(),
            TestContext.Current.CancellationToken);

        Assert.Equal(["handled"], projection.Values);
    }

    public sealed record SampleEvent(string SomeValue, int Count) : IEvent;
    public sealed record UnregisteredEvent : IEvent;

    public interface IContractEvent : IEvent
    {
        string Value { get; }
    }

    public sealed record InterfaceEvent(string Value) : IContractEvent;

    private sealed class SampleAggregate(StreamName streamName) : AggregateBase
    {
        public override StreamName StreamName { get; } = streamName;
        public void Apply(SampleEvent _) { }
    }

    private sealed class InterfaceAggregate(StreamName streamName) : AggregateBase
    {
        public override StreamName StreamName { get; } = streamName;
        public void Apply(InterfaceEvent _) { }
    }

    private sealed class InterfaceProjection : ProjectionBase
    {
        public List<string> Values { get; } = [];
        public void Apply(IContractEvent @event) => Values.Add(@event.Value);
    }

    public static class FirstContainer
    {
        public sealed record DuplicateEvent : IEvent;
    }

    public static class SecondContainer
    {
        public sealed record DuplicateEvent : IEvent;
    }
}

internal sealed class CallbackObserver<T>(Action<T> onNext) : IObserver<T>
{
    public void OnCompleted() { }

    public void OnError(Exception error) => throw error;

    public void OnNext(T value) => onNext(value);
}
