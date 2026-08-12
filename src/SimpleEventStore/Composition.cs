using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace SimpleEventStore;

/// <summary>Standalone composition root for an event store.</summary>
public sealed class SimpleEventStoreBuilder
{
    private readonly EventStoreConfiguration _configuration = new();
    private IEventStorage? _storage;

    public SimpleEventStoreBuilder RegisterEvent<TEvent>(string eventName)
        where TEvent : IEvent
    {
        _configuration.Register(eventName, typeof(TEvent));
        return this;
    }

    public SimpleEventStoreBuilder RegisterEventsFromAssembly(
        Assembly assembly,
        Func<Type, string>? eventName = null)
    {
        _configuration.RegisterAssembly(assembly, eventName);
        return this;
    }

    public SimpleEventStoreBuilder ConfigureJson(Action<JsonSerializerOptions> configure)
    {
        _configuration.ConfigureJson(configure);
        return this;
    }

    public SimpleEventStoreBuilder UseInMemoryStorage()
    {
        _storage = new InMemoryEventStorage();
        return this;
    }

    public SimpleEventStoreBuilder UsePostgreSql(NpgsqlDataSource dataSource)
    {
        _storage = new PostgreSqlEventStorage(dataSource);
        return this;
    }

    public SimpleEventStoreBuilder UseStorage(IEventStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        return this;
    }

    public EventStore Build()
    {
        var converter = new MapBackedEventTypeConverter(_configuration.CopyEventTypes());
        var serializer = new JsonEventSerializer(converter, _configuration.CreateJsonOptions());
        return new EventStore(
            _storage ?? new InMemoryEventStorage(),
            serializer,
            converter,
            new ReflectionEventApplier());
    }
}

/// <summary>Configuration used by <see cref="SimpleEventStoreServiceCollectionExtensions"/>.</summary>
public sealed class SimpleEventStoreRegistration
{
    private readonly EventStoreConfiguration _configuration = new();

    internal EventStoreConfiguration Configuration => _configuration;

    internal NpgsqlDataSource? DataSource { get; private set; }

    internal string? PostgreSqlConnectionString { get; private set; }

    public SimpleEventStoreRegistration RegisterEvent<TEvent>(string eventName)
        where TEvent : IEvent
    {
        _configuration.Register(eventName, typeof(TEvent));
        return this;
    }

    public SimpleEventStoreRegistration RegisterEventsFromAssembly(
        Assembly assembly,
        Func<Type, string>? eventName = null)
    {
        _configuration.RegisterAssembly(assembly, eventName);
        return this;
    }

    public SimpleEventStoreRegistration ConfigureJson(Action<JsonSerializerOptions> configure)
    {
        _configuration.ConfigureJson(configure);
        return this;
    }

    public SimpleEventStoreRegistration UseInMemoryStorage()
    {
        DataSource = null;
        PostgreSqlConnectionString = null;
        return this;
    }

    public SimpleEventStoreRegistration UsePostgreSql(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        DataSource = null;
        PostgreSqlConnectionString = connectionString;
        return this;
    }

    public SimpleEventStoreRegistration UsePostgreSql(NpgsqlDataSource dataSource)
    {
        DataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        PostgreSqlConnectionString = null;
        return this;
    }
}

public static class SimpleEventStoreServiceCollectionExtensions
{
    /// <summary>Adds one event store and its selected singleton adapters without initializing a database.</summary>
    public static IServiceCollection AddSimpleEventStore(
        this IServiceCollection services,
        Action<SimpleEventStoreRegistration>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        var registration = new SimpleEventStoreRegistration();
        configure?.Invoke(registration);
        var configuration = registration.Configuration;

        if (registration.DataSource is not null)
        {
            services.AddSingleton(registration.DataSource);
            services.AddSingleton<PostgreSqlEventStorage>();
            services.AddSingleton<IEventStorage>(static provider =>
                provider.GetRequiredService<PostgreSqlEventStorage>());
        }
        else if (registration.PostgreSqlConnectionString is not null)
        {
            var connectionString = registration.PostgreSqlConnectionString;
            services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
            services.AddSingleton<PostgreSqlEventStorage>();
            services.AddSingleton<IEventStorage>(static provider =>
                provider.GetRequiredService<PostgreSqlEventStorage>());
        }
        else
        {
            services.AddSingleton<InMemoryEventStorage>();
            services.AddSingleton<IEventStorage>(static provider =>
                provider.GetRequiredService<InMemoryEventStorage>());
        }

        services.AddSingleton<IEventTypeConverter>(_ =>
            new MapBackedEventTypeConverter(configuration.CopyEventTypes()));
        services.AddSingleton<IEventSerializer>(provider =>
            new JsonEventSerializer(
                provider.GetRequiredService<IEventTypeConverter>(),
                configuration.CreateJsonOptions()));
        services.AddSingleton<IEventApplier, ReflectionEventApplier>();
        services.AddSingleton<EventStore>();
        return services;
    }
}

internal sealed class EventStoreConfiguration
{
    private readonly Dictionary<string, Type> _eventTypes = new(StringComparer.Ordinal);
    private readonly List<Action<JsonSerializerOptions>> _jsonConfiguration = [];

    public void Register(string eventName, Type eventType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventName);
        ArgumentNullException.ThrowIfNull(eventType);
        if (_eventTypes.TryGetValue(eventName, out var existing))
        {
            throw new ArgumentException(
                $"Event name '{eventName}' is already registered for '{existing}'.",
                nameof(eventName));
        }

        if (_eventTypes.ContainsValue(eventType))
        {
            throw new ArgumentException($"Event type '{eventType}' is already registered.", nameof(eventType));
        }

        _eventTypes.Add(eventName, eventType);
    }

    public void RegisterAssembly(Assembly assembly, Func<Type, string>? eventName)
    {
        foreach (var pair in MapBackedEventTypeConverter.Scan(assembly, eventName))
        {
            Register(pair.Key, pair.Value);
        }
    }

    public void ConfigureJson(Action<JsonSerializerOptions> configure) =>
        _jsonConfiguration.Add(configure ?? throw new ArgumentNullException(nameof(configure)));

    public IReadOnlyDictionary<string, Type> CopyEventTypes() =>
        new Dictionary<string, Type>(_eventTypes, StringComparer.Ordinal);

    public JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        foreach (var configure in _jsonConfiguration)
        {
            configure(options);
        }

        return options;
    }
}
