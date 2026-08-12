using System.Reflection;
using System.Text.Json;

namespace SimpleEventStore;

/// <summary>An immutable, explicit mapping between persisted names and event types.</summary>
public sealed class MapBackedEventTypeConverter : IEventTypeConverter
{
    private readonly Dictionary<string, Type> _typesByName;
    private readonly Dictionary<Type, string> _namesByType;

    public MapBackedEventTypeConverter(IReadOnlyDictionary<string, Type> eventTypes)
    {
        ArgumentNullException.ThrowIfNull(eventTypes);

        var byName = new Dictionary<string, Type>(StringComparer.Ordinal);
        var byType = new Dictionary<Type, string>();
        foreach (var (name, type) in eventTypes)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("An event name cannot be empty.", nameof(eventTypes));
            }

            if (!typeof(IEvent).IsAssignableFrom(type) || type.IsInterface || type.IsAbstract)
            {
                throw new ArgumentException($"'{type}' must be a concrete {nameof(IEvent)} type.", nameof(eventTypes));
            }

            if (!byName.TryAdd(name, type))
            {
                throw new ArgumentException($"Duplicate event name '{name}'.", nameof(eventTypes));
            }

            if (!byType.TryAdd(type, name))
            {
                throw new ArgumentException($"Event type '{type}' is registered more than once.", nameof(eventTypes));
            }
        }

        _typesByName = byName;
        _namesByType = byType;
    }

    public string GetEventName(Type eventType)
    {
        ArgumentNullException.ThrowIfNull(eventType);
        return _namesByType.TryGetValue(eventType, out var name)
            ? name
            : throw new UnknownEventTypeException(eventType);
    }

    public Type GetEventType(string eventName)
    {
        ArgumentNullException.ThrowIfNull(eventName);
        return _typesByName.TryGetValue(eventName, out var type)
            ? type
            : throw new UnknownEventTypeException(eventName);
    }

    public IReadOnlyCollection<string> GetEventNamesAssignableTo(Type eventType)
    {
        ArgumentNullException.ThrowIfNull(eventType);
        return _typesByName
            .Where(pair => eventType.IsAssignableFrom(pair.Value))
            .Select(static pair => pair.Key)
            .Order(StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>Builds a name map from concrete event types in an assembly.</summary>
    public static IReadOnlyDictionary<string, Type> Scan(
        Assembly assembly,
        Func<Type, string>? eventName = null)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        eventName ??= static type => type.Name;

        var result = new Dictionary<string, Type>(StringComparer.Ordinal);
        foreach (var type in assembly.DefinedTypes
                     .Where(static type => !type.IsAbstract && !type.IsInterface && typeof(IEvent).IsAssignableFrom(type))
                     .OrderBy(static type => type.FullName, StringComparer.Ordinal))
        {
            var name = eventName(type.AsType());
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException($"The event name for '{type.FullName}' cannot be empty.", nameof(eventName));
            }

            if (!result.TryAdd(name, type.AsType()))
            {
                throw new ArgumentException($"Duplicate event name '{name}' discovered in '{assembly.FullName}'.", nameof(assembly));
            }
        }

        return result;
    }
}

/// <summary>System.Text.Json event serialization using an explicit type converter.</summary>
public sealed class JsonEventSerializer : IEventSerializer
{
    private readonly IEventTypeConverter _converter;
    private readonly JsonSerializerOptions _options;

    public JsonEventSerializer(IEventTypeConverter converter, JsonSerializerOptions? options = null)
    {
        _converter = converter ?? throw new ArgumentNullException(nameof(converter));
        _options = options is null
            ? new JsonSerializerOptions(JsonSerializerDefaults.Web)
            : new JsonSerializerOptions(options);
    }

    public IEvent Deserialize(string eventType, string eventJson)
    {
        ArgumentNullException.ThrowIfNull(eventType);
        ArgumentNullException.ThrowIfNull(eventJson);
        var type = _converter.GetEventType(eventType);
        return JsonSerializer.Deserialize(eventJson, type, _options) as IEvent
               ?? throw new JsonException($"JSON for event type '{eventType}' produced a null event.");
    }

    public SerializedEvent Serialize(IEvent eventValue)
    {
        ArgumentNullException.ThrowIfNull(eventValue);
        var type = eventValue.GetType();
        return new SerializedEvent(
            _converter.GetEventName(type),
            JsonSerializer.Serialize(eventValue, type, _options));
    }
}
