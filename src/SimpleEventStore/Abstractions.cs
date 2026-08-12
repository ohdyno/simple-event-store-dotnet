namespace SimpleEventStore;

/// <summary>Marks a value as an event that can be persisted by the event store.</summary>
public interface IEvent;

/// <summary>Identifies an event stream.</summary>
public readonly record struct StreamName
{
    public static readonly StreamName Empty = new(string.Empty);

    public StreamName(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Value = value;
    }

    public string Value { get; }

    public override string ToString() => Value;

    public static implicit operator StreamName(string value) => new(value);
}

/// <summary>Identifies a position within one stream.</summary>
public readonly record struct StreamVersion(long Value) : IComparable<StreamVersion>
{
    public static readonly StreamVersion Undefined = new(-1);
    public static readonly StreamVersion First = new(0);
    public static readonly StreamVersion Maximum = new(long.MaxValue);

    public int CompareTo(StreamVersion other) => Value.CompareTo(other.Value);

    public static bool operator <(StreamVersion left, StreamVersion right) => left.Value < right.Value;
    public static bool operator <=(StreamVersion left, StreamVersion right) => left.Value <= right.Value;
    public static bool operator >(StreamVersion left, StreamVersion right) => left.Value > right.Value;
    public static bool operator >=(StreamVersion left, StreamVersion right) => left.Value >= right.Value;

    public override string ToString() => Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
}

/// <summary>Identifies a record in global insertion order.</summary>
public readonly record struct RecordId(long Value) : IComparable<RecordId>
{
    public static readonly RecordId Undefined = new(0);
    public static readonly RecordId First = new(1);
    public static readonly RecordId Maximum = new(long.MaxValue);

    public int CompareTo(RecordId other) => Value.CompareTo(other.Value);

    public static bool operator <(RecordId left, RecordId right) => left.Value < right.Value;
    public static bool operator <=(RecordId left, RecordId right) => left.Value <= right.Value;
    public static bool operator >(RecordId left, RecordId right) => left.Value > right.Value;
    public static bool operator >=(RecordId left, RecordId right) => left.Value >= right.Value;

    public override string ToString() => Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
}

/// <summary>A persisted event and its stream/global metadata.</summary>
public sealed record StoredRecord(
    RecordId Id,
    StreamName StreamName,
    string EventType,
    string EventContent,
    StreamVersion Version,
    DateTimeOffset InsertedOn)
{
    public static readonly StoredRecord Empty = new(
        RecordId.Undefined,
        StreamName.Empty,
        string.Empty,
        string.Empty,
        StreamVersion.Undefined,
        DateTimeOffset.UnixEpoch);
}

/// <summary>Records selected by a query plus the unfiltered latest storage checkpoint.</summary>
public sealed record RetrievedRecords(IReadOnlyList<StoredRecord> Records, StoredRecord LatestRecord)
{
    public static readonly RetrievedRecords Empty = new(Array.Empty<StoredRecord>(), StoredRecord.Empty);
}

/// <summary>Metadata supplied to two-argument event handlers.</summary>
public sealed record RecordDetails(
    StreamName StreamName,
    StreamVersion Version,
    RecordId RecordId,
    DateTimeOffset InsertedOn);

/// <summary>A serialized event name and JSON payload.</summary>
public sealed record SerializedEvent(string EventType, string EventJson);

/// <summary>Persists and queries ordered event records.</summary>
public interface IEventStorage
{
    Task<StoredRecord> AppendAsync(
        StreamName streamName,
        StreamVersion currentVersion,
        string eventType,
        string eventContent,
        CancellationToken cancellationToken = default);

    Task<RetrievedRecords> RetrieveAsync(
        StreamName streamName,
        IReadOnlyCollection<string> eventTypes,
        StreamVersion exclusiveStartVersion,
        StreamVersion inclusiveEndVersion,
        CancellationToken cancellationToken = default);

    Task<RetrievedRecords> RetrieveAsync(
        RecordId exclusiveStartId,
        RecordId inclusiveEndId,
        IReadOnlyCollection<StreamName> streamNames,
        IReadOnlyCollection<string> eventTypes,
        CancellationToken cancellationToken = default);
}

/// <summary>Maps persisted event names to CLR event types.</summary>
public interface IEventTypeConverter
{
    string GetEventName(Type eventType);

    Type GetEventType(string eventName);

    IReadOnlyCollection<string> GetEventNamesAssignableTo(Type eventType);
}

/// <summary>Serializes registered events to and from JSON.</summary>
public interface IEventSerializer
{
    SerializedEvent Serialize(IEvent eventValue);

    IEvent Deserialize(string eventType, string eventJson);
}

/// <summary>Applies an event to a compatible public handler on an entity.</summary>
public interface IEventApplier
{
    void Apply(IEvent eventValue, RecordDetails details, object entity);

    IReadOnlyList<Type> GetHandledEventTypes(object entity);
}
