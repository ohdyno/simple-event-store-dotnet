namespace SimpleEventStore;

/// <summary>Common state exposed by event-sourced entities.</summary>
public interface IEventSourceEntity
{
    bool IsEnriched { get; }

    void MarkEnriched();
}
/// <summary>An entity rebuilt from one named stream.</summary>
public interface IAggregate : IEventSourceEntity
{
    StreamName StreamName { get; }

    StreamVersion Version { get; set; }
}

/// <summary>An entity caught up from the global event log.</summary>
public interface IProjection : IEventSourceEntity
{
    RecordId LastRecordId { get; set; }

    DateTimeOffset LastUpdatedOn { get; set; }

    IReadOnlyCollection<StreamName> StreamNames => Array.Empty<StreamName>();
}

/// <summary>Base aggregate with Java-compatible initial version and enrichment state.</summary>
public abstract class AggregateBase : IAggregate
{
    public bool IsEnriched { get; private set; }

    public abstract StreamName StreamName { get; }

    public StreamVersion Version { get; set; } = StreamVersion.Undefined;

    public void MarkEnriched() => IsEnriched = true;
}

/// <summary>Base projection with Java-compatible initial checkpoint and timestamp.</summary>
public abstract class ProjectionBase : IProjection
{
    public bool IsEnriched { get; private set; }

    public RecordId LastRecordId { get; set; } = RecordId.Undefined;

    public DateTimeOffset LastUpdatedOn { get; set; } = DateTimeOffset.UnixEpoch;

    public virtual IReadOnlyCollection<StreamName> StreamNames => Array.Empty<StreamName>();

    public void MarkEnriched() => IsEnriched = true;
}
