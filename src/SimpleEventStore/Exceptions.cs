namespace SimpleEventStore;

public class UnknownEventTypeException : InvalidOperationException
{
    public UnknownEventTypeException(Type eventType)
        : base($"Unknown event type: '{eventType.FullName}'.") { }

    public UnknownEventTypeException(string eventType)
        : base($"Unknown event type: '{eventType}'.") { }
}

public class StreamNotFoundException : InvalidOperationException
{
    public StreamNotFoundException(StreamName streamName)
        : base($"Event stream '{streamName}' was not found.") => StreamName = streamName;

    public StreamNotFoundException(StreamName streamName, Exception innerException)
        : base($"Event stream '{streamName}' was not found.", innerException) => StreamName = streamName;

    public StreamName StreamName { get; }
}

public class DuplicateStreamException : InvalidOperationException
{
    public DuplicateStreamException(StreamName streamName)
        : base($"Event stream '{streamName}' already exists.") => StreamName = streamName;

    public StreamName StreamName { get; }
}

public class StaleStreamVersionException : InvalidOperationException
{
    public StaleStreamVersionException(StreamName streamName, StreamVersion expectedVersion)
        : base($"Event stream '{streamName}' is not at expected version {expectedVersion}.")
    {
        StreamName = streamName;
        ExpectedVersion = expectedVersion;
    }

    public StreamName StreamName { get; }

    public StreamVersion ExpectedVersion { get; }
}

public class StaleAggregateStateException : InvalidOperationException
{
    public StaleAggregateStateException(Exception innerException)
        : base("The aggregate state is stale and must be reloaded before saving.", innerException) { }
}
