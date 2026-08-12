namespace SimpleEventStore;

/// <summary>A thread-safe, process-local event storage adapter.</summary>
public sealed class InMemoryEventStorage : IEventStorage
{
    private readonly object _gate = new();
    private readonly Func<DateTimeOffset> _clock;
    private readonly List<StoredRecord> _records = [];
    private readonly Dictionary<StreamName, List<StoredRecord>> _streams = [];

    public InMemoryEventStorage()
        : this(static () => DateTimeOffset.UtcNow) { }

    public InMemoryEventStorage(Func<DateTimeOffset> clock) =>
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));

    public Task<StoredRecord> AppendAsync(
        StreamName streamName,
        StreamVersion currentVersion,
        string eventType,
        string eventContent,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(eventType);
        ArgumentNullException.ThrowIfNull(eventContent);

        lock (_gate)
        {
            StreamVersion nextVersion;
            if (currentVersion == StreamVersion.Undefined)
            {
                if (_streams.ContainsKey(streamName))
                {
                    throw new DuplicateStreamException(streamName);
                }

                nextVersion = StreamVersion.First;
            }
            else
            {
                if (!_streams.TryGetValue(streamName, out var stream))
                {
                    throw new StreamNotFoundException(streamName);
                }

                if (stream[^1].Version != currentVersion)
                {
                    throw new StaleStreamVersionException(streamName, currentVersion);
                }

                nextVersion = new StreamVersion(checked(currentVersion.Value + 1));
            }

            var record = new StoredRecord(
                new RecordId(checked(_records.Count + RecordId.First.Value)),
                streamName,
                eventType,
                eventContent,
                nextVersion,
                _clock());
            _records.Add(record);
            if (!_streams.TryGetValue(streamName, out var eventStream))
            {
                eventStream = [];
                _streams.Add(streamName, eventStream);
            }

            eventStream.Add(record);
            return Task.FromResult(record);
        }
    }

    public Task<RetrievedRecords> RetrieveAsync(
        StreamName streamName,
        IReadOnlyCollection<string> eventTypes,
        StreamVersion exclusiveStartVersion,
        StreamVersion inclusiveEndVersion,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(eventTypes);

        lock (_gate)
        {
            if (!_streams.TryGetValue(streamName, out var stream))
            {
                throw new StreamNotFoundException(streamName);
            }

            var records = stream
                .Where(record =>
                    record.Version.Value > exclusiveStartVersion.Value
                    && record.Version.Value <= inclusiveEndVersion.Value
                    && (eventTypes.Count == 0 || eventTypes.Contains(record.EventType)))
                .ToArray();
            return Task.FromResult(new RetrievedRecords(records, stream[^1]));
        }
    }

    public Task<RetrievedRecords> RetrieveAsync(
        RecordId exclusiveStartId,
        RecordId inclusiveEndId,
        IReadOnlyCollection<StreamName> streamNames,
        IReadOnlyCollection<string> eventTypes,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(streamNames);
        ArgumentNullException.ThrowIfNull(eventTypes);

        lock (_gate)
        {
            var latest = _records.Count == 0 ? StoredRecord.Empty : _records[^1];
            var records = _records
                .Where(record =>
                    record.Id.Value > exclusiveStartId.Value
                    && record.Id.Value <= inclusiveEndId.Value
                    && (streamNames.Count == 0 || streamNames.Contains(record.StreamName))
                    && (eventTypes.Count == 0 || eventTypes.Contains(record.EventType)))
                .ToArray();
            return Task.FromResult(new RetrievedRecords(records, latest));
        }
    }
}
