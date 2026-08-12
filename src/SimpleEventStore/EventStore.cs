using System.Reactive.Subjects;

namespace SimpleEventStore;

/// <summary>Coordinates serialization, persistence, rehydration, and record publication.</summary>
public sealed class EventStore : IDisposable
{
    private readonly IEventStorage _storage;
    private readonly IEventSerializer _serializer;
    private readonly IEventTypeConverter _converter;
    private readonly IEventApplier _applier;
    private readonly ISubject<StoredRecord> _records;

    public EventStore(
        IEventStorage storage,
        IEventSerializer serializer,
        IEventTypeConverter converter,
        IEventApplier applier)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _converter = converter ?? throw new ArgumentNullException(nameof(converter));
        _applier = applier ?? throw new ArgumentNullException(nameof(applier));
        _records = Subject.Synchronize(new ReplaySubject<StoredRecord>(1));
    }

    /// <summary>Publishes each successfully saved record and replays the latest one to late subscribers.</summary>
    public IObservable<StoredRecord> Records => _records;

    public async Task<TAggregate> SaveAsync<TAggregate>(
        IEvent @event,
        TAggregate aggregate,
        CancellationToken cancellationToken = default)
        where TAggregate : IAggregate
    {
        ArgumentNullException.ThrowIfNull(@event);
        ArgumentNullException.ThrowIfNull(aggregate);
        cancellationToken.ThrowIfCancellationRequested();

        var serialized = _serializer.Serialize(@event);
        StoredRecord record;
        try
        {
            record = await _storage.AppendAsync(
                    aggregate.StreamName,
                    aggregate.Version,
                    serialized.EventType,
                    serialized.EventJson,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is DuplicateStreamException or StaleStreamVersionException)
        {
            throw new StaleAggregateStateException(exception);
        }

        _applier.Apply(@event, ToDetails(record), aggregate);
        aggregate.Version = record.Version;
        aggregate.MarkEnriched();
        _records.OnNext(record);
        return aggregate;
    }

    public Task<TEntity> EnrichAsync<TEntity>(
        TEntity entity,
        CancellationToken cancellationToken = default)
        where TEntity : IEventSourceEntity
    {
        ArgumentNullException.ThrowIfNull(entity);
        return entity switch
        {
            IAggregate aggregate => EnrichAggregateAsync(entity, aggregate, cancellationToken),
            IProjection projection => EnrichProjectionAsync(entity, projection, cancellationToken),
            _ => throw new ArgumentException("The entity must be an aggregate or projection.", nameof(entity)),
        };
    }

    public void Dispose()
    {
        _records.OnCompleted();
        if (_records is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    private async Task<TEntity> EnrichAggregateAsync<TEntity>(
        TEntity entity,
        IAggregate aggregate,
        CancellationToken cancellationToken)
        where TEntity : IEventSourceEntity
    {
        RetrievedRecords retrieved;
        try
        {
            retrieved = await _storage.RetrieveAsync(
                    aggregate.StreamName,
                    GetEventNames(entity),
                    aggregate.Version,
                    StreamVersion.Maximum,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (StreamNotFoundException exception)
        {
            throw new StreamNotFoundException(aggregate.StreamName, exception);
        }

        Apply(retrieved.Records, entity);
        aggregate.Version = retrieved.LatestRecord.Version;
        return entity;
    }

    private async Task<TEntity> EnrichProjectionAsync<TEntity>(
        TEntity entity,
        IProjection projection,
        CancellationToken cancellationToken)
        where TEntity : IEventSourceEntity
    {
        var retrieved = await _storage.RetrieveAsync(
                projection.LastRecordId,
                RecordId.Maximum,
                projection.StreamNames,
                GetEventNames(entity),
                cancellationToken)
            .ConfigureAwait(false);
        Apply(retrieved.Records, entity);
        projection.LastRecordId = retrieved.LatestRecord.Id;
        projection.LastUpdatedOn = retrieved.LatestRecord.InsertedOn;
        return entity;
    }

    private void Apply<TEntity>(IReadOnlyList<StoredRecord> records, TEntity entity)
        where TEntity : IEventSourceEntity
    {
        foreach (var record in records)
        {
            var @event = _serializer.Deserialize(record.EventType, record.EventContent);
            _applier.Apply(@event, ToDetails(record), entity);
        }

        if (records.Count > 0)
        {
            entity.MarkEnriched();
        }
    }

    private string[] GetEventNames(object entity)
    {
        var handledTypes = _applier.GetHandledEventTypes(entity);
        if (handledTypes.Contains(typeof(IEvent)))
        {
            return Array.Empty<string>();
        }

        return handledTypes
            .SelectMany(_converter.GetEventNamesAssignableTo)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
    }

    private static RecordDetails ToDetails(StoredRecord record) =>
        new(record.StreamName, record.Version, record.Id, record.InsertedOn);
}
