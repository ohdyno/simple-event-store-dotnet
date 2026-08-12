using System.Data;
using System.Reflection;
using Npgsql;
using NpgsqlTypes;

namespace SimpleEventStore;

/// <summary>PostgreSQL storage compatible with the Java <c>eventsource.events</c> schema.</summary>
public sealed class PostgreSqlEventStorage : IEventStorage
{
    private const string LatestRecordSql = """
        SELECT id, stream_name, event_type, event_content::text, version, inserted_on
        FROM eventsource.events
        ORDER BY id DESC
        LIMIT 1
        """;

    private readonly NpgsqlDataSource _dataSource;

    public PostgreSqlEventStorage(NpgsqlDataSource dataSource) =>
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));

    /// <summary>Explicitly creates or upgrades the idempotent event-store schema.</summary>
    public async Task InitializeSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var stream = Assembly.GetExecutingAssembly()
            .GetManifestResourceStream("SimpleEventStore.Schema.schema.sql")
            ?? throw new InvalidOperationException("The embedded PostgreSQL schema was not found.");
        using var reader = new StreamReader(stream);
        var sql = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);

        await using var command = _dataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<StoredRecord> AppendAsync(
        StreamName streamName,
        StreamVersion currentVersion,
        string eventType,
        string eventContent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventType);
        ArgumentNullException.ThrowIfNull(eventContent);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .ConfigureAwait(false);

        await using (var lockCommand = new NpgsqlCommand(
                         "SELECT pg_advisory_xact_lock(hashtextextended(@stream_name, 0));",
                         connection,
                         transaction))
        {
            lockCommand.Parameters.AddWithValue("stream_name", streamName.Value);
            await lockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var actualVersion = await GetLatestVersionAsync(connection, transaction, streamName, cancellationToken)
            .ConfigureAwait(false);
        StreamVersion nextVersion;
        if (currentVersion == StreamVersion.Undefined)
        {
            if (actualVersion is not null)
            {
                throw new DuplicateStreamException(streamName);
            }

            nextVersion = StreamVersion.First;
        }
        else
        {
            if (actualVersion is null)
            {
                throw new StreamNotFoundException(streamName);
            }

            if (actualVersion.Value != currentVersion)
            {
                throw new StaleStreamVersionException(streamName, currentVersion);
            }

            nextVersion = new StreamVersion(checked(currentVersion.Value + 1));
        }

        const string insertSql = """
            INSERT INTO eventsource.events (stream_name, event_type, event_content, version)
            VALUES (@stream_name, @event_type, @event_content, @version)
            RETURNING id, stream_name, event_type, event_content::text, version, inserted_on
            """;

        try
        {
            await using var insert = new NpgsqlCommand(insertSql, connection, transaction);
            insert.Parameters.AddWithValue("stream_name", streamName.Value);
            insert.Parameters.AddWithValue("event_type", eventType);
            insert.Parameters.AddWithValue("event_content", NpgsqlDbType.Jsonb, eventContent);
            insert.Parameters.AddWithValue("version", nextVersion.Value);
            await using var result = await insert.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await result.ReadAsync(cancellationToken).ConfigureAwait(false);
            var record = ReadRecord(result);
            await result.DisposeAsync().ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return record;
        }
        catch (PostgresException exception) when (exception.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw currentVersion == StreamVersion.Undefined
                ? new DuplicateStreamException(streamName)
                : new StaleStreamVersionException(streamName, currentVersion);
        }
    }

    public async Task<RetrievedRecords> RetrieveAsync(
        StreamName streamName,
        IReadOnlyCollection<string> eventTypes,
        StreamVersion exclusiveStartVersion,
        StreamVersion inclusiveEndVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventTypes);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var latest = await GetLatestStreamRecordAsync(connection, streamName, cancellationToken).ConfigureAwait(false)
                     ?? throw new StreamNotFoundException(streamName);

        const string sql = """
            SELECT id, stream_name, event_type, event_content::text, version, inserted_on
            FROM eventsource.events
            WHERE stream_name = @stream_name
              AND version > @exclusive_start
              AND version <= @inclusive_end
              AND (cardinality(@event_types) = 0 OR event_type = ANY(@event_types))
            ORDER BY version
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("stream_name", streamName.Value);
        command.Parameters.AddWithValue("exclusive_start", exclusiveStartVersion.Value);
        command.Parameters.AddWithValue("inclusive_end", inclusiveEndVersion.Value);
        command.Parameters.AddWithValue("event_types", NpgsqlDbType.Array | NpgsqlDbType.Text, eventTypes.ToArray());
        var records = await ReadRecordsAsync(command, cancellationToken).ConfigureAwait(false);
        return new RetrievedRecords(records, latest);
    }

    public async Task<RetrievedRecords> RetrieveAsync(
        RecordId exclusiveStartId,
        RecordId inclusiveEndId,
        IReadOnlyCollection<StreamName> streamNames,
        IReadOnlyCollection<string> eventTypes,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(streamNames);
        ArgumentNullException.ThrowIfNull(eventTypes);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var latest = await GetLatestRecordAsync(connection, cancellationToken).ConfigureAwait(false);

        const string sql = """
            SELECT id, stream_name, event_type, event_content::text, version, inserted_on
            FROM eventsource.events
            WHERE id > @exclusive_start
              AND id <= @inclusive_end
              AND (cardinality(@stream_names) = 0 OR stream_name = ANY(@stream_names))
              AND (cardinality(@event_types) = 0 OR event_type = ANY(@event_types))
            ORDER BY id
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("exclusive_start", exclusiveStartId.Value);
        command.Parameters.AddWithValue("inclusive_end", inclusiveEndId.Value);
        command.Parameters.AddWithValue(
            "stream_names",
            NpgsqlDbType.Array | NpgsqlDbType.Text,
            streamNames.Select(static name => name.Value).ToArray());
        command.Parameters.AddWithValue("event_types", NpgsqlDbType.Array | NpgsqlDbType.Text, eventTypes.ToArray());
        var records = await ReadRecordsAsync(command, cancellationToken).ConfigureAwait(false);
        return new RetrievedRecords(records, latest ?? StoredRecord.Empty);
    }

    private static async Task<StreamVersion?> GetLatestVersionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        StreamName streamName,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT version
            FROM eventsource.events
            WHERE stream_name = @stream_name
            ORDER BY version DESC
            LIMIT 1
            """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("stream_name", streamName.Value);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null ? null : new StreamVersion((long)result);
    }

    private static async Task<StoredRecord?> GetLatestStreamRecordAsync(
        NpgsqlConnection connection,
        StreamName streamName,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT id, stream_name, event_type, event_content::text, version, inserted_on
            FROM eventsource.events
            WHERE stream_name = @stream_name
            ORDER BY version DESC
            LIMIT 1
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("stream_name", streamName.Value);
        return await ReadSingleRecordAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<StoredRecord?> GetLatestRecordAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(LatestRecordSql, connection);
        return await ReadSingleRecordAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<StoredRecord?> ReadSingleRecordAsync(
        NpgsqlCommand command,
        CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? ReadRecord(reader) : null;
    }

    private static async Task<IReadOnlyList<StoredRecord>> ReadRecordsAsync(
        NpgsqlCommand command,
        CancellationToken cancellationToken)
    {
        var records = new List<StoredRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            records.Add(ReadRecord(reader));
        }

        return records;
    }

    private static StoredRecord ReadRecord(NpgsqlDataReader reader)
    {
        var insertedOn = reader.GetFieldValue<DateTime>(5);
        return new StoredRecord(
            new RecordId(reader.GetInt64(0)),
            new StreamName(reader.GetString(1)),
            reader.GetString(2),
            reader.GetString(3),
            new StreamVersion(reader.GetInt64(4)),
            new DateTimeOffset(DateTime.SpecifyKind(insertedOn, DateTimeKind.Utc)));
    }
}
