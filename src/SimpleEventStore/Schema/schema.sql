CREATE SCHEMA IF NOT EXISTS eventsource;

CREATE TABLE IF NOT EXISTS eventsource.events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_content JSONB NOT NULL,
    version BIGINT NOT NULL,
    inserted_on TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (stream_name, version)
);

CREATE INDEX IF NOT EXISTS index_event_type ON eventsource.events(event_type);
CREATE INDEX IF NOT EXISTS index_stream_name ON eventsource.events(stream_name);
CREATE INDEX IF NOT EXISTS index_inserted_on ON eventsource.events(inserted_on);

CREATE OR REPLACE VIEW eventsource.latest_stream_event AS
SELECT DISTINCT ON (stream_name) *
FROM eventsource.events
ORDER BY stream_name, version DESC;

CREATE OR REPLACE VIEW eventsource.latest_event AS
SELECT *
FROM eventsource.events
ORDER BY id DESC
LIMIT 1;
