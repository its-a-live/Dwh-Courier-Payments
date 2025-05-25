CREATE TABLE IF NOT EXISTS stg.bonussystem_events
(
    id integer not null,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT outbox_pkey PRIMARY KEY (id)
);