CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS stg.user_sessions;
CREATE TABLE stg.user_sessions (
    session_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    pages_count INT NOT NULL,
    actions_count INT NOT NULL,
    device_type VARCHAR(50) NOT NULL
);

DROP TABLE IF EXISTS stg.support_tickets;
CREATE TABLE stg.support_tickets (
    ticket_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    message_count INT NOT NULL
);

DROP TABLE IF EXISTS dds.user_sessions;
CREATE TABLE dds.user_sessions (
    session_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    pages_count INT NOT NULL,
    actions_count INT NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    session_duration_minutes INT NOT NULL
);

DROP TABLE IF EXISTS dds.support_tickets;
CREATE TABLE dds.support_tickets (
    ticket_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    message_count INT NOT NULL,
    resolution_time_hours NUMERIC(10,2) NOT NULL
);