DROP TABLE IF EXISTS mart.user_activity;
CREATE TABLE mart.user_activity (
    user_id BIGINT PRIMARY KEY,
    sessions_count INT NOT NULL,
    avg_session_duration_minutes NUMERIC(10,2) NOT NULL,
    total_pages_count INT NOT NULL,
    total_actions_count INT NOT NULL
);

DROP TABLE IF EXISTS mart.support_performance;
CREATE TABLE mart.support_performance (
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(100) NOT NULL,
    tickets_count INT NOT NULL,
    avg_resolution_time_hours NUMERIC(10,2) NOT NULL,
    open_tickets_count INT NOT NULL
);