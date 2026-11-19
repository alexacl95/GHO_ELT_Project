-- Single source of truth for the DDL. Safe to run repeatedly (IF NOT EXISTS).
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;

-- ============================ STAGING ============================
-- Landing zone: mirrors the latest snapshot, truncated and reloaded each run.
CREATE TABLE IF NOT EXISTS staging.gho_indicators (
    indicator_code  TEXT PRIMARY KEY,
    indicator_name  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.gho_countries (
    country_code     TEXT PRIMARY KEY,
    country_name     TEXT,
    who_region_code  TEXT,
    who_region_name  TEXT
);

-- value stays nullable here: staging keeps every observation, even without a number.
-- The PK doubles as a quality gate: if Spark ever emits a duplicate, COPY fails loudly.
CREATE TABLE IF NOT EXISTS staging.gho_values (
    indicator_code  TEXT    NOT NULL,
    country_code    TEXT    NOT NULL,
    year            INTEGER NOT NULL,
    dim1            TEXT    NOT NULL DEFAULT '',
    dim2            TEXT    NOT NULL DEFAULT '',
    dim3            TEXT    NOT NULL DEFAULT '',
    value           DOUBLE PRECISION,
    value_low       DOUBLE PRECISION,
    value_high      DOUBLE PRECISION,
    value_display   TEXT,
    source_id       BIGINT,
    extracted_at    TIMESTAMPTZ,
    PRIMARY KEY (indicator_code, country_code, year, dim1, dim2, dim3)
);

-- ============================= CORE ==============================
-- Serving layer: only rows with a numeric value, plus derived columns.
CREATE TABLE IF NOT EXISTS core.gho_indicators (
    indicator_code  TEXT PRIMARY KEY,
    indicator_name  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.gho_countries (
    country_code     TEXT PRIMARY KEY,
    country_name     TEXT,
    who_region_code  TEXT,
    who_region_name  TEXT
);

CREATE TABLE IF NOT EXISTS core.gho_values (
    indicator_code  TEXT             NOT NULL,
    country_code    TEXT             NOT NULL,
    year            INTEGER          NOT NULL,
    dim1            TEXT             NOT NULL DEFAULT '',
    dim2            TEXT             NOT NULL DEFAULT '',
    dim3            TEXT             NOT NULL DEFAULT '',
    sex             TEXT,                          -- derived from dim1 (NULL if no sex breakdown)
    value           DOUBLE PRECISION NOT NULL,     -- core never holds null measurements
    value_low       DOUBLE PRECISION,
    value_high      DOUBLE PRECISION,
    value_display   TEXT,
    source_id       BIGINT,
    extracted_at    TIMESTAMPTZ,                   -- snapshot in which this value last changed
    PRIMARY KEY (indicator_code, country_code, year, dim1, dim2, dim3)
);

-- The PK serves "one country + one indicator over time".
-- This index serves "one indicator, one year, all countries" (rankings, comparisons).
CREATE INDEX IF NOT EXISTS idx_core_values_ind_year
    ON core.gho_values (indicator_code, year);