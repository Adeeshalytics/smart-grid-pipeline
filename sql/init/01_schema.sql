
SET TIME ZONE 'UTC';

CREATE TABLE sim_clock (
    id          INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    anchor_wall TIMESTAMPTZ NOT NULL,
    sim_epoch   TIMESTAMPTZ NOT NULL,
    day_seconds INT NOT NULL
);

CREATE TABLE rt_zone_load (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    grid_zone        TEXT NOT NULL,
    consumption_kwh  DOUBLE PRECISION NOT NULL,
    solar_kwh        DOUBLE PRECISION NOT NULL,
    net_load_kwh     DOUBLE PRECISION NOT NULL,   -- what the grid must supply
    renewable_share  DOUBLE PRECISION NOT NULL,   -- solar / consumption
    active_meters    INT NOT NULL,
    readings         INT NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, grid_zone)
);
CREATE INDEX rt_zone_load_zone_idx ON rt_zone_load (grid_zone, window_start DESC);

CREATE TABLE rt_household_today (
    sim_date         DATE NOT NULL,
    household_id     TEXT NOT NULL,
    grid_zone        TEXT NOT NULL,
    consumption_kwh  DOUBLE PRECISION NOT NULL,
    solar_kwh        DOUBLE PRECISION NOT NULL,
    readings         INT NOT NULL,
    tariff_rate      DOUBLE PRECISION NOT NULL,
    billing_tier     TEXT NOT NULL,
    subsidy_flag     BOOLEAN NOT NULL,
    tariff_source    TEXT NOT NULL,               -- latest_known | default
    provisional_bill DOUBLE PRECISION NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, household_id)
);

CREATE TABLE tariffs (
    dt              DATE NOT NULL,
    household_id    TEXT NOT NULL,
    tariff_rate     DOUBLE PRECISION NOT NULL,
    billing_tier    TEXT NOT NULL,
    subsidy_flag    BOOLEAN NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, household_id)
);

CREATE TABLE tariff_rejects (
    dt              DATE NOT NULL,
    line_no         INT NOT NULL,
    reason          TEXT NOT NULL,
    raw             JSONB NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (dt, line_no)
);

CREATE TABLE daily_household_bill (
    sim_date               DATE NOT NULL,
    household_id           TEXT NOT NULL,
    grid_zone              TEXT NOT NULL,
    readings               INT NOT NULL,
    consumption_kwh        DOUBLE PRECISION NOT NULL,
    solar_kwh              DOUBLE PRECISION NOT NULL,
    net_kwh                DOUBLE PRECISION NOT NULL,
    import_kwh             DOUBLE PRECISION NOT NULL,
    export_kwh             DOUBLE PRECISION NOT NULL,
    solar_contribution_pct DOUBLE PRECISION NOT NULL,
    tariff_rate            DOUBLE PRECISION NOT NULL,
    billing_tier           TEXT NOT NULL,
    subsidy_flag           BOOLEAN NOT NULL,
    tariff_source          TEXT NOT NULL,
    energy_charge          DOUBLE PRECISION NOT NULL,
    subsidy_discount       DOUBLE PRECISION NOT NULL,
    export_credit          DOUBLE PRECISION NOT NULL,
    fixed_charge           DOUBLE PRECISION NOT NULL,
    total_bill             DOUBLE PRECISION NOT NULL,
    pipeline_run_id        TEXT NOT NULL,
    computed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, household_id)
);

CREATE TABLE stg_daily_household_bill (LIKE daily_household_bill INCLUDING DEFAULTS);

CREATE TABLE daily_zone_summary (
    sim_date         DATE NOT NULL,
    grid_zone        TEXT NOT NULL,
    households       INT NOT NULL,
    consumption_kwh  DOUBLE PRECISION NOT NULL,
    solar_kwh        DOUBLE PRECISION NOT NULL,
    renewable_share  DOUBLE PRECISION NOT NULL,
    import_kwh       DOUBLE PRECISION NOT NULL,
    export_kwh       DOUBLE PRECISION NOT NULL,
    total_billed     DOUBLE PRECISION NOT NULL,
    pipeline_run_id  TEXT NOT NULL,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, grid_zone)
);

CREATE TABLE reconciliation (
    sim_date        DATE NOT NULL,
    grid_zone       TEXT NOT NULL,
    batch_kwh       DOUBLE PRECISION NOT NULL,
    speed_kwh       DOUBLE PRECISION NOT NULL,
    diff_kwh        DOUBLE PRECISION NOT NULL,
    diff_pct        DOUBLE PRECISION NOT NULL,
    batch_readings  INT NOT NULL,
    speed_readings  INT NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, grid_zone)
);

CREATE TABLE daily_reports (
    sim_date        DATE PRIMARY KEY,
    html_key        TEXT NOT NULL,
    csv_key         TEXT NOT NULL,
    summary         JSONB NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE grid_alerts (
    id           BIGSERIAL PRIMARY KEY,
    alert_type   TEXT NOT NULL,
    grid_zone    TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    severity     TEXT NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    threshold    DOUBLE PRECISION NOT NULL,
    message      TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (alert_type, grid_zone, window_start)
);

CREATE TABLE ops_alerts (
    id          BIGSERIAL PRIMARY KEY,
    alertname   TEXT NOT NULL,
    status      TEXT NOT NULL,                     -- firing | resolved
    severity    TEXT,
    summary     TEXT,
    labels      JSONB NOT NULL,
    starts_at   TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipeline_runs (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL,
    task        TEXT NOT NULL,
    sim_date    DATE,
    status      TEXT NOT NULL,                     -- success | failed
    duration_ms INT,
    details     JSONB NOT NULL DEFAULT '{}',
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX pipeline_runs_date_idx ON pipeline_runs (sim_date, task);

CREATE VIEW v_rt_zone_load AS
SELECT z.*,
       c.anchor_wall + (z.window_start - c.sim_epoch) * c.day_seconds / 86400.0 AS wall_time
FROM rt_zone_load z
CROSS JOIN sim_clock c;

CREATE VIEW v_grid_now AS
SELECT DISTINCT ON (grid_zone) *
FROM rt_zone_load
WHERE window_end <= (SELECT max(window_start) FROM rt_zone_load)
ORDER BY grid_zone, window_start DESC;

CREATE VIEW v_latest_tariff AS
SELECT DISTINCT ON (household_id) household_id, tariff_rate, billing_tier, subsidy_flag, dt
FROM tariffs
ORDER BY household_id, dt DESC;
