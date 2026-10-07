-- Sampling plan档
CREATE TABLE IF NOT EXISTS plans (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    definition  JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 检验流：绑定正常/加严/放宽三个方案档
CREATE TABLE IF NOT EXISTS streams (
    id                   BIGSERIAL PRIMARY KEY,
    name                 TEXT NOT NULL UNIQUE,
    normal_plan_id       BIGINT NOT NULL REFERENCES plans(id),
    tightened_plan_id    BIGINT NOT NULL REFERENCES plans(id),
    reduced_plan_id      BIGINT NOT NULL REFERENCES plans(id),
    production_stable    BOOLEAN NOT NULL DEFAULT FALSE,
    supervisor_approval  BOOLEAN NOT NULL DEFAULT FALSE,
    -- 时间线开始（第一批之前）的初始标记；PATCH 标记只在标记事件之后生效，
    -- 因此从头重放需要知道流的初始标记值，它可能与当前标记不同。
    initial_production_stable   BOOLEAN NOT NULL DEFAULT FALSE,
    initial_supervisor_approval BOOLEAN NOT NULL DEFAULT FALSE,
    current_state        JSONB NOT NULL,
    version              BIGINT NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 逐批检验结果（含折叠后的严格度、判定、得分、所用方案快照）
CREATE TABLE IF NOT EXISTS batches (
    id            BIGSERIAL PRIMARY KEY,
    stream_id     BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    batch_no      TEXT NOT NULL,
    inspected_at  TIMESTAMPTZ NOT NULL,
    d1            INTEGER NOT NULL,
    d2            INTEGER,
    result        JSONB NOT NULL,
    UNIQUE (stream_id, batch_no)
);
CREATE INDEX IF NOT EXISTS batches_stream_time
    ON batches (stream_id, inspected_at, id);

-- 管理事件（暂停后人工恢复 kind='resume'；标记变更 kind='flags'）。
-- flags 事件把两个标记整体置为 production_stable/supervisor_approval 的值，
-- 在 occurred_at 处切入时间线，只对其后的批次生效。
CREATE TABLE IF NOT EXISTS stream_events (
    id                  BIGSERIAL PRIMARY KEY,
    stream_id           BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    kind                TEXT NOT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL,
    production_stable   BOOLEAN,
    supervisor_approval BOOLEAN
);

-- 既有库的幂等升级（CREATE TABLE IF NOT EXISTS 不会补列）。
ALTER TABLE streams ADD COLUMN IF NOT EXISTS initial_production_stable BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE streams ADD COLUMN IF NOT EXISTS initial_supervisor_approval BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE stream_events ADD COLUMN IF NOT EXISTS production_stable BOOLEAN;
ALTER TABLE stream_events ADD COLUMN IF NOT EXISTS supervisor_approval BOOLEAN;
CREATE INDEX IF NOT EXISTS events_stream_time
    ON stream_events (stream_id, occurred_at, id);

-- 重放检查点：item_seq 是时间线上已折叠的条目数（批次与事件合计），
-- ordinal 是其中的批次数（对应状态机的 Ordinal）。
CREATE TABLE IF NOT EXISTS stream_checkpoints (
    stream_id  BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    item_seq   INTEGER NOT NULL,
    ordinal    INTEGER NOT NULL,
    state      JSONB NOT NULL,
    PRIMARY KEY (stream_id, item_seq)
);
