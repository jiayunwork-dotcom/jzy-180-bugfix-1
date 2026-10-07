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
    -- 当前（最近一次标记变更之后）的标记值；对新录入/补录到最新事件之后
    -- 的批次生效。
    production_stable    BOOLEAN NOT NULL DEFAULT FALSE,
    supervisor_approval  BOOLEAN NOT NULL DEFAULT FALSE,
    -- 建流时的初始标记：没有更早的标记事件时，时间线从头按这组值折叠。
    -- 只在「流上还没有批次时打标记」时被整体改写，之后不再变化。
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

-- 管理事件：
--   kind='resume'：暂停后人工恢复；
--   kind='flags' ：流级标记变更（生产稳定/主管同意），只对排在它之后的
--                  批次生效。occurred_at 固定为提交变更那一刻流上检验时间
--                  最晚的批次的 inspected_at（无批次时不产生事件，而是改写
--                  streams 上的初始标记），因此标记事件的位置一经写定，不随
--                  之后的补录/删改移动。
CREATE TABLE IF NOT EXISTS stream_events (
    id                    BIGSERIAL PRIMARY KEY,
    stream_id             BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    kind                  TEXT NOT NULL,
    occurred_at           TIMESTAMPTZ NOT NULL,
    production_stable     BOOLEAN,
    supervisor_approval   BOOLEAN
);
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
