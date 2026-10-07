-- 标记改为时间线上的事件：PATCH 的标记只对分界点之后的批次生效。
--
-- streams 上新增两列保存「流的初始标记」（时间线开始、第一批之前的值），
-- 从头重放以它为种子；streams.production_stable / supervisor_approval
-- 继续表示当前标记值（= 最近一次 flags 事件之后的值）。
-- stream_events 新增 kind='flags' 事件，两列携带变更后的整体标记值。
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS initial_production_stable   BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS initial_supervisor_approval BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE stream_events
    ADD COLUMN IF NOT EXISTS production_stable   BOOLEAN,
    ADD COLUMN IF NOT EXISTS supervisor_approval BOOLEAN;
