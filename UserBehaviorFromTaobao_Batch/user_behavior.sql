drop table if exists user_behavior;
create table user_behavior
(
    user_id       String,
    item_id       String,
    category_id   String,
    behavior_type String,
    timestamp     DateTime('Asia/Shanghai')
)
    engine = ReplacingMergeTree()
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (item_id, behavior_type, timestamp,user_id)
        SETTINGS index_granularity = 8192;