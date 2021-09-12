drop table if exists test.age_of_barbarians;
create table test.age_of_barbarians
(
user_id UInt32,
register_time DateTime('Asia/Shanghai'),
pvp_battle_count UInt32,
pvp_lanch_count UInt32,
pvp_win_count UInt32,
pve_battle_count UInt32,
pve_lanch_count UInt32,
pve_win_count UInt32,
avg_online_minutes Float32,
pay_price Float32,
pay_count UInt32
)
engine=ReplacingMergeTree()
PARTITION BY toYYYYMMDD(register_time)
order by user_id