-- ============================================
-- 用户分析数据库表结构
-- ============================================

-- --------------------------------------------
-- 1. 用户表 (b_user)
-- --------------------------------------------
CREATE TABLE `b_user_{appId}` (
                            `device_id` BIGINT NULL,
                            `zg_id` BIGINT NULL,
                            `user_id` BIGINT NULL,
                            `begin_date` BIGINT NULL,
                            `platform` SMALLINT NULL
) ENGINE=OLAP
UNIQUE KEY(`device_id`, `zg_id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`zg_id`) BUCKETS 8
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

-- --------------------------------------------
-- 2. 用户属性表 (b_user_property)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS b_user_property_{appId} (
    `zg_id`              BIGINT(20)    NULL,
    `property_id`        INT(11)       NULL,
    `user_id`            BIGINT(20)    NULL,
    `property_name`      VARCHAR(500)  NULL,
    `property_data_type` VARCHAR(500)  NULL,
    `property_value`     VARCHAR(500)  NULL,
    `platform`           SMALLINT(6)   NULL,
    `last_update_date`   BIGINT(20)    NULL
    ) ENGINE = OLAP
    UNIQUE KEY(`zg_id`, `property_id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`zg_id`) BUCKETS 8
    PROPERTIES (
                   "replication_allocation" = "tag.location.default: 3",
                   "storage_format" = "V2",
                   "enable_unique_key_merge_on_write" = "true",
                   "light_schema_change" = "true",
                   "disable_auto_compaction" = "false",
                   "enable_single_replica_compaction" = "false"
               );

-- --------------------------------------------
-- 3. 设备表 (b_device)
-- --------------------------------------------
CREATE TABLE `b_device_{appId}` (
                              `device_id` BIGINT NULL,
                              `device_md5` VARCHAR(500) NULL,
                              `platform` SMALLINT NULL,
                              `device_type` VARCHAR(500) NULL,
                              `l` INT NULL,
                              `h` INT NULL,
                              `device_brand` VARCHAR(500) NULL,
                              `device_model` VARCHAR(500) NULL,
                              `resolution` VARCHAR(500) NULL,
                              `phone` VARCHAR(500) NULL,
                              `imei` VARCHAR(500) NULL,
                              `mac` VARCHAR(500) NULL,
                              `is_prison_break` SMALLINT NULL,
                              `is_crack` SMALLINT NULL,
                              `language` VARCHAR(500) NULL,
                              `timezone` VARCHAR(500) NULL,
                              `attr1` VARCHAR(500) NULL,
                              `attr2` VARCHAR(500) NULL,
                              `attr3` VARCHAR(500) NULL,
                              `attr4` VARCHAR(500) NULL,
                              `attr5` VARCHAR(500) NULL,
                              `last_update_date` BIGINT NULL
) ENGINE=OLAP
UNIQUE KEY(`device_id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`device_id`) BUCKETS 8
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

-- --------------------------------------------
-- 4. 用户事件属性表 (b_user_event_attr)
-- --------------------------------------------
-- --------------------------------------------
-- 用户事件属性表 (b_user_event_attr) - 明细模型
-- --------------------------------------------
CREATE TABLE `b_user_event_attr_{appId}` (
                                       `zg_id` BIGINT NULL,
                                       `session_id` BIGINT NULL,
                                       `uuid` VARCHAR(255) NULL,
                                       `event_id` BIGINT NULL,
                                       `begin_day_id` INT NULL,
                                       `begin_date` BIGINT NULL,
                                       `begin_time_id` INT NULL,
                                       `device_id` BIGINT NULL,
                                       `user_id` BIGINT NULL,
                                       `event_name` VARCHAR(2000) NULL,
                                       `platform` SMALLINT NULL,
                                       `network` SMALLINT NULL,
                                       `mccmnc` INT NULL,
                                       `useragent` VARCHAR(500) NULL,
                                       `website` VARCHAR(255) NULL,
                                       `current_url` VARCHAR(2000) NULL,
                                       `referrer_url` VARCHAR(2000) NULL,
                                       `channel` VARCHAR(2000) NULL,
                                       `app_version` VARCHAR(2000) NULL,
                                       `ip` BIGINT NULL,
                                       `ip_str` VARCHAR(255) NULL,
                                       `country` VARCHAR(255) NULL,
                                       `area` VARCHAR(255) NULL,
                                       `city` VARCHAR(255) NULL,
                                       `os` VARCHAR(255) NULL,
                                       `ov` INT NULL,
                                       `bs` VARCHAR(255) NULL,
                                       `bv` INT NULL,
                                       `utm_source` VARCHAR(255) NULL,
                                       `utm_medium` VARCHAR(255) NULL,
                                       `utm_campaign` VARCHAR(255) NULL,
                                       `utm_content` VARCHAR(255) NULL,
                                       `utm_term` VARCHAR(255) NULL,
                                       `duration` BIGINT NULL,
                                       `utc_date` BIGINT NULL,
                                       `attr1` VARCHAR(2000) NULL,
                                       `attr2` VARCHAR(2000) NULL,
                                       `attr3` VARCHAR(2000) NULL,
                                       `attr4` VARCHAR(2000) NULL,
                                       `attr5` VARCHAR(2000) NULL,
                                       `cus1` VARCHAR(2000) NULL,
                                       `cus2` VARCHAR(2000) NULL,
                                       `cus3` VARCHAR(2000) NULL,
                                       `cus4` VARCHAR(2000) NULL,
                                       `cus5` VARCHAR(2000) NULL,
                                       `cus6` VARCHAR(2000) NULL,
                                       `cus7` VARCHAR(2000) NULL,
                                       `cus8` VARCHAR(2000) NULL,
                                       `cus9` VARCHAR(2000) NULL,
                                       `cus10` VARCHAR(2000) NULL,
                                       `cus11` VARCHAR(2000) NULL,
                                       `cus12` VARCHAR(2000) NULL,
                                       `cus13` VARCHAR(2000) NULL,
                                       `cus14` VARCHAR(2000) NULL,
                                       `cus15` VARCHAR(2000) NULL,
                                       `cus16` VARCHAR(2000) NULL,
                                       `cus17` VARCHAR(2000) NULL,
                                       `cus18` VARCHAR(2000) NULL,
                                       `cus19` VARCHAR(2000) NULL,
                                       `cus20` VARCHAR(2000) NULL,
                                       `cus21` VARCHAR(2000) NULL,
                                       `cus22` VARCHAR(2000) NULL,
                                       `cus23` VARCHAR(2000) NULL,
                                       `cus24` VARCHAR(2000) NULL,
                                       `cus25` VARCHAR(2000) NULL,
                                       `cus26` VARCHAR(2000) NULL,
                                       `cus27` VARCHAR(2000) NULL,
                                       `cus28` VARCHAR(2000) NULL,
                                       `cus29` VARCHAR(2000) NULL,
                                       `cus30` VARCHAR(2000) NULL,
                                       `cus31` VARCHAR(2000) NULL,
                                       `cus32` VARCHAR(2000) NULL,
                                       `cus33` VARCHAR(2000) NULL,
                                       `cus34` VARCHAR(2000) NULL,
                                       `cus35` VARCHAR(2000) NULL,
                                       `cus36` VARCHAR(2000) NULL,
                                       `cus37` VARCHAR(2000) NULL,
                                       `cus38` VARCHAR(2000) NULL,
                                       `cus39` VARCHAR(2000) NULL,
                                       `cus40` VARCHAR(2000) NULL,
                                       `cus41` VARCHAR(2000) NULL,
                                       `cus42` VARCHAR(2000) NULL,
                                       `cus43` VARCHAR(2000) NULL,
                                       `cus44` VARCHAR(2000) NULL,
                                       `cus45` VARCHAR(2000) NULL,
                                       `cus46` VARCHAR(2000) NULL,
                                       `cus47` VARCHAR(2000) NULL,
                                       `cus48` VARCHAR(2000) NULL,
                                       `cus49` VARCHAR(2000) NULL,
                                       `cus50` VARCHAR(2000) NULL,
                                       `cus51` VARCHAR(2000) NULL,
                                       `cus52` VARCHAR(2000) NULL,
                                       `cus53` VARCHAR(2000) NULL,
                                       `cus54` VARCHAR(2000) NULL,
                                       `cus55` VARCHAR(2000) NULL,
                                       `cus56` VARCHAR(2000) NULL,
                                       `cus57` VARCHAR(2000) NULL,
                                       `cus58` VARCHAR(2000) NULL,
                                       `cus59` VARCHAR(2000) NULL,
                                       `cus60` VARCHAR(2000) NULL,
                                       `cus61` VARCHAR(2000) NULL,
                                       `cus62` VARCHAR(2000) NULL,
                                       `cus63` VARCHAR(2000) NULL,
                                       `cus64` VARCHAR(2000) NULL,
                                       `cus65` VARCHAR(2000) NULL,
                                       `cus66` VARCHAR(2000) NULL,
                                       `cus67` VARCHAR(2000) NULL,
                                       `cus68` VARCHAR(2000) NULL,
                                       `cus69` VARCHAR(2000) NULL,
                                       `cus70` VARCHAR(2000) NULL,
                                       `cus71` VARCHAR(2000) NULL,
                                       `cus72` VARCHAR(2000) NULL,
                                       `cus73` VARCHAR(2000) NULL,
                                       `cus74` VARCHAR(2000) NULL,
                                       `cus75` VARCHAR(2000) NULL,
                                       `cus76` VARCHAR(2000) NULL,
                                       `cus77` VARCHAR(2000) NULL,
                                       `cus78` VARCHAR(2000) NULL,
                                       `cus79` VARCHAR(2000) NULL,
                                       `cus80` VARCHAR(2000) NULL,
                                       `cus81` VARCHAR(2000) NULL,
                                       `cus82` VARCHAR(2000) NULL,
                                       `cus83` VARCHAR(2000) NULL,
                                       `cus84` VARCHAR(2000) NULL,
                                       `cus85` VARCHAR(2000) NULL,
                                       `cus86` VARCHAR(2000) NULL,
                                       `cus87` VARCHAR(2000) NULL,
                                       `cus88` VARCHAR(2000) NULL,
                                       `cus89` VARCHAR(2000) NULL,
                                       `cus90` VARCHAR(2000) NULL,
                                       `cus91` VARCHAR(2000) NULL,
                                       `cus92` VARCHAR(2000) NULL,
                                       `cus93` VARCHAR(2000) NULL,
                                       `cus94` VARCHAR(2000) NULL,
                                       `cus95` VARCHAR(2000) NULL,
                                       `cus96` VARCHAR(2000) NULL,
                                       `cus97` VARCHAR(2000) NULL,
                                       `cus98` VARCHAR(2000) NULL,
                                       `cus99` VARCHAR(2000) NULL,
                                       `cus100` VARCHAR(2000) NULL,
                                       `type1` VARCHAR(2000) NULL,
                                       `type2` VARCHAR(2000) NULL,
                                       `type3` VARCHAR(2000) NULL,
                                       `type4` VARCHAR(2000) NULL,
                                       `type5` VARCHAR(2000) NULL,
                                       `type6` VARCHAR(2000) NULL,
                                       `type7` VARCHAR(2000) NULL,
                                       `type8` VARCHAR(2000) NULL,
                                       `type9` VARCHAR(2000) NULL,
                                       `type10` VARCHAR(2000) NULL,
                                       `type11` VARCHAR(2000) NULL,
                                       `type12` VARCHAR(2000) NULL,
                                       `type13` VARCHAR(2000) NULL,
                                       `type14` VARCHAR(2000) NULL,
                                       `type15` VARCHAR(2000) NULL,
                                       `type16` VARCHAR(2000) NULL,
                                       `type17` VARCHAR(2000) NULL,
                                       `type18` VARCHAR(2000) NULL,
                                       `type19` VARCHAR(2000) NULL,
                                       `type20` VARCHAR(2000) NULL,
                                       `type21` VARCHAR(2000) NULL,
                                       `type22` VARCHAR(2000) NULL,
                                       `type23` VARCHAR(2000) NULL,
                                       `type24` VARCHAR(2000) NULL,
                                       `type25` VARCHAR(2000) NULL,
                                       `type26` VARCHAR(2000) NULL,
                                       `type27` VARCHAR(2000) NULL,
                                       `type28` VARCHAR(2000) NULL,
                                       `type29` VARCHAR(2000) NULL,
                                       `type30` VARCHAR(2000) NULL,
                                       `type31` VARCHAR(2000) NULL,
                                       `type32` VARCHAR(2000) NULL,
                                       `type33` VARCHAR(2000) NULL,
                                       `type34` VARCHAR(2000) NULL,
                                       `type35` VARCHAR(2000) NULL,
                                       `type36` VARCHAR(2000) NULL,
                                       `type37` VARCHAR(2000) NULL,
                                       `type38` VARCHAR(2000) NULL,
                                       `type39` VARCHAR(2000) NULL,
                                       `type40` VARCHAR(2000) NULL,
                                       `type41` VARCHAR(2000) NULL,
                                       `type42` VARCHAR(2000) NULL,
                                       `type43` VARCHAR(2000) NULL,
                                       `type44` VARCHAR(2000) NULL,
                                       `type45` VARCHAR(2000) NULL,
                                       `type46` VARCHAR(2000) NULL,
                                       `type47` VARCHAR(2000) NULL,
                                       `type48` VARCHAR(2000) NULL,
                                       `type49` VARCHAR(2000) NULL,
                                       `type50` VARCHAR(2000) NULL,
                                       `type51` VARCHAR(2000) NULL,
                                       `type52` VARCHAR(2000) NULL,
                                       `type53` VARCHAR(2000) NULL,
                                       `type54` VARCHAR(2000) NULL,
                                       `type55` VARCHAR(2000) NULL,
                                       `type56` VARCHAR(2000) NULL,
                                       `type57` VARCHAR(2000) NULL,
                                       `type58` VARCHAR(2000) NULL,
                                       `type59` VARCHAR(2000) NULL,
                                       `type60` VARCHAR(2000) NULL,
                                       `type61` VARCHAR(2000) NULL,
                                       `type62` VARCHAR(2000) NULL,
                                       `type63` VARCHAR(2000) NULL,
                                       `type64` VARCHAR(2000) NULL,
                                       `type65` VARCHAR(2000) NULL,
                                       `type66` VARCHAR(2000) NULL,
                                       `type67` VARCHAR(2000) NULL,
                                       `type68` VARCHAR(2000) NULL,
                                       `type69` VARCHAR(2000) NULL,
                                       `type70` VARCHAR(2000) NULL,
                                       `type71` VARCHAR(2000) NULL,
                                       `type72` VARCHAR(2000) NULL,
                                       `type73` VARCHAR(2000) NULL,
                                       `type74` VARCHAR(2000) NULL,
                                       `type75` VARCHAR(2000) NULL,
                                       `type76` VARCHAR(2000) NULL,
                                       `type77` VARCHAR(2000) NULL,
                                       `type78` VARCHAR(2000) NULL,
                                       `type79` VARCHAR(2000) NULL,
                                       `type80` VARCHAR(2000) NULL,
                                       `type81` VARCHAR(2000) NULL,
                                       `type82` VARCHAR(2000) NULL,
                                       `type83` VARCHAR(2000) NULL,
                                       `type84` VARCHAR(2000) NULL,
                                       `type85` VARCHAR(2000) NULL,
                                       `type86` VARCHAR(2000) NULL,
                                       `type87` VARCHAR(2000) NULL,
                                       `type88` VARCHAR(2000) NULL,
                                       `type89` VARCHAR(2000) NULL,
                                       `type90` VARCHAR(2000) NULL,
                                       `type91` VARCHAR(2000) NULL,
                                       `type92` VARCHAR(2000) NULL,
                                       `type93` VARCHAR(2000) NULL,
                                       `type94` VARCHAR(2000) NULL,
                                       `type95` VARCHAR(2000) NULL,
                                       `type96` VARCHAR(2000) NULL,
                                       `type97` VARCHAR(2000) NULL,
                                       `type98` VARCHAR(2000) NULL,
                                       `type99` VARCHAR(2000) NULL,
                                       `type100` VARCHAR(2000) NULL,
                                       `eid` BIGINT NULL,
                                       `yw` INT NULL
) ENGINE=OLAP
UNIQUE KEY(`zg_id`, `session_id`, `uuid`, `event_id`, `begin_day_id`)
COMMENT 'OLAP'
PARTITION BY RANGE(`begin_day_id`)
()
DISTRIBUTED BY HASH(`zg_id`) BUCKETS 8
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "WEEK",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"dynamic_partition.start" = "-2147483648",
"dynamic_partition.end" = "14",
"dynamic_partition.prefix" = "p",
"dynamic_partition.replication_allocation" = "tag.location.default: 3",
"dynamic_partition.buckets" = "3",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "3",
"dynamic_partition.hot_partition_num" = "0",
"dynamic_partition.reserved_history_periods" = "NULL",
"dynamic_partition.storage_policy" = "",
"dynamic_partition.start_day_of_week" = "1",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

-- --------------------------------------------
-- 5. 用户事件视图 (b_user_event_all)
-- --------------------------------------------
CREATE VIEW IF NOT EXISTS b_user_event_all_{appId} AS
SELECT * FROM b_user_event_attr_{appId};

-- --------------------------------------------
-- 6. 用户加入表 (t_user_join)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS t_user_join_{appId} (
                                                   `zg_id`        BIGINT,
                                                   `device_id`    BIGINT,
                                                   `begin_date`   BIGINT,
                                                   `begin_day_id` INT,
                                                   `uuid`         STRING,
                                                   `yearweek`     INT,
                                                   `yearmonth`    INT,
                                                   `platform`     INT
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`begin_day_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 7. 用户活跃表 (t_user_active)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS t_user_active_{appId} (
                                                     `zg_id`        BIGINT,
                                                     `begin_day_id` INT,
                                                     `times`        INT,
                                                     `yearweek`     INT,
                                                     `yearmonth`    INT,
                                                     `platform`     INT
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`begin_day_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 8. 用户时长表 (t_user_duration)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS t_user_duration_{appId} (
                                                       `zg_id`     BIGINT,
                                                       `day_id`    INT,
                                                       `period`    STRING,
                                                       `duration`  BIGINT,
                                                       `times`     INT,
                                                       `yearweek`  INT,
                                                       `yearmonth` INT,
                                                       `platform`  INT
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`day_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 9. 用户详情汇总表 (t_user_detail_sum)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS t_user_detail_sum_{appId} (
                                                         `zg_id`       BIGINT,
                                                         `platform`    INT,
                                                         `visit_times` INT,
                                                         `duration`    BIGINT,
                                                         `attr1`       STRING,
                                                         `attr2`       STRING,
                                                         `attr3`       STRING,
                                                         `attr4`       STRING,
                                                         `attr5`       STRING
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`zg_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 10. 用户详情表 (t_user_detail)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS t_user_detail_{appId} (
                                                     `zg_id`               BIGINT,
                                                     `platform`            INT,
                                                     `first_visit_time`    BIGINT,
                                                     `last_visit_time`     BIGINT,
                                                     `visit_times`         INT,
                                                     `duration`            BIGINT,
                                                     `first_version`       STRING,
                                                     `first_channel`       STRING,
                                                     `current_app_version` STRING,
                                                     `current_app_channel` STRING,
                                                     `first_website`       STRING,
                                                     `utm_source`          STRING,
                                                     `utm_medium`          STRING,
                                                     `utm_campaign`        STRING,
                                                     `utm_content`         STRING,
                                                     `utm_term`            STRING,
                                                     `first_referrer_url`  STRING,
                                                     `current_country`     STRING,
                                                     `current_area`        STRING,
                                                     `current_city`        STRING,
                                                     `current_mccmnc`      INT,
                                                     `current_bs`          STRING,
                                                     `current_bv`          INT,
                                                     `current_os`          STRING,
                                                     `current_ov`          INT,
                                                     `current_l`           INT,
                                                     `current_h`           INT,
                                                     `current_device_brand` STRING,
                                                     `current_device_model` STRING,
                                                     `is_registered`       INT,
                                                     `attr1`               STRING,
                                                     `attr2`               STRING,
                                                     `attr3`               STRING,
                                                     `attr4`               STRING,
                                                     `attr5`               STRING
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`zg_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 11. 首次用户加入表 (f_user_join)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS f_user_join_{appId} (
                                                   `zg_id`        BIGINT,
                                                   `device_id`    BIGINT,
                                                   `begin_date`   BIGINT,
                                                   `begin_day_id` INT,
                                                   `uuid`         STRING,
                                                   `yearweek`     INT,
                                                   `yearmonth`    INT,
                                                   `platform`     INT
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`begin_day_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 12. 首次用户详情汇总表 (f_user_detail_sum)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS f_user_detail_sum_{appId} (
                                                         `zg_id`       BIGINT,
                                                         `visit_times` INT,
                                                         `duration`    BIGINT,
                                                         `attr1`       STRING,
                                                         `attr2`       STRING,
                                                         `attr3`       STRING,
                                                         `attr4`       STRING,
                                                         `attr5`       STRING
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`zg_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 13. 首次用户详情表 (f_user_detail)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS f_user_detail_{appId} (
                                                     `zg_id`               BIGINT,
                                                     `first_visit_time`    BIGINT,
                                                     `last_visit_time`     BIGINT,
                                                     `first_version`       STRING,
                                                     `first_channel`       STRING,
                                                     `current_app_version` STRING,
                                                     `current_app_channel` STRING,
                                                     `first_website`       STRING,
                                                     `utm_source`          STRING,
                                                     `utm_medium`          STRING,
                                                     `utm_campaign`        STRING,
                                                     `utm_content`         STRING,
                                                     `utm_term`            STRING,
                                                     `first_referrer_url`  STRING,
                                                     `current_country`     STRING,
                                                     `current_area`        STRING,
                                                     `current_city`        STRING,
                                                     `current_mccmnc`      INT,
                                                     `current_bs`          STRING,
                                                     `current_bv`          INT,
                                                     `current_os`          STRING,
                                                     `current_ov`          INT,
                                                     `current_l`           INT,
                                                     `current_h`           INT,
                                                     `current_device_brand` STRING,
                                                     `current_device_model` STRING,
                                                     `is_registered`       INT,
                                                     `attr1`               STRING,
                                                     `attr2`               STRING,
                                                     `attr3`               STRING,
                                                     `attr4`               STRING,
                                                     `attr5`               STRING
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`zg_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 14. 全量用户汇总表 (f_user_all)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS f_user_all_{appId} (
                                                  `zg_id`                    BIGINT,
                                                  `first_visit_time`         BIGINT,
                                                  `last_visit_time`          BIGINT,
                                                  `first_version`            STRING,
                                                  `first_channel`            STRING,
                                                  `current_app_version`      STRING,
                                                  `current_app_channel`      STRING,
                                                  `first_website`            STRING,
                                                  `utm_source`               STRING,
                                                  `utm_medium`               STRING,
                                                  `utm_campaign`             STRING,
                                                  `utm_content`              STRING,
                                                  `utm_term`                 STRING,
                                                  `first_referrer_url`       STRING,
                                                  `current_country`          STRING,
                                                  `current_area`             STRING,
                                                  `current_city`             STRING,
                                                  `current_mccmnc`           INT,
                                                  `current_bs`               STRING,
                                                  `current_bv`               INT,
                                                  `current_os`               STRING,
                                                  `current_ov`               INT,
                                                  `current_l`                INT,
                                                  `current_h`                INT,
                                                  `current_device_brand`     STRING,
                                                  `current_device_model`     STRING,
                                                  `is_registered`            INT,
                                                  `device_id`                BIGINT,
                                                  `begin_date`               BIGINT,
                                                  `begin_day_id`             INT,
                                                  `uuid`                     STRING,
                                                  `yearweek`                 INT,
                                                  `yearmonth`                INT,
                                                  `platform`                 SMALLINT,
                                                  `visit_times`              BIGINT,
                                                  `duration`                 BIGINT,
                                                  `current_mccmnc_name`      STRING,
                                                  `current_device_model_name` STRING,
                                                  `platform_name`            STRING,
                                                  `attr1`                    STRING,
                                                  `attr2`                    STRING,
                                                  `attr3`                    STRING,
                                                  `attr4`                    STRING,
                                                  `attr5`                    STRING
) DUPLICATE KEY(`zg_id`)
    DISTRIBUTED BY HASH(`zg_id`)  BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );

-- --------------------------------------------
-- 15. 匿名用户视图 (b_user_anonymous)
-- --------------------------------------------
CREATE VIEW IF NOT EXISTS b_user_anonymous_{appId} AS
SELECT
    zg_id,
    MAX(user_id) AS user_id,
    CASE
        WHEN MAX(user_id) IS NULL THEN '匿名'
        ELSE '实名'
        END AS is_anonymous
FROM b_user_{appId}
GROUP BY zg_id;

-- 1. 设备映射表
-- KVRocks Key: d:{appId}:{deviceMd5}
-- 映射关系: deviceMd5 → zgDeviceId
CREATE TABLE IF NOT EXISTS dwd.dwd_id_device
(
    app_id          INT             NOT NULL COMMENT '应用ID',
    device_md5      VARCHAR(64)     NOT NULL COMMENT '设备指纹MD5',
    zg_device_id    BIGINT          NOT NULL COMMENT '诸葛设备ID'
    )
    UNIQUE KEY(app_id, device_md5)
    COMMENT 'ID映射-设备表 (deviceMd5 → zgDeviceId)'
    DISTRIBUTED BY HASH(app_id, device_md5) BUCKETS 8
    PROPERTIES (
                   "replication_num" = "3",
                   "enable_unique_key_merge_on_write" = "true"
               );

-- 2. 用户映射表
-- KVRocks Key: u:{appId}:{cuid}
-- 映射关系: cuid → zgUserId
CREATE TABLE IF NOT EXISTS dwd.dwd_id_user
(
    app_id          INT             NOT NULL COMMENT '应用ID',
    cuid            VARCHAR(256)    NOT NULL COMMENT '用户标识 (手机号/邮箱/自定义ID)',
    zg_user_id      BIGINT          NOT NULL COMMENT '诸葛用户ID'
    )
    UNIQUE KEY(app_id, cuid)
    COMMENT 'ID映射-用户表 (cuid → zgUserId)'
    DISTRIBUTED BY HASH(app_id, cuid) BUCKETS 8
    PROPERTIES (
                   "replication_num" = "3",
                   "enable_unique_key_merge_on_write" = "true"
               );

-- 3. 设备ID→诸葛ID映射表
-- KVRocks Key: dz:{appId}:{zgDeviceId}
-- 映射关系: zgDeviceId → zgId
CREATE TABLE IF NOT EXISTS dwd.dwd_id_device_zgid
(
    app_id          INT             NOT NULL COMMENT '应用ID',
    zg_device_id    BIGINT          NOT NULL COMMENT '诸葛设备ID',
    zg_id           BIGINT          NOT NULL COMMENT '诸葛ID'
)
    UNIQUE KEY(app_id, zg_device_id)
    COMMENT 'ID映射-设备诸葛ID表 (zgDeviceId → zgId)'
    DISTRIBUTED BY HASH(app_id, zg_device_id) BUCKETS 8
    PROPERTIES (
                   "replication_num" = "3",
                   "enable_unique_key_merge_on_write" = "true"
               );

-- 4. 用户ID→诸葛ID映射表
-- KVRocks Key: uz:{appId}:{zgUserId}
-- 映射关系: zgUserId → zgId
CREATE TABLE IF NOT EXISTS dwd.dwd_id_user_zgid
(
    app_id          INT             NOT NULL COMMENT '应用ID',
    zg_user_id      BIGINT          NOT NULL COMMENT '诸葛用户ID',
    zg_id           BIGINT          NOT NULL COMMENT '诸葛ID'
)
    UNIQUE KEY(app_id, zg_user_id)
    COMMENT 'ID映射-用户诸葛ID表 (zgUserId → zgId)'
    DISTRIBUTED BY HASH(app_id, zg_user_id) BUCKETS 8
    PROPERTIES (
                   "replication_num" = "3",
                   "enable_unique_key_merge_on_write" = "true"
               );

-- 5. 诸葛ID→用户ID映射表 (反向映射)
-- KVRocks Key: zu:{appId}:{zgId}
-- 映射关系: zgId → zgUserId
CREATE TABLE IF NOT EXISTS dwd.dwd_id_zgid_user
(
    app_id          INT             NOT NULL COMMENT '应用ID',
    zg_id           BIGINT          NOT NULL COMMENT '诸葛ID',
    zg_user_id      BIGINT          NOT NULL COMMENT '诸葛用户ID'
)
    UNIQUE KEY(app_id, zg_id)
    COMMENT 'ID映射-诸葛ID用户表 (zgId → zgUserId)'
    DISTRIBUTED BY HASH(app_id, zg_id) BUCKETS 8
    PROPERTIES (
                   "replication_num" = "3",
                   "enable_unique_key_merge_on_write" = "true"
               );