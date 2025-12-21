-- ================================================================
-- ID 映射 Doris 表结构 (5张表，统一3字段结构，UNIQUE KEY)
-- 数据库: dwd
-- ================================================================

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


-- ================================================================
-- 对照关系
-- ================================================================
-- 
-- | Kafka type    | KVRocks Key 模式              | Doris 表             | 主键                    |
-- |---------------|-------------------------------|----------------------|------------------------|
-- | DEVICE        | d:{appId}:{deviceMd5}         | dwd_id_device        | (app_id, device_md5)   |
-- | USER          | u:{appId}:{cuid}              | dwd_id_user          | (app_id, cuid)         |
-- | DEVICE_ZGID   | dz:{appId}:{zgDeviceId}       | dwd_id_device_zgid   | (app_id, zg_device_id) |
-- | USER_ZGID     | uz:{appId}:{zgUserId}         | dwd_id_user_zgid     | (app_id, zg_user_id)   |
-- | ZGID_USER     | zu:{appId}:{zgId}             | dwd_id_zgid_user     | (app_id, zg_id)        |


-- ================================================================
-- 常用查询示例
-- ================================================================

-- 1. 根据设备MD5查询设备ID
-- SELECT zg_device_id FROM dwd.dwd_id_device 
-- WHERE app_id = 1001 AND device_md5 = 'abc123';

-- 2. 根据用户标识查询用户ID
-- SELECT zg_user_id FROM dwd.dwd_id_user 
-- WHERE app_id = 1001 AND cuid = 'user_001';

-- 3. 根据设备ID查询诸葛ID
-- SELECT zg_id FROM dwd.dwd_id_device_zgid 
-- WHERE app_id = 1001 AND zg_device_id = 7890123456789;

-- 4. 根据用户ID查询诸葛ID
-- SELECT zg_id FROM dwd.dwd_id_user_zgid 
-- WHERE app_id = 1001 AND zg_user_id = 8901234567890;

-- 5. 根据诸葛ID查询绑定的用户ID
-- SELECT zg_user_id FROM dwd.dwd_id_zgid_user 
-- WHERE app_id = 1001 AND zg_id = 9012345678901;

-- 6. 统计各应用的设备数
-- SELECT app_id, COUNT(*) as device_count 
-- FROM dwd.dwd_id_device 
-- GROUP BY app_id;

-- 7. 统计各应用的实名用户数 (有绑定用户的诸葛ID)
-- SELECT app_id, COUNT(*) as identified_user_count 
-- FROM dwd.dwd_id_zgid_user 
-- GROUP BY app_id;

-- 8. 完整的ID链路查询: 从设备MD5到用户标识
-- SELECT 
--     d.device_md5,
--     d.zg_device_id,
--     dz.zg_id,
--     zu.zg_user_id,
--     u.cuid
-- FROM dwd.dwd_id_device d
-- JOIN dwd.dwd_id_device_zgid dz ON d.app_id = dz.app_id AND d.zg_device_id = dz.zg_device_id
-- LEFT JOIN dwd.dwd_id_zgid_user zu ON dz.app_id = zu.app_id AND dz.zg_id = zu.zg_id
-- LEFT JOIN dwd.dwd_id_user u ON zu.app_id = u.app_id AND zu.zg_user_id = u.zg_user_id
-- WHERE d.app_id = 1001 AND d.device_md5 = 'abc123';
