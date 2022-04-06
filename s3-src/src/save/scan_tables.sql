
CREATE TABLE `inv_storage_maints` (
	`id` INT(10) NOT NULL AUTO_INCREMENT,
	`inv_storage_scan_id` INT(10) NOT NULL,
	`inv_node_id` SMALLINT(5) UNSIGNED NOT NULL,
	`keymd5` CHAR(32) NOT NULL COLLATE 'utf8_general_ci',
	`size` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0',
	`file_created` TIMESTAMP NULL DEFAULT NULL,
	`created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`file_removed` TIMESTAMP NULL DEFAULT NULL,
	`maint_status` ENUM(
            'review',
            'hold',
            'delete',
            'removed',
            'objremoved',
            'admin',
            'note',
            'error',
            'unknown'
            ) NOT NULL DEFAULT 'unknown' COLLATE 'utf8_general_ci',
	`maint_type` ENUM(
            'non-ark',
            'missing-ark',
            'orphan-copy',
            'missing-file',
            'unknown'
            ) NOT NULL DEFAULT 'unknown' COLLATE 'utf8_general_ci',
	`s3key` MEDIUMTEXT NOT NULL COLLATE 'utf8mb4_unicode_ci',
	`note` MEDIUMTEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci',
	PRIMARY KEY (`id`) USING BTREE,
	UNIQUE INDEX `keymd5_idx` (`inv_node_id`, `keymd5`) USING BTREE,
	INDEX `type_idx` (`maint_type`) USING BTREE,
	INDEX `status_idx` (`maint_status`) USING BTREE,
	CONSTRAINT `inv_scans_ibfk_2` FOREIGN KEY (`inv_node_id`) REFERENCES `inv`.`inv_nodes` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB
ROW_FORMAT=DYNAMIC
;

CREATE TABLE `inv_storage_scans` (
	`id` INT(10) NOT NULL AUTO_INCREMENT,
	`inv_node_id` SMALLINT(5) UNSIGNED NOT NULL,
	`created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`scan_status` ENUM(
            'pending',
            'started',
            'completed',
            'cancelled',
            'failed',
            'unknown'
            ) NOT NULL DEFAULT 'unknown' COLLATE 'utf8mb4_general_ci',
	`scan_type` ENUM(
            'list',
            'next',
            'delete',
            'build',
            'unknown'
            ) NOT NULL DEFAULT 'unknown' COLLATE 'utf8mb4_general_ci',
	`keys_processed` BIGINT(20) NOT NULL DEFAULT '0',
	`key_list_name` VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_unicode_ci',
	`last_s3_key` MEDIUMTEXT NOT NULL COLLATE 'utf8mb4_unicode_ci',
	PRIMARY KEY (`id`) USING BTREE,
	INDEX `scan_type_idx` (`scan_type`) USING BTREE,
	INDEX `scan_status_idx` (`scan_status`) USING BTREE,
	INDEX `inv_scans_node_id_ibfk_3` (`inv_node_id`) USING BTREE,
	CONSTRAINT `inv_scans_node_id_ibfk_3` FOREIGN KEY (`inv_node_id`) REFERENCES `inv`.`inv_nodes` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB
ROW_FORMAT=DYNAMIC
;

