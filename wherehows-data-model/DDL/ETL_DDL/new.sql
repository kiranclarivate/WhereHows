
-- https://github.com/linkedin/WhereHows/issues/1041
-- this table doesn't exist in Linkedin Wherehows github repo, the following DDL was from old version of the Wherehows, may or may not be valid

CREATE TABLE `log_dataset_instance_load_status` (
  `dataset_id` int(11) NOT NULL DEFAULT '0',
  `db_id` smallint(6) NOT NULL DEFAULT '0',
  `dataset_type` varchar(30) NOT NULL COMMENT 'hive,teradata,oracle,hdfs...',
  `dataset_native_name` varchar(200) NOT NULL,
  `operation_type` varchar(50) DEFAULT NULL COMMENT 'load, merge, compact, update, delete',
  `partition_grain` varchar(30) NOT NULL DEFAULT '' COMMENT 'snapshot, delta, daily, daily, monthly...',
  `partition_expr` varchar(500) DEFAULT NULL COMMENT 'partition name or expression',
  `data_time_expr` varchar(20) NOT NULL COMMENT 'datetime literal of the data datetime',
  `data_time_epoch` int(11) NOT NULL COMMENT 'epoch second of the data datetime',
  `record_count` bigint(20) DEFAULT NULL,
  `size_in_byte` bigint(20) DEFAULT NULL,
  `log_time_epoch` int(11) NOT NULL COMMENT 'When data is loaded or published',
  `ref_dataset_type` varchar(30) DEFAULT NULL COMMENT 'Refer to the underlying dataset',
  `ref_db_id` int(11) DEFAULT NULL COMMENT 'Refer to db of the underlying dataset',
  `ref_uri` varchar(300) DEFAULT NULL COMMENT 'Table name or HDFS location',
  `last_modified` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`dataset_id`,`db_id`,`partition_grain`,`data_time_epoch`),
  KEY `dataset_native_name` (`dataset_native_name`,`partition_expr`),
  KEY `ref_uri` (`ref_uri`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
 COMMENT='Capture the events of load/publish operation for dataset';

CREATE TABLE dataset_classification (
	dataset_id int(10) unsigned NOT NULL,
	dataset_urn varchar(200) NOT NULL,
	classification_result varchar(200) DEFAULT NULL,
	last_modified DATE      DEFAULT NULL,
	PRIMARY KEY(dataset_id),
	UNIQUE KEY(dataset_urn)
)ENGINE = InnoDB
  DEFAULT CHARSET = latin1;


CREATE TABLE user_workspace (
    `user_name`           varchar(50) NOT NULL,
    `db_id`             SMALLINT UNSIGNED,
    `notebook_id`       VARCHAR(10)  NOT NULL,
    `note_url`          VARCHAR(100)  NOT NULL,
    `interpreter_name`  VARCHAR(50)   DEFAULT NULL,
    `last_modified`     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`user_name`, `db_id`)
  )ENGINE = InnoDB
DEFAULT CHARSET = latin1;

CREATE TABLE db_info (
    db_id SMALLINT UNSIGNED NOT NULL,
    type VARCHAR(50)   COMMENT 'hive, oracle, mySQL, teradata etc...' NOT NULL,
    alias VARCHAR(10) COMMENT 'optional, will be used as part of notebook name' NULL,
    zeppelin_host VARCHAR(100)  NOT NULL,
    interpreter_name varchar(20) NOT NULL COMMENT 'used at the beginning of each paragraph such as %mysql',
    database_name VARCHAR(50),
    last_modified     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (db_id),
    UNIQUE KEY(alias)
)ENGINE = InnoDB
 DEFAULT CHARSET = latin1;


