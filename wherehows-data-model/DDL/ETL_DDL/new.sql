
-- https://github.com/linkedin/WhereHows/issues/1041
-- this table doesn't exist in Linkedin Wherehows github repo, the following DDL was from old version of the Wherehows, may or may not be valid

-- Combine multiple data log status from Kafka events into a status table
SET TIME_ZONE='US/Pacific';	-- this needs to be customized based on your time zone
SELECT @@session.time_zone, current_timestamp;

CREATE TABLE log_dataset_instance_load_status  (
	dataset_id         	int(11) UNSIGNED NOT NULL DEFAULT '0',
	db_id              	smallint(6) NOT NULL DEFAULT '0',
	dataset_type       	varchar(30) COMMENT 'hive,teradata,oracle,hdfs...'  NOT NULL,
	dataset_native_name	varchar(200) NOT NULL,
	operation_type     	varchar(50) COMMENT 'load, merge, compact, update, delete'  NULL,
	partition_grain    	varchar(30) COMMENT 'snapshot, delta, daily, daily, monthly...'  NOT NULL,
	partition_expr     	varchar(500) COMMENT 'partition name or expression'  NOT NULL,
	data_time_expr     	varchar(20) COMMENT 'datetime literal of the data datetime'  NOT NULL,
	data_time_epoch    	int(11) COMMENT 'epoch second of the data datetime'  NOT NULL,
	record_count       	bigint(20) NULL,
	size_in_byte       	bigint(20) NULL,
	log_time_epoch     	int(11) COMMENT 'When data is loaded or published'  NOT NULL,
	ref_dataset_type   	varchar(30) COMMENT 'Refer to the underlying dataset'  NULL,
	ref_db_id          	int(11) COMMENT 'Refer to db of the underlying dataset'  NULL,
	ref_uri            	varchar(300) COMMENT 'Table name or HDFS location'  NULL,
	last_modified      	timestamp NULL,
	PRIMARY KEY(dataset_id,db_id,data_time_epoch,partition_grain,partition_expr),
	KEY(dataset_native_name),
	KEY(ref_uri)
)
ENGINE = InnoDB
CHARACTER SET latin1
AUTO_INCREMENT = 0
COMMENT = 'Capture the load/publish ops for dataset instance'
PARTITION BY RANGE COLUMNS (data_time_epoch)
( 	PARTITION P201801 VALUES LESS THAN (unix_timestamp(date'2018-02-01')),
  	PARTITION P201802 VALUES LESS THAN (unix_timestamp(date'2018-03-01')),
  	PARTITION P201803 VALUES LESS THAN (unix_timestamp(date'2018-04-01')),
  	PARTITION P201804 VALUES LESS THAN (unix_timestamp(date'2018-05-01')),
  	PARTITION P201805 VALUES LESS THAN (unix_timestamp(date'2018-06-01')),
  	PARTITION P201806 VALUES LESS THAN (unix_timestamp(date'2018-07-01')),
  	PARTITION P201807 VALUES LESS THAN (unix_timestamp(date'2018-08-01')),
  	PARTITION P201808 VALUES LESS THAN (unix_timestamp(date'2018-09-01')),
  	PARTITION P201809 VALUES LESS THAN (unix_timestamp(date'2018-10-01')),
  	PARTITION P201810 VALUES LESS THAN (unix_timestamp(date'2018-11-01')),
  	PARTITION P201811 VALUES LESS THAN (unix_timestamp(date'2018-12-01')),
	PARTITION P201812 VALUES LESS THAN (unix_timestamp(date'2019-01-01')),
	PARTITION P201901 VALUES LESS THAN (unix_timestamp(date'2019-02-01')),
	PARTITION P201902 VALUES LESS THAN (unix_timestamp(date'2019-03-01')),
    PARTITION P201903 VALUES LESS THAN (unix_timestamp(date'2019-04-01')),
    PARTITION P201904 VALUES LESS THAN (unix_timestamp(date'2019-05-01')),
    PARTITION P201905 VALUES LESS THAN (unix_timestamp(date'2019-06-01')),
    PARTITION P201906 VALUES LESS THAN (unix_timestamp(date'2019-07-01')),
    PARTITION P201907 VALUES LESS THAN (unix_timestamp(date'2019-08-01')),
    PARTITION P201908 VALUES LESS THAN (unix_timestamp(date'2019-09-01')),
    PARTITION P201909 VALUES LESS THAN (unix_timestamp(date'2019-10-01')),
    PARTITION P201910 VALUES LESS THAN (unix_timestamp(date'2019-11-01')),
    PARTITION P201911 VALUES LESS THAN (unix_timestamp(date'2019-12-01')),
    PARTITION P201912 VALUES LESS THAN (unix_timestamp(date'2020-01-01')),
  	PARTITION P203507 VALUES LESS THAN (unix_timestamp(date'2035-08-01'))
) ;