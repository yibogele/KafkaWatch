/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.10.22
 Source Server Type    : PostgreSQL
 Source Server Version : 90511
 Source Host           : 192.168.10.22:5432
 Source Catalog        : iot
 Source Schema         : public

 Target Server Type    : PostgreSQL
 Target Server Version : 90511
 File Encoding         : 65001

 Date: 22/10/2019 14:24:08
*/


-- ----------------------------
-- Table structure for kafka_iot_stat
-- ----------------------------
DROP TABLE IF EXISTS "public"."kafka_iot_stat";
CREATE TABLE "public"."kafka_iot_stat" (
  "devid" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "productkey" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "date" date NOT NULL,
  "count" int4 NOT NULL,
  "msgtype" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "update" time NOT NULL
)
;

-- ----------------------------
-- Primary Key structure for table kafka_iot_stat
-- ----------------------------
-- ALTER TABLE "public"."kafka_iot_stat" ADD CONSTRAINT "kafka_iot_stat_pkey" PRIMARY KEY ("devid", "msgtype", "date");


-- ---------------------
-- create partition table
-- ----------------------------
--CREATE TABLE kafka_iot_stat_1910 (
--	CHECK (date >= DATE '2019-10-01' AND date < DATE '2019-11-01')
--) inherits (kafka_iot_stat);

CREATE TABLE kafka_iot_stat_1911 (
	CHECK (date >= DATE '2019-11-01' AND date < DATE '2019-12-01')
) inherits (kafka_iot_stat);

CREATE TABLE kafka_iot_stat_1912 (
	CHECK (date >= DATE '2019-12-01' AND date < DATE '2020-01-01')
) inherits (kafka_iot_stat);

CREATE TABLE kafka_iot_stat_2001 (
	CHECK (date >= DATE '2020-01-01' AND date < DATE '2020-02-01')
) inherits (kafka_iot_stat);

CREATE TABLE kafka_iot_stat_2002 (
	CHECK (date >= DATE '2020-02-01' AND date < DATE '2020-03-01')
) inherits (kafka_iot_stat);

CREATE TABLE kafka_iot_stat_2003 (
	CHECK (date >= DATE '2020-03-01' AND date < DATE '2020-04-01')
) inherits (kafka_iot_stat);

CREATE TABLE kafka_iot_stat_2004 (
	CHECK (date >= DATE '2020-04-01' AND date < DATE '2020-05-01')
) inherits (kafka_iot_stat);

CREATE TABLE kafka_iot_stat_2005 (
	CHECK (date >= DATE '2020-05-01' AND date < DATE '2020-06-01')
) inherits (kafka_iot_stat);

CREATE TABLE kafka_iot_stat_2006 (
	CHECK (date >= DATE '2020-06-01' AND date < DATE '2020-07-01')
) inherits (kafka_iot_stat);

-- -------------------------
-- create trigger function
-- ------------------------------
CREATE OR REPLACE FUNCTION iotstat_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
	IF ( NEW.date >= DATE '2019-11-01' AND NEW.date < DATE '2019-12-01' ) THEN
        INSERT INTO kafka_iot_stat_1911 VALUES (NEW.*);
    ELSIF ( NEW.date >= DATE '2019-12-01' AND NEW.date < DATE '2020-01-01' ) THEN
        INSERT INTO kafka_iot_stat_1912 VALUES (NEW.*);
    ELSIF ( NEW.date >= DATE '2020-01-01' AND NEW.date < DATE '2020-02-01' ) THEN
        INSERT INTO kafka_iot_stat_2001 VALUES (NEW.*);
    ELSIF ( NEW.date >= DATE '2020-02-01' AND NEW.date < DATE '2020-03-01' ) THEN
        INSERT INTO kafka_iot_stat_2002 VALUES (NEW.*);
    ELSIF ( NEW.date >= DATE '2020-03-01' AND NEW.date < DATE '2020-04-01' ) THEN
        INSERT INTO kafka_iot_stat_2003 VALUES (NEW.*);
    ELSIF ( NEW.date >= DATE '2020-04-01' AND NEW.date < DATE '2020-05-01' ) THEN
        INSERT INTO kafka_iot_stat_2004 VALUES (NEW.*);
    ELSIF ( NEW.date >= DATE '2020-05-01' AND NEW.date < DATE '2020-06-01' ) THEN
        INSERT INTO kafka_iot_stat_2005 VALUES (NEW.*);
    ELSIF ( NEW.date >= DATE '2020-06-01' AND NEW.date < DATE '2020-07-01' ) THEN
        INSERT INTO kafka_iot_stat_2006 VALUES (NEW.*);

    ELSE
        RAISE EXCEPTION 'Date out of range.  Fix the iotstat_insert_trigger() function!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

-- -------------------
-- create trigger
-- ----------------------
CREATE TRIGGER insert_kafkaiotstat_trigger
    BEFORE INSERT ON kafka_iot_stat
    FOR EACH ROW EXECUTE PROCEDURE iotstat_insert_trigger();
