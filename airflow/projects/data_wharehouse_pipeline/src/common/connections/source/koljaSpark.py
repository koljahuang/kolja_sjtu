import logging
from pyspark.sql import SparkSession
from config import settings

LOG_NAME = settings.LOG_NAME
module_logger = logging.getLogger(f'{LOG_NAME}')


class SparkFactory:

    def __init__(self) -> None:
        self.spark = None

    def __enter__(self):
        self.spark = SparkSession.builder \
            .config("spark.sql.parquet.binaryAsString", "true") \
            .config("spark.driver.userClassPathFirst", "true") \
            .config("spark.excutor.userClassPathFirst", "true") \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .config("hive.execution.engine", "spark") \
            .config("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec") \
            .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.yarn.am.waitTime", "300s") \
            .config("spark.sql.storeAssignmentPolicy", "LEGACY") \
            .config("spark.yarn.maxAppAttempts", "1") \
            .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
            .config("spark.hadoop.parquet.enable.summary-metadata", "false") \
            .config("spark.sql.parquet.mergeSchema", "true") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .config("spark.sql.hive.metastorePartitionPruning", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.crossJoin.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.join.enabled", "true") \
            .config("spark.sql.hive.convertMetastoreParquet", "false") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
            .config("spark.sql.hive.verifyPartitionPath", "false") \
            .config("spark.sql.files.ignoreMissingFiles", "true") \
            .config("spark.sql.files.ignoreCorruptFiles", "true") \
            .config("mapred.input.dir.recursive", "true") \
            .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
            .config("spark.sql.broadcastTimeout", "72000") \
            .enableHiveSupport() \
            .getOrCreate()
        # TODO some args move to airflow
        module_logger.info('spark session is created successfully')
        return self.spark

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.spark.stop()
        if exc_type == None:
            module_logger.info('spark session is closed successfully')
        else:
            module_logger.error(f'spark session closed failed cause {exc_value}')

