from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import logging
import time
import uuid

# Configuration constants - Customize these for your project
DATABASE_NAME = "analytics_layer"
TARGET_TABLE = f"{DATABASE_NAME}.processed_data"
LOG_TABLE = "metadata_layer.pipeline_log"
PIPELINE_NAME = "data_processing_pipeline"
TABLE_PATH = f"/user/hive/warehouse/{DATABASE_NAME}.db/processed_data"
LOG_TABLE_PATH = f"/user/hive/warehouse/metadata_layer.db/pipeline_log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_spark_session():
    """Initialize and return Spark session with Hudi support"""
    try:
        spark = (
            SparkSession.builder
            .appName("GenericDataPipeline")
            .enableHiveSupport()
            # Core Spark SQL optimizations
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.caseSensitive", "false")
            # Serialization
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            # Hudi integration
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            # Partitioning
            .config("spark.sql.shuffle.partitions", "100")
            .config("spark.default.parallelism", "100")
            # Memory settings
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.5")
            # File processing optimizations
            .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
            .config("spark.sql.files.maxPartitionBytes", "256MB")
            # Compression
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.orc.compression.codec", "snappy")
            .config("spark.rdd.compress", "true")
            # Hudi-specific configurations
            .config("hoodie.schema.on.read.enable", "true")
            .config("hoodie.datasource.write.drop.partition.columns", "false")
            .config("hoodie.avro.schema.validate", "false")
            .config("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
            .config("hoodie.clean.automatic", "true")
            .config("hoodie.clean.async", "false")
            .config("hoodie.metadata.enable", "false")
            .getOrCreate()
        )
        logger.info("Spark session initialized with Hudi support")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise

def create_database_and_table(spark):
    """Create the database and Hudi tables"""
    try:
        # Create databases
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        spark.sql("CREATE DATABASE IF NOT EXISTS metadata_layer")
        logger.info(f"Databases {DATABASE_NAME} and metadata_layer created/verified")
        
        # Create main data table (customize schema as needed)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.processed_data (
                record_id STRING,
                entity_id STRING,
                event_timestamp TIMESTAMP,
                event_type STRING,
                data_field_1 STRING,
                data_field_2 STRING,
                numeric_field_1 DOUBLE,
                numeric_field_2 INT,
                processing_date DATE,
                load_timestamp TIMESTAMP
            )
            USING HUDI
            TBLPROPERTIES (
                type = 'cow',
                primaryKey = 'record_id',
                preCombineField = 'load_timestamp'
            )
        """)
        logger.info(f"Hudi table {DATABASE_NAME}.processed_data created/verified")        
        # Create log table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS metadata_layer.pipeline_log (
                pipeline_name STRING,
                last_run_timestamp BIGINT,
                current_run_timestamp BIGINT,
                status STRING,
                records_processed INT,
                processing_time_seconds DOUBLE,
                error_message STRING,
                created_at BIGINT
            )
            USING HUDI
            TBLPROPERTIES (
                type = 'cow',
                primaryKey = 'pipeline_name,created_at',
                preCombineField = 'created_at'
            )
        """)
        logger.info("Log table metadata_layer.pipeline_log created/verified")
    #################################################################    
    except Exception as e:
        logger.error(f"Error creating database/table: {str(e)}")
        raise

def convert_ts_to_timestamp(ts_col):
    """Convert timestamp string (YYYYMMDDHHMMSS) to timestamp"""
    return to_timestamp(col(ts_col), "yyyyMMddHHmmss")

def get_last_run_timestamp(spark):
    """Get the last successful run timestamp from log table"""
    try:
        last_run_df = spark.sql(f"""
            SELECT current_run_timestamp 
            FROM metadata_layer.pipeline_log 
            WHERE pipeline_name = '{PIPELINE_NAME}' 
            AND status = 'SUCCESS'
            ORDER BY current_run_timestamp DESC 
            LIMIT 1
        """)
        if last_run_df.count() > 0:
            timestamp = last_run_df.collect()[0]['current_run_timestamp']
            logger.info(f"Last run timestamp: {datetime.fromtimestamp(timestamp/1000)}")
            return timestamp
        else:
            logger.info("No previous successful run found, assuming first run")
            return None
    except Exception as e:
        logger.warning(f"Could not get last run timestamp, assuming first run: {str(e)}")
        return None

def create_log_entry(spark, last_run_ts, current_run_ts, status, records_processed=0, 
                    processing_time=0.0, error_message=None):
    """Create a log entry in the Hudi log table"""
    try:
        current_time_ms = int(time.time() * 1000)
        log_data = [(
            PIPELINE_NAME,
            last_run_ts if last_run_ts else 0,
            current_run_ts,
            status,
            records_processed,
            processing_time,
            error_message,
            current_time_ms
        )]
        #################################################################
        schema = StructType([
            StructField("pipeline_name", StringType(), False),
            StructField("last_run_timestamp", LongType(), True),
            StructField("current_run_timestamp", LongType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("processing_time_seconds", DoubleType(), True),
            StructField("error_message", StringType(), True),
            StructField("created_at", LongType(), False)
        ])
        #################################################################
        log_df = spark.createDataFrame(log_data, schema)
        #################################################################
        log_df.write \
            .format("hudi") \
            .option("hoodie.table.name", "pipeline_log") \
            .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
            .option("hoodie.datasource.write.operation", "upsert") \
            .option("hoodie.datasource.write.recordkey.field", "pipeline_name,created_at") \
            .option("hoodie.datasource.write.precombine.field", "created_at") \
            .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator") \
            .option("hoodie.datasource.hive_sync.enable", "true") \
            .option("hoodie.datasource.hive_sync.database", "metadata_layer") \
            .option("hoodie.datasource.hive_sync.table", "pipeline_log") \
            .option("hoodie.datasource.hive_sync.mode", "hms") \
            .option("hoodie.metadata.enable", "false") \
            .option("hoodie.clean.automatic", "false") \
            .mode("append") \
            .save(LOG_TABLE_PATH)
        #################################################################    
        logger.info(f"Logged pipeline execution: status={status}, records={records_processed}")
    except Exception as e:
        logger.error(f"Error writing to log table: {str(e)}")
        raise

def extract_source_data(spark, last_run_timestamp=None):
    """
    Extract data from source tables - CUSTOMIZE THIS FUNCTION
    This is a template - replace with your actual data extraction logic
    """
    # Example query - replace with your actual source tables and logic
    base_query = """
    SELECT 
      uuid() as record_id,
      source.entity_id,
      source.event_timestamp,
      source.event_type,
      source.data_field_1,
      source.data_field_2,
      source.numeric_field_1,
      source.numeric_field_2
    FROM your_source_database.source_table source
    WHERE source.event_timestamp IS NOT NULL
    """    
    # Add incremental processing logic
    if last_run_timestamp:
        base_query += f"""
        AND source.created_at > {last_run_timestamp}
        """
    #################################################################
    base_query += " ORDER BY source.entity_id, source.event_timestamp"    
    raw_df = spark.sql(base_query)    
    # Apply any necessary transformations
    # Example: Convert timestamp columns if needed
    # converted_df = raw_df.withColumn("event_timestamp", convert_ts_to_timestamp("event_timestamp_raw"))    
    return raw_df

def process_data(df):
    """Add processing metadata and any business logic transformations"""
    return df.withColumn("processing_date", current_date()) \
            .withColumn("load_timestamp", current_timestamp())

def write_hudi_table(df, table_name, hdfs_path, primary_key, precombine_field):
    """Write DataFrame to Hudi table"""
    try:
        df.write \
            .format("hudi") \
            .option("hoodie.table.name", table_name) \
            .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
            .option("hoodie.datasource.write.operation", "upsert") \
            .option("hoodie.datasource.write.recordkey.field", primary_key) \
            .option("hoodie.datasource.write.precombine.field", precombine_field) \
            .option("hoodie.datasource.hive_sync.enable", "true") \
            .option("hoodie.datasource.hive_sync.database", DATABASE_NAME) \
            .option("hoodie.datasource.hive_sync.table", table_name) \
            .option("hoodie.datasource.hive_sync.mode", "hms") \
            .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator") \
            .option("hoodie.clean.automatic", "true") \
            .option("hoodie.clean.async", "false") \
            .mode("append") \
            .save(hdfs_path)
        logger.info(f"Successfully wrote to Hudi table {table_name}")
    except Exception as e:
        logger.error(f"Error writing to Hudi table {table_name}: {str(e)}")
        raise

def run_pipeline(spark=None):
    """Main pipeline execution with logging and error handling"""
    if spark is None:
        spark = get_spark_session()
        cleanup_spark = True
    else:
        cleanup_spark = False
    #################################################################
    start_time = time.time()
    current_run_timestamp = int(time.time() * 1000)
    records_processed = 0
    #################################################################
    try:
        logger.info(f"Starting pipeline run at {datetime.now()}")
        create_database_and_table(spark)        
        # Get last run timestamp for incremental processing
        last_run_timestamp = get_last_run_timestamp(spark)
        mode = "incremental" if last_run_timestamp else "full"
        logger.info(f"Running in {mode} mode")        
        # Extract and process data
        df = extract_source_data(spark, last_run_timestamp)
        records_processed = df.count()
        logger.info(f"Found {records_processed} records to process")        
        if records_processed == 0:
            logger.info("No new data to process")
            processing_time = time.time() - start_time
            create_log_entry(spark, last_run_timestamp, current_run_timestamp, "SUCCESS", 0, processing_time)
            #################################################################
            if cleanup_spark:
                logger.info("Cleaning up Spark session")
                spark.stop()
            return 0        
        # Process data (add business logic, metadata, etc.)
        processed_df = process_data(df)        
        # Write to Hudi table
        write_hudi_table(
            processed_df,
            "processed_data",
            TABLE_PATH,
            "record_id",  # Customize primary key
            "load_timestamp"
        )        
        # Log successful completion
        processing_time = time.time() - start_time
        create_log_entry(
            spark,
            last_run_timestamp,
            current_run_timestamp,
            "SUCCESS",
            records_processed,
            processing_time
        )
        #################################################################
        logger.info(f"Pipeline completed successfully! Processed {records_processed} records in {processing_time:.2f} seconds")
        return records_processed
    #################################################################    
    except Exception as e:
        processing_time = time.time() - start_time
        create_log_entry(
            spark,
            last_run_timestamp,
            current_run_timestamp,
            "FAILED",
            records_processed,
            processing_time,
            str(e)
        )
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    #################################################################
    finally:
        if cleanup_spark:
            logger.info("Cleaning up Spark session")
            spark.stop()

def get_pipeline_status(spark=None, days=7):
    """Get recent pipeline run status"""
    if spark is None:
        spark = get_spark_session()
        cleanup_spark = True
    else:
        cleanup_spark = False
    #################################################################
    try:
        cutoff_time = int((time.time() - (days * 24 * 3600)) * 1000)
        result = spark.sql(f"""
            SELECT 
                pipeline_name,
                FROM_UNIXTIME(last_run_timestamp/1000) as last_run_time,
                FROM_UNIXTIME(current_run_timestamp/1000) as current_run_time,
                status,
                records_processed,
                processing_time_seconds,
                error_message,
                FROM_UNIXTIME(created_at/1000) as created_time
            FROM metadata_layer.pipeline_log
            WHERE pipeline_name = '{PIPELINE_NAME}'
            AND current_run_timestamp >= {cutoff_time}
            ORDER BY current_run_timestamp DESC
        """)
        result.show(truncate=False)
    except Exception as e:
        logger.error(f"Error getting pipeline status: {str(e)}")
        raise
    finally:
        if cleanup_spark:
            logger.info("Cleaning up Spark session")
            spark.stop()

def daily_pipeline_run():
    """Simple function for scheduled execution"""
    try:
        records = run_pipeline()
        logger.info(f"Daily pipeline completed successfully. Processed {records} records.")
        return True
    except Exception as e:
        logger.error(f"Daily pipeline failed: {str(e)}")
        return False

# Usage examples:
if __name__ == "__main__":
    # Option 1: Run with automatic Spark session management
    daily_pipeline_run()
    
    # Option 2: Run with existing Spark session
    # spark = get_spark_session()
    # try:
    #     records = run_pipeline(spark)
    #     get_pipeline_status(spark)
    # finally:
    #     logger.info("Cleaning up Spark session")
    #     spark.stop()
    
    # Option 3: Check pipeline status only
    # get_pipeline_status(days=14)

def create_sample_data_for_testing(spark):
    """
    Create sample data for testing the pipeline
    Remove this function in production
    """
    sample_data = [
        ("1", "user_123", datetime.now(), "login", "web", "chrome", 1.0, 100),
        ("2", "user_456", datetime.now(), "purchase", "mobile", "app", 25.99, 200),
        ("3", "user_789", datetime.now(), "view", "web", "firefox", 0.0, 150),
    ]
    #################################################################
    schema = StructType([
        StructField("record_id", StringType(), False),
        StructField("entity_id", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("event_type", StringType(), False),
        StructField("data_field_1", StringType(), True),
        StructField("data_field_2", StringType(), True),
        StructField("numeric_field_1", DoubleType(), True),
        StructField("numeric_field_2", IntegerType(), True)
    ])
    #################################################################
    return spark.createDataFrame(sample_data, schema)
#################################################################
# Configuration template for different environments
ENVIRONMENT_CONFIGS = {
    "development": {
        "database_name": "dev_analytics_layer",
        "pipeline_name": "dev_data_processing_pipeline",
        "spark_configs": {
            "spark.sql.shuffle.partitions": "10",
            "spark.default.parallelism": "10"
        }
    },
    "staging": {
        "database_name": "staging_analytics_layer",
        "pipeline_name": "staging_data_processing_pipeline",
        "spark_configs": {
            "spark.sql.shuffle.partitions": "50",
            "spark.default.parallelism": "50"
        }
    },
    "production": {
        "database_name": "prod_analytics_layer",
        "pipeline_name": "prod_data_processing_pipeline",
        "spark_configs": {
            "spark.sql.shuffle.partitions": "200",
            "spark.default.parallelism": "200"
        }
    }
}