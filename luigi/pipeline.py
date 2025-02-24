import luigi
import logging
import os
import shutil
import time
import duckdb

from luigi.contrib.spark import SparkSubmitTask

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('luigi-interface')

class InputFileTask(luigi.ExternalTask):
    path = luigi.Parameter()
    
    def output(self):
        return luigi.LocalTarget(self.path)

class SparkJobTask(SparkSubmitTask):

    taxi_data = '../data/yellow_tripdata_2024-10.parquet'
    zone_data = '../data/taxi_zone_lookup.csv'
    weather_data = '../data/72505394728.csv'
    success_flag = '../output/_OKLUIGI'
    output_dir = '../output'
    job = '../spark/spark-job.py'

    def requires(self):
        input_files = [
                self.taxi_data,
                self.zone_data,
                self.weather_data
        ]
        return [InputFileTask(path=f) for f in input_files]

    def output(self):
        return luigi.LocalTarget(self.success_flag)

    def complete(self):
        """Force Luigi to check for _OKLUIGI instead of assuming the task is incomplete."""
        if os.path.exists(self.success_flag):
            return True
        else:
            return False

    def remove_output(self):
        """Ensure output directory is removed BEFORE execution"""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

    def app_options(self):
        """Pass the required command-line arguments to Spark job."""
        return [
                self.taxi_data,
                self.zone_data,
                self.weather_data,
                self.output_dir
        ]

    def run(self):
        """Ensure output is deleted before running the Spark job and create _OKLUIGI after."""
        self.remove_output()

        # Run Spark job
        cmd = [
            "/opt/spark/bin/spark-submit",
            "--master", "local[*]",
            "--deploy-mode", "client",
            "--name", '"NYC Taxi/Weather Analysis"',
            "--conf", "spark.sql.shuffle.partitions=2",
            self.job,
            self.taxi_data,
            self.zone_data,
            self.weather_data,
            self.output_dir
        ]

        os.system(" ".join(cmd))

        with open(self.success_flag, "w") as f:
            f.write("Spark Job Completed")

        if os.path.exists(self.success_flag):
            logger.info(f"✅ _OKLUIGI successfully created:{ self.success_flag}")
        else:
            logger.error(f"❌ Failed to create _OKLUIGI:{ self.success_flag}")


class LoadDuckDBTask(luigi.Task):
    """Loads the Spark output into DuckDB"""

    loadpath = luigi.Parameter(default="../output/*.parquet")
    queries = '../duckdb/queries.sql'
    duckdb = '../duckdb/final.db'
    spark_success = '../output/_OKLUIGI'
    duckdb_success = '../output/duckdb_loaded.flag'

    def requires(self):
        """Ensure Spark processing completes first"""
        return SparkJobTask()

    def output(self):
        """Check for a flag file to track successful execution"""
        return luigi.LocalTarget(self.duckdb_success)

    def run(self):
        """Ensure Spark job completed by checking for _OKLUIGI before executing SQL."""

        spark_success_file = os.path.abspath(self.spark_success)  # Convert to absolute path
        sql_file = os.path.abspath(self.queries)  # Convert to absolute path
        db_path = os.path.abspath(self.duckdb)  # Convert to absolute path

        # Debug log: Confirm execution
        logger.info(f"🔍 Checking for {self.spark_success} before running DuckDB job...")

        # Ensure the file system sees _OKLUIGI before proceeding
        time.sleep(5)  # Extra delay to prevent file system sync issues

        # Check for _OKLUIGI explicitly
        if not os.path.exists(spark_success_file):
            raise RuntimeError(f"Expected file not found: {self.spark_success}")

        # Read and replace $LOADPATH in SQL before execution
        with open(sql_file, "r") as f:
            sql_script = f.read().replace("$LOADPATH", self.loadpath)

        # Connect to DuckDB and execute the script
        con = duckdb.connect(database=db_path, read_only=False)
        con.execute(sql_script)
        con.close()

        logger.info("✅ DuckDB data load completed successfully!")

        # Create a flag file to indicate successful execution
        with open(self.output().path, "w") as f:
            f.write("DuckDB Load Successful")

if __name__ == "__main__":
    logger.info("Starting example pipeline...")

    luigi.build(
        [LoadDuckDBTask()],  # The final task that triggers everything
        local_scheduler=False,  # Run without a central Luigi scheduler
        workers=1,  # Number of concurrent workers
        no_lock=True  # Avoid lock issues
    )

    logger.info("Pipeline execution completed successfully.")
