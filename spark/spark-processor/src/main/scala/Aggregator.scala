import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Properties

object Aggregator {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: UniqueUsersAggregator <start_date> <end_date>")
      sys.exit(1)
    }

    val start_date = args(0)
    val end_date = args(1)

    // Paths to Parquet files
    val inputPath = "/opt/spark/events/init/data"
    val outputPath = "/opt/spark/events/aggregated/daily_users"

    // Postgres connection properties
    val jdbcUrl = "jdbc:postgresql://postgres:5432/game-events"
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "user")
    connectionProperties.setProperty("password", "password")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("Aggregator")
      .getOrCreate()

    // Read partitioned Parquet files
    // Avoid mergeSchema if you know all files have the same schema
    val initEventsDF = spark.read
      // .option("mergeSchema", "true") // Remove or disable if not needed
      .parquet(inputPath)
      .select("user_id", "country", "platform", "event_date")
      .filter(col("event_date") >= start_date && col("event_date") < end_date)

    // Perform the aggregation (no need for .distinct() here)
    val aggregatedDF = initEventsDF
      .groupBy("event_date", "country", "platform")
      .agg(countDistinct("user_id").alias("unique_users"))

    // Save the aggregated data to Parquet files partitioned by event_date
    aggregatedDF.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println(s"Aggregated data saved to: $outputPath")

    // Save the aggregated results to Postgres
    // Optionally specify batch size or other JDBC options
    aggregatedDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "unique_users", connectionProperties)

    println("Aggregated data saved to Postgres SQL table 'unique_users'")

    spark.stop()
  }
}
