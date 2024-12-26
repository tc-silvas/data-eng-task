import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object DataTransformation {
  def main(args: Array[String]): Unit = {
    // Ensure correct number of arguments are passed
    if (args.length != 2) {
      println("Usage: DataTransformation <start_date> <end_date>")
      sys.exit(1)
    }

    // Parse input arguments
    val start_date = args(0)
    val end_date = args(1)

    // Define input and output paths for Parquet files
    val iapPath = "/opt/spark/events/raw/iap/data"
    val matchPath = "/opt/spark/events/raw/match/data"
    val iapOutputPath = "/opt/spark/events/transformations/iap"
    val matchOutputPath = "/opt/spark/events/transformations/match"

    // Postgres connection properties
    val jdbcUrl = "jdbc:postgresql://postgres:5432/game-events"
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "user")
    connectionProperties.setProperty("password", "password")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Aggregator")
      .getOrCreate()

    // Read partitioned Parquet files and filter by event_date range
    val initIapDF = spark.read
      .parquet(iapPath)
      .where(col("event_date") >= start_date && col("event_date") <= end_date)

    val initMatchDF = spark.read
      .parquet(matchPath)
      .where(col("event_date") >= start_date && col("event_date") <= end_date)

    // Transform IAP DataFrame by adding new columns
    val transformedIapDF = initIapDF
      .withColumn("is_high_value", col("purchase_value") > 100)
      .withColumn("purchase_category", when(col("purchase_value") < 10, "small")
      .when(col("purchase_value") < 100, "medium")
      .otherwise("large"))
      .drop("event_type", "time")

    // Transform Match DataFrame by adding new columns and transforming existing ones
    val transformedMatchDF = initMatchDF
      .withColumn("is_tournament", col("game_tier") === 5)
      .withColumn("user_a_platform", upper(col("user_a_platform")))
      .withColumn("user_b_platform", upper(col("user_b_platform")))
      .withColumn("user_a_device", upper(col("user_a_device")))
      .withColumn("user_b_device", upper(col("user_b_device")))
      .drop("event_type", "time")

    // Write transformed IAP data to Parquet, partitioned by event_date
    transformedIapDF.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(iapOutputPath)

    // Write transformed Match data to Parquet, partitioned by event_date
    transformedMatchDF.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(matchOutputPath)

    println(s"Aggregated data saved to: $iapOutputPath and $matchOutputPath")

    // Write transformed IAP data to PostgreSQL table 'iap_transformed'
    transformedIapDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "iap_transformed", connectionProperties)

    // Write transformed Match data to PostgreSQL table 'match_transformed'
    transformedMatchDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "match_transformed", connectionProperties)

    println("Aggregated data saved to PostgreSQL")

    // Write transformed Match data to PostgreSQL table 'match_transformed'
    spark.stop()
  }
}
