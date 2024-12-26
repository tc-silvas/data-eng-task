import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Locale
import java.util.Properties

object Aggregator {
  def main(args: Array[String]): Unit = {
    // Ensure correct number of arguments are passed
    if (args.length != 2) {
      println("Usage: UniqueUsersAggregator <start_date> <end_date>")
      sys.exit(1)
    }

    // Parse input arguments
    val start_date = args(0)
    val end_date = args(1)

    // Define input and output paths for Parquet files
    val inputPath = "/opt/spark/events/init/data"
    val outputPath = "/opt/spark/events/aggregated/daily_users"

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

    // User Defined Function (UDF) to map country_id to country_name using Locale
    val mapCountryIdToName = udf { (countryId: String) =>
      try {
        val locale = new Locale.Builder().setRegion(countryId).build()
        Option(locale.getDisplayCountry).filter(_.nonEmpty).getOrElse("Unknown")
      } catch {
        case _: Exception => "Unknown"
      }
    }

    // Read partitioned Parquet files and filter by event_date range
    val initEventsDF = spark.read
      .parquet(inputPath)
      .where(col("event_date") >= start_date && col("event_date") <= end_date)

    // Apply transformations to the DataFrame
    val transformedDF = initEventsDF.select(
      col("user_id"),
      mapCountryIdToName(col("country")).as("country_name"),
      upper(col("platform")).as("platform"),
      col("event_date")
    )

    // Aggregate unique users by event_date, country_name, and platform
    val aggregatedDF = transformedDF
      .groupBy("event_date", "country_name", "platform")
      .agg(countDistinct("user_id").alias("unique_users"))

    // Add is_mobile flag based on platform
    val finalDF = aggregatedDF
      .withColumn("is_mobile", when(col("platform").isin("IOS", "ANDROID"), true).otherwise(false))

    // Write the aggregated results to Parquet files, partitioned by event_date
    finalDF.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println(s"Aggregated data saved to: $outputPath")

    // Write the aggregated results to PostgreSQL table 'unique_users'
    finalDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "unique_users", connectionProperties)

    println("Aggregated data saved to PostgreSQL table 'unique_users'")

    // Stop the Spark session
    spark.stop()
  }
}
