import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Locale
import java.util.Properties

object Aggregator {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: UniqueUsersAggregator <start_date> <end_date>")
      sys.exit(1)
    }

    val start_date = args(0)
    val end_date = args(1)

    // Parquet paths
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

    // UDF to map country_id to country_name using Locale
    val mapCountryIdToName = udf { (countryId: String) =>
      try {
        val locale = new Locale.Builder().setRegion(countryId).build()
        Option(locale.getDisplayCountry).filter(_.nonEmpty).getOrElse("Unknown")
      } catch {
        case _: Exception => "Unknown"
      }
    }

    // Read partitioned Parquet files, then filter by event_date
    val initEventsDF = spark.read
      .parquet(inputPath)
      .where(col("event_date") >= start_date && col("event_date") <= end_date)

    // Apply transformations in a single select
    val transformedDF = initEventsDF.select(
      col("user_id"),
      mapCountryIdToName(col("country")).as("country_name"),
      upper(col("platform")).as("platform"),
      col("event_date")
    )

    // Aggregate unique users
    val aggregatedDF = transformedDF
      .groupBy("event_date", "country_name", "platform")
      .agg(countDistinct("user_id").alias("unique_users"))

    // Write results to Parquet, partitioned by event_date
    aggregatedDF.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println(s"Aggregated data saved to: $outputPath")

    // Write aggregated results to PostgreSQL
    aggregatedDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "unique_users", connectionProperties)

    println("Aggregated data saved to PostgreSQL table 'unique_users'")

    spark.stop()
  }
}
