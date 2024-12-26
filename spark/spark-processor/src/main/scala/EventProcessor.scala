import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties

object EventProcessor {
  def main(args: Array[String]): Unit = {
    // Create a Spark Session
    val spark = SparkSession.builder
      .appName("EventProcessor")
      .getOrCreate()

    // Define Kafka server and topics
    val kafkaBootstrapServers = "kafka:9092"
    val topics = Map(
      "init_events"  -> "/opt/spark/events/raw/init/data",
      "match_events" -> "/opt/spark/events/raw/match/data",
      "iap_events"   -> "/opt/spark/events/raw/iap/data"
    )

    // Schema Definitions for different event types
    val initSchema = new StructType()
      .add("event_type", StringType)
      .add("time", LongType)
      .add("user_id", StringType)
      .add("country", StringType)
      .add("platform", StringType)

    val matchSchema = new StructType()
      .add("event_type", StringType)
      .add("time", LongType)
      .add("user_a", StringType)
      .add("user_b", StringType)
      .add("user_a_postmatch_info", new StructType()
        .add("coin_balance_after_match", IntegerType)
        .add("level_after_match", IntegerType)
        .add("device", StringType)
        .add("platform", StringType))
      .add("user_b_postmatch_info", new StructType()
        .add("coin_balance_after_match", IntegerType)
        .add("level_after_match", IntegerType)
        .add("device", StringType)
        .add("platform", StringType))
      .add("winner", StringType)
      .add("game_tier", IntegerType)
      .add("duration", IntegerType)

    val iapSchema = new StructType()
      .add("event_type", StringType)
      .add("time", LongType)
      .add("purchase_value", DoubleType)
      .add("user_id", StringType)
      .add("product_id", StringType)

    // Postgres connection properties
    val jdbcUrl = "jdbc:postgresql://postgres:5432/game-events"
    val jdbcProperties = new Properties()

    // NOT SECURE: Set username and password for PostgreSQL connection
    jdbcProperties.setProperty("user", "user")
    jdbcProperties.setProperty("password", "password")

    jdbcProperties.setProperty("driver", "org.postgresql.Driver")

    /**
     * Generic processing function for a topic.
     *
     * @param topic          Kafka topic name
     * @param schema         StructType for JSON parsing
     * @param outputPath     Path to write Parquet files
     * @param jdbcUrl        Postgres JDBC URL
     * @param jdbcProperties Postgres JDBC properties
     */
    def processTopic(
                      topic: String,
                      schema: StructType,
                      outputPath: String,
                      jdbcUrl: String,
                      jdbcProperties: java.util.Properties
                    ): Unit = {

      // Read data from Kafka
      val kafkaDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrapServers)
        .option("subscribe", topic)
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "earliest")
        .load()

      // Parse JSON and flatten columns if necessary
      var events = kafkaDF
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), schema).as("data"))
        .select("data.*")
        .withColumn("event_date", from_unixtime(col("time"), "yyyy-MM-dd"))

      // Flatten schema for match_events; postgreSQL does not accept StructType
      if (topic == "match_events") {
        events = events
          .withColumn("user_a_coin_balance_after_match", col("user_a_postmatch_info.coin_balance_after_match"))
          .withColumn("user_a_level_after_match", col("user_a_postmatch_info.level_after_match"))
          .withColumn("user_a_device", col("user_a_postmatch_info.device"))
          .withColumn("user_a_platform", col("user_a_postmatch_info.platform"))
          .withColumn("user_b_coin_balance_after_match", col("user_b_postmatch_info.coin_balance_after_match"))
          .withColumn("user_b_level_after_match", col("user_b_postmatch_info.level_after_match"))
          .withColumn("user_b_device", col("user_b_postmatch_info.device"))
          .withColumn("user_b_platform", col("user_b_postmatch_info.platform"))
          .drop("user_a_postmatch_info", "user_b_postmatch_info")
      }

      // Single write stream for both Parquet and JDBC
      events.writeStream
        .outputMode("append")
        // We schedule micro-batches every 10 seconds
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("checkpointLocation", s"$outputPath/../_checkpoint")
        .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
          // --- 1) Write to Parquet ---
          batchDF
            .write
            .mode("append")
            .partitionBy("event_date")
            .parquet(outputPath)

          // --- 2) Write to PostgreSQL ---
          batchDF
            .write
            .mode("append")
            .jdbc(jdbcUrl, topic, jdbcProperties)
        }
        .start()
    }

    // Start processing each topic in parallel
    processTopic("init_events", initSchema, topics("init_events"), jdbcUrl, jdbcProperties)
    processTopic("match_events", matchSchema, topics("match_events"), jdbcUrl, jdbcProperties)
    processTopic("iap_events", iapSchema, topics("iap_events"), jdbcUrl, jdbcProperties)

    // Await termination of any stream
    spark.streams.awaitAnyTermination()
  }
}