# Data Engineering Task

![Pipeline](https://github.com/user-attachments/assets/2eca4273-3c04-4190-876e-0eb893ec0d6e)

## Setup the Environment

1. Clone the repository:

   ```bash
   git clone https://github.com/tc-silvas/data-eng-task.git
   ```

2. Navigate to the project directory and bring up the environment by running:

   ```bash
   make up
   ```

   This will set up the following services:
   - Zookeeper
   - Kafka
   - Spark
   - PostgreSQL
   - Kafka Producer

3. After the environment is set up, you can check if the Producer is generating data by running the following commands:

   - To initialize events:
     ```bash
     make init_events
     ```
   - To produce match events:
     ```bash
     make match_events
     ```
   - To generate in-app purchase (IAP) events:
     ```bash
     make iap_event
     ```

4. Once these commands are executed, visit [localhost:8080](http://localhost:8080) to confirm that the Spark cluster is running.

   If all three commands are producing data, the environment setup is complete and successful.

---

## Task

Now, let's run the Spark scripts to process the data. Follow these steps:

1. **Submit the Spark job** to process data from Kafka to the Parquet and PostgreSQL sinks:

   ```bash
   make submit
   ```

2. Go to [localhost:8080](http://localhost:8080) and verify that the EventProcessor is running. You should see something similar to the following screenshot:
   
   ![EventProcessor Screenshot](https://github.com/user-attachments/assets/b72f08f8-3c10-44fb-aef3-109d5d5893e9)

3. Verify that the data is being stored correctly in Parquet format. Run the following commands to check if folders are partitioned by day:

   ```bash
   ls events/iap/data
   ls events/match/data
   ls events/init/data
   ```

4. After confirming the Parquet data, check if the data is being saved to the PostgreSQL data warehouse. Run the following commands:

   ```bash
   make init_events
   make match_events
   make iap_events
   ```

5. To check how many events have been produced, run:

   ```bash
   make total_events
   ```

---

### Data Transformations and Quality Improvements

Now, we will perform some data transformations, aggregations, and quality improvements.

1. **Get the list of unique users** for the desired date range by running:

   ```bash
   make unique_users START_DATE=<start_date> END_DATE=<end_date>
   ```

   Example:

   ```bash
   make unique_users START_DATE=2024-12-01 END_DATE=2024-12-24
   ```

   You can run this command multiple times for different date ranges.

2. To verify that the data is aggregated correctly, check the `events/aggregated/daily_users` directory. If the command returns folders partitioned by day, the aggregation is correct:

   ```bash
   ls events/aggregated/daily_users
   ```

3. To query the PostgreSQL table created for daily unique users on a specific day, run:

   ```bash
   make daily_users DAY=<desired_day>
   ```

   Example:

   ```bash
   make daily_users DAY=2024-12-23
   ```

4. For data quality improvements, run:

   ```bash
   make transformations
   ```

   This script applies the following transformations:

   | Event Type | Column        | Transformation                                       |
   |------------|---------------|------------------------------------------------------|
   | **Init**   | `country`     | Map `country_id` to `country_name` using `Locale`    |
   |            | `platform`    | Convert to uppercase                                 |
   |            | Derived Field | Add `is_mobile` flag                                 |
   | **Match**  | `game-tier`   | Derive `is_tournament` flag (if `game-tier` is 5)    |
   |            | `platform`    | Convert to uppercase                                 |
   |            | `device`      | Convert to uppercase                                 |
   | **IAP**    | Derived Field | Add `purchase_category` and `is_high_value` flag     |

5. To verify the transformed data, run:

   ```bash
   make match_transformed
   make iap_transformed
   ```

---

With these steps completed, you will have processed, transformed, and improved the data quality of the events.

--- 
