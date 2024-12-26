# Data Engineering Task

![Pipeline](https://github.com/user-attachments/assets/2eca4273-3c04-4190-876e-0eb893ec0d6e)

## Setup the Environment

### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/tc-silvas/data-eng-task.git
```

**Note:** Ensure that Docker is installed on your machine.

### 2. Set Up the Environment

Navigate to the project directory and bring up the environment by running:

```bash
make up
```

This command will start the following services:
- Zookeeper
- Kafka
- Spark
- PostgreSQL
- Kafka Producer

### 3. Verify the Producer

After setting up the environment, check if the Producer is generating data by running the following commands:

- To consume **init events**:
  ```bash
  make consume-init
  ```

- To consume **match events**:
  ```bash
  make consume-match
  ```

- To consume **in-app purchase (IAP) events**:
  ```bash
  make consume-iap
  ```

### 4. Confirm Spark Cluster

Once the above commands are executed, visit [localhost:8080](http://localhost:8080) to verify that the Spark cluster is running. If the three commands are producing data, the environment setup is complete.

---

## Task

Now let's process the data using Spark.

### 1. Submit the Spark Job

To process the data from Kafka to Parquet and PostgreSQL sinks, submit the Spark job by running:

```bash
make submit
```

You can press `CTRL + C` to stop the script once it's running. The script will continue executing in the Spark master, and you can monitor its progress.

### 2. Verify EventProcessor

Visit [localhost:8080](http://localhost:8080) to verify that the EventProcessor is running. You should see something similar to the screenshot below:

![EventProcessor Screenshot](https://github.com/user-attachments/assets/b72f08f8-3c10-44fb-aef3-109d5d5893e9)

### 3. Verify Parquet Data

Check if the data is being stored correctly in Parquet format. Run the following commands to ensure that the folders are partitioned by day:

```bash
ls events/iap/data
ls events/match/data
ls events/init/data
```

### 4. Verify PostgreSQL Data

To check if data is being saved to PostgreSQL, run the following commands to produce events:

```bash
make init_events
make match_events
make iap_events
```

### 5. Check Total Events Produced

To see how many events have been produced, run:

```bash
make total_events
```

---

### Data Transformations and Quality Improvements

We will now perform data transformations, aggregations, and quality improvements.

### 1. Get the List of Unique Users

To get the list of unique users for a specified date range, run:

```bash
make unique_users START_DATE=<start_date> END_DATE=<end_date>
```

Example:

```bash
make unique_users START_DATE=2024-12-01 END_DATE=2024-12-24
```

You can run this command multiple times for different date ranges. Press `CTRL + C` to stop the script once it's running. It will show as "Running" in the Spark master until completion.

### 2. Verify Aggregation

Check if the data is aggregated correctly by verifying the `events/aggregated/daily_users` directory. If the directory contains folders partitioned by day, the script has run successfully. Run:

```bash
ls events/aggregated/daily_users
```

### 3. Query Daily Unique Users

To query the PostgreSQL table for daily unique users on a specific day, run:

```bash
make daily_users DAY=<desired_day>
```

Example:

```bash
make daily_users DAY=2024-12-23
```

### 4. Apply Data Transformations

To apply data transformations and quality improvements for a specific date range, run:

```bash
make transformations START_DATE=<start_date> END_DATE=<end_date>
```

Example:

```bash
make transformations START_DATE=2024-12-01 END_DATE=2024-12-24
```

Press `CTRL + C` to stop the script once it's running. It will show as "Running" in the Spark master until completion.

The following transformations will be applied:

| Event Type | Column        | Transformation                                       |
|------------|---------------|------------------------------------------------------|
| **Init**   | `country`     | Map `country_id` to `country_name` using `Locale`    |
|            | `platform`    | Convert to uppercase                                 |
|            | Derived Field | Add `is_mobile` flag                                 |
| **Match**  | `game-tier`   | Derive `is_tournament` flag (if `game-tier` is 5)    |
|            | `platform`    | Convert to uppercase                                 |
|            | `device`      | Convert to uppercase                                 |
| **IAP**    | Derived Field | Add `purchase_category` and `is_high_value` flag     |

### 5. Verify Transformed Data

To verify the transformed data, run:

```bash
make match_transformed
make iap_transformed
```

### 6. Query Other Tables

You can run additional queries on the following PostgreSQL tables if desired:

| Schema | Name             | Type  | Owner  |
|--------|------------------|-------|--------|
| public | iap_events       | table | user   |
| public | iap_transformed  | table | user   |
| public | init_events      | table | user   |
| public | match_events     | table | user   |
| public | match_transformed| table | user   |
| public | unique_users     | table | user   |

Run the following to connect to PostgreSQL and execute queries:

```bash
make postgres
```

Then, execute your desired SQL query. For example:

```sql
SELECT * FROM iap_events WHERE event_date = '2024-12-23';
```

### 7. Stopping Services

Once you're done, stop the services by running:

```bash
make down
```
