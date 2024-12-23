deploy:
	@docker build -t spark_master .
	@docker compose up -d
	@docker exec spark-master spark-submit \
	--class EventProcessor \
	--master spark://spark-master:7077 \
	--conf spark.executor.cores=1 \
	--conf spark.driver.cores=1 \
	--conf spark.executor.instances=1 \
	--conf spark.cores.max=1 \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.4 \
	./apps/eventprocessor_2.12-1.0.jar
up:
	@docker compose up -d
consume-init:
	@docker exec -it kafka /bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic init_events --from-beginning
consume-match:
	@docker exec -it kafka /bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic match_events --from-beginning
consume-iap:
	@docker exec -it kafka /bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic iap_events --from-beginning	
down:
	@docker compose down
submit:
	@docker exec spark-master spark-submit \
	--class EventProcessor \
	--master spark://spark-master:7077 \
	--conf spark.executor.cores=1 \
	--conf spark.driver.cores=1 \
	--conf spark.executor.instances=1 \
	--conf spark.cores.max=1 \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.4 \
	./apps/eventprocessor_2.12-1.0.jar
init_events:
	@docker exec -it postgres psql -U user -d game-events -c "SELECT * FROM init_events LIMIT 10;"
match_events:
	@docker exec -it postgres psql -U user -d game-events -c "SELECT * FROM match_events LIMIT 10;"
iap_events:
	@docker exec -it postgres psql -U user -d game-events -c "SELECT * FROM iap_events LIMIT 10;"
total_events:
	@docker exec -it postgres psql -U user -d game-events -c "SELECT init_events, match_events, iap_events, (init_events + match_events + iap_events) as total_events FROM (SELECT (SELECT COUNT(*) FROM init_events) AS init_events, (SELECT COUNT(*) FROM match_events) AS match_events, (SELECT COUNT(*) FROM iap_events) AS iap_events) a;"
daily_users:
	@docker exec -it postgres psql -U user -d game-events -c "SELECT country, platform, COUNT(DISTINCT user_id) AS unique_users FROM init_events WHERE (event_date = '$(DAY)') GROUP BY country, platform ORDER BY unique_users DESC;"
daily_users_spark:
	@docker exec spark-master spark-submit \
	--class Aggregator \
	--master spark://spark-master:7077 \
	--conf spark.executor.cores=1 \
	--conf spark.driver.cores=1 \
	--conf spark.executor.instances=1 \
	--conf spark.cores.max=1 \
	--packages org.postgresql:postgresql:42.7.4 \
	./apps/eventprocessor_2.12-1.0.jar