# flink-sql-submit
used to submit flink sql

```
/data/app/sailfish-interface-machine/platform_flink/flink_lib/flink-huya-1.14/bin/flink run -d -p 1 -m 10.219.57.87:8081  \
-C file:/data/app/sailfish-interface-machine/platform_flink/realtime_platform/job/sql/lib/flink-avro-huya-1.12-SNAPSHOT.jar \
-C file:/data/app/sailfish-interface-machine/platform_flink/realtime_platform/job/sql/lib/pulsar-flink-connector-2.12-1.12-huya-2.7.6-SNAPSHOT.jar \
-C file:/home/jinyaqia/blink-test/flink-connector-kafka_2.12-1.14.2.jar \
-C file:/home/jinyaqia/blink-test/flink-text-0.1.0-SNAPSHOT.jar \
-C file:/home/jinyaqia/blink-test/flink-udf-1.0-SNAPSHOT.jar \
/home/jinyaqia/blink-test/flink-sql-submit-0.1.0-SNAPSHOT.jar
-name test_sql
-f /home/jinyaqia/blink-test/114kafka.sql -explain
```
