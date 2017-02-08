USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `events`
(
  event_id INT,
  event_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;