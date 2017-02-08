USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `search_engines`
(
  search_engine_id INT,
  search_engine_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;