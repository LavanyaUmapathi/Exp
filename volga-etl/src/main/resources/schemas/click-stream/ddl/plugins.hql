USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `plugins`
(
  plugin_id INT,
  plugin_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;