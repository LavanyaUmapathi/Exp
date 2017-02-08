USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `connection_types`
(
  connection_type_id INT,
  connection_type_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;