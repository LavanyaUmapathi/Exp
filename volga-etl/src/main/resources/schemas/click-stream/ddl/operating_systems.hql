USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `operating_systems`
(
  operating_system_id INT,
  operating_system_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;