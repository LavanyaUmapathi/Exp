USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `color_depths`
(
  color_depth_id INT,
  color_depth_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;