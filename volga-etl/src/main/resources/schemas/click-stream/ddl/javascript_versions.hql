USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

CREATE TABLE `javascript_versions`
(
  javascript_version_id INT,
  javascript_version_name STRING
)
ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
STORED AS TEXTFILE;