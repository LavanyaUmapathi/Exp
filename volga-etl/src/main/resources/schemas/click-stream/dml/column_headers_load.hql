USE clickstream;

ADD JAR ${CSV_SERDE_JAR};

LOAD DATA INPATH ${INPUT_PATH} INTO TABLE column_headers;