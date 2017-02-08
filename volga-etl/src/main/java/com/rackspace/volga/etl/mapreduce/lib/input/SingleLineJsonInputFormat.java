package com.rackspace.volga.etl.mapreduce.lib.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Assumes one line per JSON object
 */
public class SingleLineJsonInputFormat extends FileInputFormat<LongWritable, MapWritable> {


    @Override
    public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new JsonRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration())
                .getCodec(file);
        return codec == null;
    }

    public static class JsonRecordReader extends RecordReader<LongWritable, MapWritable> {
        private static final Logger LOG = LogManager.getLogger(JsonRecordReader.class);

        private LineRecordReader reader = new LineRecordReader();

        private final MapWritable value = new MapWritable();

        private final JSONParser jParser = new JSONParser();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            reader.initialize(split, context);
        }

        @Override
        public synchronized void close() throws IOException {
            reader.close();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return reader.getCurrentKey();
        }

        @Override
        public MapWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return reader.getProgress();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (reader.nextKeyValue()) {
                value.clear();
                if (decodeLineToJson(jParser, reader.getCurrentValue(), value)) {
                    return true;
                }
            }
            return false;
        }

        boolean decodeLineToJson(JSONParser parser, Text line, MapWritable value) {
            try {
                JSONObject jsonObj = (JSONObject) parser.parse(line.toString());
                for (Object key : jsonObj.keySet()) {
                    Text mapKey = new Text(key.toString());
                    Text mapValue = new Text();
                    if (jsonObj.get(key) != null) {
                        mapValue.set(jsonObj.get(key).toString());
                    }

                    value.put(mapKey, mapValue);
                }
                return true;
            } catch (ParseException e) {
                LOG.warn("Could not json decode string: " + line, e);
                return false;
            } catch (NumberFormatException e) {
                LOG.warn("Could not parse field into number: " + line, e);
                return false;
            }
        }
    }
}
