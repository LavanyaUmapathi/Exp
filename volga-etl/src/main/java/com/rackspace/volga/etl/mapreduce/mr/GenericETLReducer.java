package com.rackspace.volga.etl.mapreduce.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class GenericETLReducer extends Reducer<Text, Text, NullWritable, Text> {

    MultipleOutputs outputs;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
        super.setup(context);
    }

    @Override
    public void reduce(Text key, Iterable<Text> jsonRows, Context context) throws IOException,
            InterruptedException {
        for (Text json : jsonRows) {
            //we don't care about the key. The mapper just outputed the key so that their outputs are properly shuffled.
            // context.write(NullWritable.get(), event);
            String rkey = key.toString().split("-")[1];
            outputs.write(NullWritable.get(), json, rkey);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (outputs != null) {
            outputs.close();
        }
        super.cleanup(context);
    }
}
