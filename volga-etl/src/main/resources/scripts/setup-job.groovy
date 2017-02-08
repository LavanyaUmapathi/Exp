package scripts

import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

if (fsh.test(outputDir)) {
   fsh.rmr(outputDir)
}

//set up lazy output for multiple outputs
cfg.setClass("mapreduce.outputformat.class", LazyOutputFormat.class, OutputFormat.class);
cfg.setClass(LazyOutputFormat.OUTPUT_FORMAT, TextOutputFormat.class, OutputFormat.class);
