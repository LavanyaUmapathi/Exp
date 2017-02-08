package com.rackspace.volga.etl.mapreduce.mr;

import com.rackspace.volga.etl.common.data.ETLConstants;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.data.Timestampable;
import com.rackspace.volga.etl.common.transform.DozerTransformer;
import com.rackspace.volga.etl.common.transform.Transformer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 4/2/14
 * Time: 5:02 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class GenericETLMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger LOGGER = LogManager.getLogger(GenericETLMapper.class);

    private static final Logger ETL_BAD_RECORDS_LOGGER = LogManager.getLogger("ETLBadDataLogger");

    private Transformer inputTransformer;

    private Transformer<Object, MappedRows> outputTransformer;

    private String inputMappingId;

    private String outputMappingId;

    private DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");

    private Class<?> dtoClass;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        dtoClass = context.getConfiguration().getClass(ETLConstants.DTO_CLASS_KEY, null);
        Class<? extends Transformer> inputTransfClass = context.getConfiguration().getClass(ETLConstants
                .INPUT_TRANSFORMER_CLASS_KEY, DozerTransformer.class, Transformer.class);
        inputTransformer = ReflectionUtils.newInstance(inputTransfClass, context.getConfiguration());
        Class<? extends Transformer> outputTransfClass = context.getConfiguration().getClass(ETLConstants
                .OUTPUT_TRANSFOMER_CLASS_KEY, DozerTransformer.class, Transformer.class);
        outputTransformer = ReflectionUtils.newInstance(outputTransfClass, context.getConfiguration());
        inputMappingId = context.getConfiguration().get(ETLConstants.INPUT_MAPPING_ID);
        outputMappingId = context.getConfiguration().get(ETLConstants.OUTPUT_MAPPING_ID);
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text json, Mapper.Context context) throws IOException, InterruptedException {
        String timestamp = null;
        Object obj;
        MappedRows mrows;
        try {

            obj = inputTransformer.transform(json.toString(), dtoClass, inputMappingId);
            mrows = outputTransformer.transform(obj, MappedRows.class, outputMappingId);

            Map<String, Collection<String>> rowsByKey = mrows.getRows();

            timestamp = getTimeStamp(obj);

            String keyts = timestamp != null ? timestamp : String.valueOf(Math.abs(obj.hashCode()));

            for (Map.Entry<String, Collection<String>> row : rowsByKey.entrySet()) {
                for (String rowValue : row.getValue()) {
                    context.write(new Text(keyts + "-" + row.getKey()), new Text(rowValue));
                }
            }
            context.getCounter(ETLDriverUtils.Counters.VALID).increment(ETLDriverUtils.ONE);


            if (timestamp != null) {
                context.getCounter(ETLDriverUtils.VALID_COUNTER_GROUP, new DateTime(Long.parseLong(timestamp.toString()), DateTimeZone.UTC).toString(formatter)).increment(ETLDriverUtils.ONE);
            }

        } catch (Exception e) {
            context.getCounter(ETLDriverUtils.Counters.ERRORS).increment(ETLDriverUtils.ONE);
            if (timestamp != null) {
                context.getCounter(ETLDriverUtils.INVALID_COUNTER_GROUP, new DateTime(Long.parseLong(timestamp.toString()), DateTimeZone.UTC).toString(formatter)).increment(1);
            }
            LOGGER.error(e);
            ETL_BAD_RECORDS_LOGGER.error(json);
        }
    }


    private String getTimeStamp(Object obj) {
        return obj instanceof Timestampable ? String.valueOf(((Timestampable) obj).getTimestamp()) : null;
    }

}
