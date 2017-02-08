package com.rackspace.volga.etl.stacktach.mapreduce;

import com.jayway.jsonpath.JsonPath;
import com.rackspace.volga.etl.common.data.ETLConstants;
import com.rackspace.volga.etl.mapreduce.mr.GenericETLDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

/**
 * User: alex.silva
 * Date: 4/3/15
 * Time: 3:13 PM
 * Copyright Rackspace Hosting, Inc.
 * <p/>
 * This is a convenience driver implementation that uses @see com.rackspace.volga.etl.common.transform
 * .JSONPathTransformer as the input transformer for the job.
 * <p/>
 * This is equivalent to setting the "volga.etl.input.transformer.class" parameter to "com.rackspace.volga.etl
 * .common.transform.JSONPathTransformer"
 * <p/>
 * <p/>
 * The StackTach JSON format is substantially more complex than MAAS.  The JSONPathTransformer does not translate the
 * JSON string into a Java object; instead it creates a JsonPath object that can be manipulated using JSON path
 * expressions.
 * <p/>
 * For instance:
 * <ul>
 * <li>"$.store.book[0].author"</li>
 * <li>"$.store.book[?(@.price < 10)]"</li>
 * <li>payload.request_spec.name</li>
 * </ul>
 * <p/>
 * This implementation can be coupled with a external file that defines the paths to be extracted from the underlying
 * StackTach JSON event.
 */
public class StackTachJSONPathDriver extends GenericETLDriver {

    @Override
    protected void setUpJob(Job job) {
        Configuration conf = job.getConfiguration();
        conf.set(ETLConstants.DTO_CLASS_KEY, JsonPath.class.getCanonicalName());
        super.setUpJob(job);
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new StackTachJSONPathDriver(), args));
    }

}
