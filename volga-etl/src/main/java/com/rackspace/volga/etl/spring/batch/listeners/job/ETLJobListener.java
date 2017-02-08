package com.rackspace.volga.etl.spring.batch.listeners.job;

import org.springframework.batch.core.listener.JobExecutionListenerSupport;

/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 9:39 AM
 * Copyright Rackspace Hosting, Inc.
 */
public abstract class ETLJobListener extends JobExecutionListenerSupport {

    public static final String MAP_INPUT_RECORDS_KEY = "Map-Reduce Framework::Map input records";

    public static final String MAP_OUTPUT_RECORDS_KEY = "Map-Reduce Framework::Map output records";

    public static final String INVALID_RECORDS_KEY = "com.rackspace.volga.etl.mapreduce.mr" +
            ".ETLDriverUtils$Counters::INVALID";

    public static final String VALID_RECORDS_KEY = "com.rackspace.volga.etl.mapreduce.mr" +
            ".ETLDriverUtils$Counters::VALID";


    public static final String INCIDENT_KEY_PREFIX = "com.rackspace.volga.etl.spring.batch.listeners.job" +
            ".ETLJobListener::INCIDENT::";

    public static final String REDUCE_OUTPUT_RECORDS_KEY = "Map-Reduce Framework::Reduce output records";

    public static final String REDUCE_INPUT_RECORDS_KEY = "Map-Reduce Framework::Reduce input records";
}
