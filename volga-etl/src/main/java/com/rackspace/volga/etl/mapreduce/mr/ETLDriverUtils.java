package com.rackspace.volga.etl.mapreduce.mr;

/**
 * User: alex.silva
 * Date: 4/3/14
 * Time: 2:25 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class ETLDriverUtils {


    public static final int ONE = 1;

    public static enum Counters {ERRORS, SKIPPED_NOT_PARSEABLE, VALID}

    public static String VALID_COUNTER_GROUP = "DAILY_VALID_RECORDS";

    public static String INVALID_COUNTER_GROUP = "DAILY_INVALID_RECORDS";


    private ETLDriverUtils() {
    }
}
