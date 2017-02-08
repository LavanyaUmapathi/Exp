package com.rackspace.volga.etl.common.data;

/**
 * User: alex.silva
 * Date: 7/23/14
 * Time: 3:16 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class ETLConstants {


    private ETLConstants() {
    }

    public static final String INPUT_TRANSFORMER_CLASS_KEY = "volga.etl.input.transformer.class";

    public static final String OUTPUT_TRANSFOMER_CLASS_KEY = "volga.etl.output.transformer.class";

    public static final String EVENT_TYPE_KEY = "volga.etl.maas.event.type";

    public static final String RECORD_TYPE_KEY = "volga.etl.newrelic.record.type";

    public static final String DTO_CLASS_KEY = "volga.etl.dto.class";

    public static final String INPUT_MAPPING_ID = "volga.etl.input.mapping.id";

    public static final String OUTPUT_MAPPING_ID = "volga.etl.output.mapping.id";
}
