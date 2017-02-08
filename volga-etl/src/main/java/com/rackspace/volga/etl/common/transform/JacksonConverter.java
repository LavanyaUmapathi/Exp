package com.rackspace.volga.etl.common.transform;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * User: alex.silva
 * Date: 4/14/14
 * Time: 2:59 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class JacksonConverter<T> extends BaseConverter<T, String> {
    private static final ObjectMapper mapper = new ObjectMapper();

    private Class<T> prototypeB;

    public JacksonConverter(Class<T> prototypeB) {
        super(prototypeB, String.class);
        this.prototypeB = prototypeB;
    }


    @Override
    public T convertFrom(String source, Object destination) {
        try {
            T obj = destination == null ? mapper.readValue(source, prototypeB) :
                    (T) mapper.readerForUpdating(destination).readValue(source);
            return obj;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String convertTo(Object source, String destination) {
        try {
            return mapper.writer().writeValueAsString(source);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
