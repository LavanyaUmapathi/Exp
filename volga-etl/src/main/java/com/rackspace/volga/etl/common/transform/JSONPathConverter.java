package com.rackspace.volga.etl.common.transform;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

/**
 * User: alex.silva
 * Date: 4/1/15
 * Time: 4:20 PM
 * Copyright Rackspace Hosting, Inc.
 * <p/>
 * <p/>
 * Returns a JsonPath implementation of the string passed in.
 */
public class JSONPathConverter extends BaseConverter<DocumentContext, String> {

    public JSONPathConverter() {
        super(DocumentContext.class, String.class);
    }

    @Override
    public DocumentContext convertFrom(String source, DocumentContext destination) {
        Object o = JsonPath.parse(source);
        return (DocumentContext) o;
    }

    @Override
    public String convertTo(DocumentContext source, String destination) {
        return source.jsonString();
    }
}
