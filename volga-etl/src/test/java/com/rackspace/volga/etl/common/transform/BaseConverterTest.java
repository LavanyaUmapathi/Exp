package com.rackspace.volga.etl.common.transform;

import org.junit.Test;

/**
 * User: alex.silva
 * Date: 4/13/15
 * Time: 12:59 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class BaseConverterTest {


    @Test(expected = UnsupportedOperationException.class)
    public void whenconvertingTo() {
        new BaseConverter(Object.class, Object.class) {
        }.convertTo(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void whenconvertingFrom() {
        new BaseConverter(Object.class, Object.class) {
        }.convertFrom(null, null);
    }
}
