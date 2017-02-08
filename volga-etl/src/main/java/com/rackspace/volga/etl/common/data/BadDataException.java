package com.rackspace.volga.etl.common.data;

/**
 * User: alex.silva
 * Date: 5/19/14
 * Time: 12:39 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class BadDataException extends RuntimeException {
    public BadDataException() {
        super();
    }

    public BadDataException(Exception e) {
        super(e);
    }

    public BadDataException(String msg) {
        super(msg);
    }

}
