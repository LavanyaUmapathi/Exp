package com.rackspace.volga.etl.common.transform;

import org.dozer.DozerConverter;

/**
 * User: alex.silva
 * Date: 4/13/15
 * Time: 12:59 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class BaseConverter<A, B> extends DozerConverter<A, B> {

    protected BaseConverter(Class<A> prototypeA, Class<B> prototypeB) {
        super(prototypeA, prototypeB);
    }

    @Override
    public B convertTo(A source, B destination) {
        throw new UnsupportedOperationException("Cannot convert.");
    }

    @Override
    public A convertFrom(B source, A destination) {
        throw new UnsupportedOperationException("Cannot convert.");
    }
}
