package com.rackspace.volga.etl.common.transform;

import com.google.common.collect.Lists;
import org.dozer.DozerBeanMapper;

/**
 * User: alex.silva
 * Date: 4/14/14
 * Time: 3:03 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class DozerTransformer<A, B> implements Transformer<A, B> {
    final DozerBeanMapper dozer;

    public DozerTransformer() {
        dozer = new DozerBeanMapper();
        dozer.setMappingFiles(Lists.newArrayList("dozer/dozer.xml"));
    }

    @Override
    public B transform(A object, Class<? extends B> type) {
        return dozer.map(object, type);
    }

    @Override
    public B transform(A object, Class<? extends B> type, String transformationId) {
        return dozer.map(object, type, transformationId);
    }
}
