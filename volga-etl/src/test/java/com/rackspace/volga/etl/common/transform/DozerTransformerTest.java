package com.rackspace.volga.etl.common.transform;

import org.junit.Assert;
import org.junit.Test;

/**
 * User: alex.silva
 * Date: 7/28/14
 * Time: 10:17 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class DozerTransformerTest {
    DozerTransformer d = new DozerTransformer();

    @Test
    public void whenCheckingMappingFiles(){
        Assert.assertTrue(d.dozer.getMappingFiles().contains("dozer/dozer.xml"));
    }
}
