package com.rackspace.volga.etl.mapreduce.lib.selectors;

/**
 * User: alex.silva
 * Date: 6/4/14
 * Time: 10:31 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class Selectors {
    private Selectors() {
    }

    public static Selector get(String s) {
        String selectorName = "com.rackspace.volga.etl.mapreduce.lib.selectors." + s.split(":")[0];
        try {
            return (Selector) Class.forName(selectorName).getConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Selector not found: " + e.getMessage());
        }
    }
}
