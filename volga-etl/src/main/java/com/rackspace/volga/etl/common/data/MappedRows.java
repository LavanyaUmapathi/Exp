package com.rackspace.volga.etl.common.data;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

/**
 * User: alex.silva
 * Date: 4/14/14
 * Time: 1:44 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class MappedRows {
    private Multimap<String, String> rows = LinkedListMultimap.create();

    public void addRow(String key, String row) {
        rows.put(key, row);
    }

    public void addRows(String key, Iterable<String> lrows) {
        for (String row : lrows) {
            rows.put(key, row);
        }
    }

    public Map<String, Collection<String>> getRows() {
        return rows.asMap();
    }
}
