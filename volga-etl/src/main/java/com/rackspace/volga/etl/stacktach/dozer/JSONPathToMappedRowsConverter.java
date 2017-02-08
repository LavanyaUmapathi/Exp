package com.rackspace.volga.etl.stacktach.dozer;

import com.jayway.jsonpath.DocumentContext;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.hive.HiveFormatUtils;
import com.rackspace.volga.etl.common.transform.BaseConverter;
import com.rackspace.volga.etl.stacktach.yaml.StackDistiller;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 4/7/15
 * Time: 3:47 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class JSONPathToMappedRowsConverter extends BaseConverter<MappedRows, DocumentContext> {

    public JSONPathToMappedRowsConverter() {
        super(MappedRows.class, DocumentContext.class);
    }

    @Override
    public MappedRows convertFrom(DocumentContext source, MappedRows rows) {

        StackDistiller distiller;
        try {
            distiller = new StackDistiller(getParameter());
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        Map<String, String> fields = distiller.distill(source);

        if (!fields.isEmpty()) {
            rows.addRow(FilenameUtils.getBaseName(getParameter()), HiveFormatUtils.iterableToString(fields.values(),
                    HiveFormatUtils.FIELD_SEP_CHAR));
        }

        return rows;
    }


}
