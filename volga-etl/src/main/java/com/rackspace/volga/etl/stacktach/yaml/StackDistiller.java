package com.rackspace.volga.etl.stacktach.yaml;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.core.io.DefaultResourceLoader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 4/8/15
 * Time: 4:02 PM
 * Copyright Rackspace Hosting, Inc.
 * <p/>
 * Sort of like a converter.  Takes a StackTach YAML file and creates a dictionary with keys and values.
 * Keys are the trait names (as defined in the YAML file) and values are extracted directly from the JSON stream.
 */
public class StackDistiller {

    private YamlReader reader;

    private List<Map<String, ?>> yaml;

    private static final String EVENT_TYPE = "event_type";

    public StackDistiller(String yamlLocation) throws IOException {
        try {
            reader = new YamlReader(new InputStreamReader(new DefaultResourceLoader().getResource
                    (yamlLocation).getInputStream()));
            yaml = reader.read(List.class, Map.class);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public Map<String, String> distill(DocumentContext json) {
        String incomingEventType = json.read(EVENT_TYPE);

        final Map<String, String> dict = Maps.newLinkedHashMap();
        for (Map<String, ?> traits : yaml) {
            String yamlEventType = ObjectUtils.toString(traits.get(EVENT_TYPE));
            if (incomingEventType.matches(yamlEventType)) {
                dict.put("event_type",incomingEventType);
                for (Map.Entry<String, Map> traitFields : ((Map<String, Map>) traits.get("traits")).entrySet()) {
                    String trait = traitFields.getKey();
                    Map<String, ?> fields = traitFields.getValue();
                    FieldListIterable iterable = new FieldListIterable(fields.get("fields"));

                    //Careful - what if traits are not unique?
                    dict.put(trait, getValue(iterable.iterator(), json));
                }
            }
        }

        return dict;
    }

    private String getValue(Iterator<String> paths, DocumentContext ctx) {
        String val = StringUtils.EMPTY;
        while (paths.hasNext() && StringUtils.isEmpty(val)) {
            try {
                val = String.valueOf(ctx.read(paths.next()));
            } catch (PathNotFoundException e) {
                continue;
            }
        }

        return val;
    }

    private class FieldListIterable implements Iterable<String> {

        private List<String> fields;

        private FieldListIterable(Object fields) {
            this.fields = fields instanceof Iterable ? Lists.newArrayList((Iterable) fields) : Lists.newArrayList
                    (fields);
        }

        @Override
        public Iterator<String> iterator() {
            return fields.iterator();
        }
    }
}
