package com.rackspace.volga.etl.common.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/**
 * User: alex.silva
 * Date: 4/14/14
 * Time: 10:36 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class HiveFormatUtils {

    private static final String EMPTY = StringUtils.EMPTY;

    public static final String MAP_SEP_CHAR = "=";

    public static final String COLLECTION_SEP_CHAR = "|";

    public static final String FIELD_SEP_CHAR = ",";

    public static final String FOURTH_LVL_DELIM = new String(new char[]{4});

    public static final String ESCAPE_CHAR = "\\";

    private static final ExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();


    private HiveFormatUtils() {
    }


    public static String iterableToString(Iterable<?> list, String delim) {
        if (list == null) {
            return EMPTY;
        }
        return Joiner.on(delim).useForNull(EMPTY).join(list);
    }

    public static String mapToString(Map<String, Object> map, String mapKeysDelim, String delim) {
        if (map == null) {
            return EMPTY;
        }
        List<String> temp = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            temp.add(sanitize(entry.getKey()) + mapKeysDelim + sanitize(entry.getValue()));
        }
        return Joiner.on(delim).useForNull(EMPTY).join(temp);
    }


    public static String getDistinctPropertyValues(List<?> array, String property, String delim) {
        SortedSet<String> attributes = Sets.newTreeSet();
        if (array != null) {
            for (Object object : array) {
                StandardEvaluationContext ctx = new StandardEvaluationContext(object);
                String value = ObjectUtils.toString(EXPRESSION_PARSER.parseExpression(property).getValue(ctx));
                if (!StringUtils.isEmpty(value)) {
                    attributes.add(value);
                }
            }
        }

        return Joiner.on(delim).useForNull(EMPTY).join(attributes);
    }

    public static String toStructMap(Map<String, List<String>> values) {
        Map<String, String> intermediate = Maps.newLinkedHashMap();
        for (Map.Entry<String, List<String>> entry : values.entrySet()) {
            intermediate.put(entry.getKey(), Joiner.on(FOURTH_LVL_DELIM).join(entry.getValue()));
        }

        List<String> temp = Lists.newArrayList();
        for (Map.Entry<String, String> entry : intermediate.entrySet()) {
            temp.add(entry.getKey() + MAP_SEP_CHAR + entry.getValue());
        }
        return Joiner.on(COLLECTION_SEP_CHAR).join(temp);
    }

    public static String toStructMap(Map<String, ?> map, String[] keys) {
        Map<String, List<String>> intermediate = Maps.newLinkedHashMap();
        List<String> temp;
        for (Object key : map.keySet()) {
            Map<String, ?> child = (Map<String, ?>) map.get(key);
            temp = Lists.newArrayList();
            for (String mkey : keys) {
                temp.add(sanitize(ObjectUtils.toString(child.get(mkey))));
            }
            intermediate.put(sanitize(key.toString()), temp);
        }

        return toStructMap(intermediate);
    }

    /**
     * Uses reflection to create a struct (fourth-level) based collection to be stored as a ARRAY<STRUCT> column in
     * Hive.
     *
     * @param list The list containing the java object(s) to be converted into the Struct String.
     * @param keys  The property name(s) of the objects in the list that will be used to construct the Struct.
     * @return The stringfied struct, ready for insertion as an ARRAY<STRUCT> column.
     */
    public static String toStructList(List<?> list, String[] keys) {
        List<String> intermediate = Lists.newArrayList();

        for (Object entry : list) {
            List<Object> fields = Lists.newArrayList();
            for (String key : keys) {
                try {
                    fields.add(PropertyUtils.getProperty(entry, key));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            intermediate.add(Joiner.on(FOURTH_LVL_DELIM).useForNull(StringUtils.EMPTY).join(fields));
        }

        return Joiner.on(COLLECTION_SEP_CHAR).useForNull(StringUtils.EMPTY).join(intermediate);
    }

    public static String sanitize(Object o) {
        return sanitize(o, true);
    }

    public static String sanitize(Object o, boolean toLower) {
        String s = toLower ? ObjectUtils.toString(o).toLowerCase() : ObjectUtils.toString(o);
        s = StringEscapeUtils.escapeJava(s);
        s = s.replace(HiveFormatUtils.ESCAPE_CHAR, HiveFormatUtils.ESCAPE_CHAR + HiveFormatUtils.ESCAPE_CHAR);
        s = s.replace(HiveFormatUtils.FIELD_SEP_CHAR, HiveFormatUtils.ESCAPE_CHAR + HiveFormatUtils.FIELD_SEP_CHAR);
        s = s.replace(HiveFormatUtils.MAP_SEP_CHAR, HiveFormatUtils.ESCAPE_CHAR + HiveFormatUtils.MAP_SEP_CHAR);
        s = s.replace(HiveFormatUtils.COLLECTION_SEP_CHAR, HiveFormatUtils.ESCAPE_CHAR + HiveFormatUtils
                .COLLECTION_SEP_CHAR);
        s = s.replace(HiveFormatUtils.FOURTH_LVL_DELIM, HiveFormatUtils.ESCAPE_CHAR + HiveFormatUtils.FOURTH_LVL_DELIM);
        return s;
    }

}
