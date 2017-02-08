package com.rackspace.volga.etl.common.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.rackspace.volga.etl.maas.dto.json.Entity;
import com.rackspace.volga.etl.utils.CoverageForPrivateConstructor;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 7/23/14
 * Time: 4:13 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class HiveFormatUtilsTest {
    @Test
    public void testListToString() {
        assertEquals("", HiveFormatUtils.iterableToString(null, ""));
        List<String> values = Lists.newArrayList("one", "two", "three");
        assertEquals("one;two;three", HiveFormatUtils.iterableToString(values, ";"));
        values = Lists.newArrayList("one", "two", "three", null);
        assertEquals("one;two;three;", HiveFormatUtils.iterableToString(values, ";"));
    }

    @Test
    public void testMapToString() {
        assertEquals("", HiveFormatUtils.mapToString(null, "", ""));
        Map map = ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3");
        assertEquals("k1=v1,k2=v2,k3=v3", HiveFormatUtils.mapToString(map, "=", ","));
        map = ImmutableMap.of("k1", "v1", "k2", "v2", "null", "v3");
        assertEquals("k1=v1,k2=v2,null=v3", HiveFormatUtils.mapToString(map, "=", ","));
    }

    @Test
    public void testGetDistinctValuesForField() {
        assertEquals("", HiveFormatUtils.getDistinctPropertyValues(null, "", ""));
        Entity e1 = new Entity();
        e1.setId("e1");
        Entity e2 = new Entity();
        e2.setId("e1");
        Entity e3 = new Entity();
        e3.setId("e3");
        List<Entity> vals = Lists.newArrayList(e1, e2, e3);
        assertEquals("e1,e3", HiveFormatUtils.getDistinctPropertyValues(vals, "id", ","));
        vals = Lists.newArrayList(e1, e2, new Entity());
        assertEquals("e1", HiveFormatUtils.getDistinctPropertyValues(vals, "id", ","));
    }

    @Test
    public void testToStructMap() {
        Map test = ImmutableMap.of("k1", Lists.newArrayList(1, 2, 3, 4, 5), "k2", Lists.newArrayList(6, 7, 8));
        String s = HiveFormatUtils.toStructMap(test);
        String truth = "k1=1\u00042\u00043\u00044\u00045|k2=6\u00047\u00048";
        assertEquals(truth, s);
    }

    @Test
    public void testToStructMapKeyed() {
        Map test = ImmutableMap.of("k1", ImmutableMap.of("k11", "v11", "k12", "v12"), "k2", ImmutableMap.of("k21",
                "v21", "k22", "v22"));
        String s = HiveFormatUtils.toStructMap(test, new String[]{"k11"});
        assertEquals("k1=v11|k2=", s);
        s = HiveFormatUtils.toStructMap(test, new String[]{"k11","k21","k22"});
        assertEquals("k1=v11\u0004\u0004|k2=\u0004v21\u0004v22", s);
    }

    @Test
    public void whenSanitizingJavaChars() throws IOException {
        assertSanitize("empty string", "", "");
        assertSanitize("tab", "\\\\t", "\t");
        assertSanitize("backslash", "\\\\\\\\", "\\");
        assertSanitize("single quote should not be escaped", "'", "'");
        assertSanitize(null, "\\\\u1234", "\u1234");
        assertSanitize(null, "\\\\u0234", "\u0234");
        assertSanitize(null, "\\\\u00EF", "\u00ef");
        assertSanitize(null, "\\\\u0001", "\u0001");
        assertSanitize("Should use capitalized Unicode hex", "\\\\uABCD", "\uabcd");
        assertSanitize(null, "he didn't say\\, \\\\\"stop!\\\\\"", "He didn't say, \"stop!\"");
        assertSanitize("non-breaking space", "this space is non-breaking\\=" + "\\\\u00A0",
                "this space is non-breaking=\u00a0");
        assertSanitize("fourthlevel", "this is a fourth level char:\\\\u0004",
                "this is a fourth level char:\u0004");
    }

    private void assertSanitize(String message, final String expected, final String original) throws IOException {
        final String converted = HiveFormatUtils.sanitize(original);
        message = "whenSanitizingJavaChars(String) failed" + (message == null ? "" : (": " + message));
        assertEquals(message, expected, converted);

    }

    @Test
    public void whenLowerCasingString() {
        assertEquals("lower", HiveFormatUtils.sanitize("LOWER"));
        assertEquals("lower", HiveFormatUtils.sanitize("LOWER", true));
        assertEquals("LOWER", HiveFormatUtils.sanitize("LOWER", false));
    }

    @Test
    public void testConstructorPrivate() throws Exception {
        CoverageForPrivateConstructor.giveMeCoverage(HiveFormatUtils.class);
    }

}
