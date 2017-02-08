package com.rackspace.volga.etl.mapreduce.etl.transform.hive;

import com.rackspace.volga.etl.common.transform.Transformer;
import org.junit.Before;
import org.junit.Test;

/**
 * User: alex.silva
 * Date: 4/7/14
 * Time: 12:41 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class HiveJSONMultiOutputsJSONTransformerTest {

    private Transformer transformer;

    @Before
    public void init() {
//        transformer = new HiveConstants();
//        transformer.addMappingResources("classpath:hive/notifications.map");
//        transformer.init();
    }

    @Test
    public void testNotificationOutput() throws Exception {
        String event = "{\"event_id\":\"acQbNvhp5G:en3DhtNVYW:alfwsx5IXI:ch59FjYqvB:1395789300000:OK\"," +
                "\"log_entry_id\":\"8b1f7460-b473-11e3-ab32-fee51b7db4f8\",\"details\":{\"target\":null," +
                "\"timestamp\":1395789409958,\"metrics\":{\"total\":{\"type\":\"l\",\"data\":8356659200," +
                "\"unit\":\"bytes\"},\"swap_used\":{\"type\":\"l\",\"data\":286720,\"unit\":\"bytes\"}," +
                "\"free\":{\"type\":\"l\",\"data\":677883904,\"unit\":\"bytes\"},\"swap_free\":{\"type\":\"l\"," +
                "\"data\":2047705088,\"unit\":\"bytes\"},\"ram\":{\"type\":\"l\",\"data\":7976," +
                "\"unit\":\"megabytes\"},\"swap_page_out\":{\"type\":\"l\",\"data\":231379,\"unit\":\"bytes\"}," +
                "\"actual_used\":{\"type\":\"l\",\"data\":1505488896,\"unit\":\"bytes\"}," +
                "\"actual_free\":{\"type\":\"l\",\"data\":6851170304,\"unit\":\"bytes\"}," +
                "\"swap_page_in\":{\"type\":\"l\",\"data\":145571,\"unit\":\"bytes\"},\"used\":{\"type\":\"l\"," +
                "\"data\":7678775296,\"unit\":\"bytes\"},\"swap_total\":{\"type\":\"l\",\"data\":2047991808," +
                "\"unit\":\"bytes\"}},\"state\":\"OK\",\"status\":\"More than 64MB of swap available\"," +
                "\"txn_id\":\".rh-kkil.h-ord1-maas-prod-agent0.r-NRV9HH4X.c-204.ts-1395785089015.v-29c1c1d\"," +
                "\"observations\":[{\"monitoring_zone_id\":null,\"state\":\"OK\",\"status\":\"More than 64MB of swap " +
                "available\",\"timestamp\":1395789409958}]},\"entity\":{\"id\":\"en3DhtNVYW\"," +
                "\"label\":\"s4a-cloud-1\",\"ip_addresses\":{\"private0_v4\":\"10.176.99.114\"," +
                "\"public0_v4\":\"198.61.235.247\",\"public1_v6\":\"2001:4801:7811:0069:4155:3bd2:ff10:39cf\"," +
                "\"access_ip0_v4\":\"184.106.60.91\",\"access_ip1_v6\":\"2001:4801:7811:0069:4155:3bd2:ff10:39cf\"}," +
                "\"metadata\":null,\"managed\":true,\"uri\":\"https://ord.servers.api.rackspacecloud" +
                ".com/777307/servers/09622cfd-a553-4596-a719-6d06c11eeb35\",\"agent_id\":\"s4a-cloud-1\"," +
                "\"created_at\":1359065625543,\"updated_at\":1359066573842},\"check\":{\"id\":\"ch59FjYqvB\"," +
                "\"label\":\"Memory\",\"type\":\"agent.memory\",\"details\":{},\"monitoring_zones_poll\":[]," +
                "\"timeout\":30,\"period\":300,\"target_alias\":null,\"target_hostname\":null," +
                "\"target_resolver\":null,\"disabled\":false,\"metadata\":null,\"created_at\":1383676965737," +
                "\"updated_at\":1390643180814},\"alarm\":{\"id\":\"alfwsx5IXI\",\"label\":\"Low Swap Space\"," +
                "\"check_id\":\"ch59FjYqvB\",\"criteria\":\"if (metric['swap_free'] < 67108864) {\\n  return new " +
                "AlarmStatus(CRITICAL, 'Less than 64MB of swap remaining');\\n}\\nreturn new AlarmStatus(OK, " +
                "'More than 64MB of swap available');\\n\",\"disabled\":false,\"notification_plan_id\":\"npManaged\"," +
                "\"metadata\":{\"template_name\":\"agent.managed_low_swap_free\"},\"created_at\":1383676969086," +
                "\"updated_at\":1390643486385},\"tenant_id\":\"777307\"}";

//        String s = transformer.transform(new JSONParser().parse(event)).get("ROW").toString();
//
//        System.out.println(s);
    }


}
