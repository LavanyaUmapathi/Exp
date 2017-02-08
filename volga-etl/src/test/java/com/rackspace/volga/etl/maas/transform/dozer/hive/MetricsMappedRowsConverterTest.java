package com.rackspace.volga.etl.maas.transform.dozer.hive;

import com.google.common.io.Files;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.transform.JacksonConverter;
import com.rackspace.volga.etl.maas.dto.json.Metric;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * User: alex.silva
 * Date: 7/24/14
 * Time: 11:58 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class MetricsMappedRowsConverterTest {
    private MetricsMappedRowsConverter cvt = new MetricsMappedRowsConverter();
    private static List<String> json;

    @BeforeClass
    public static void setUp() throws IOException {
        json = Files.readLines(new ClassPathResource("maas/metrics.json").getFile(), Charset.defaultCharset());
    }

    @Test
    public void whenPassingExistingMappedRow() {
        Metric m = (Metric) new JacksonConverter(Metric.class).convertFrom(json.get(0));
        MappedRows rows = new MappedRows();
        MappedRows nrows = cvt.convertFrom(m, rows);
        assertSame(nrows, rows);
    }

    private String getRow(int i) {
        Metric m = (Metric) new JacksonConverter(Metric.class).convertFrom(json.get(i));
        MappedRows rows = cvt.convertFrom(m);
        assertEquals(rows.getRows().size(), 1);
        return (String) ((List) rows.getRows().get("metric")).get(0);
    }

    @Test
    public void testLine1() throws IOException {
        String row = getRow(0);
        assertEquals(",ac4ubIxzmW,898450,enC6anIcNY,chOdIdiZu0,,,agent.load_average,,,true,1395941810615,3,27,2014," +
                "15m=110\u00040.05\u0004unknown\u0004|5m=110\u00040" +
                ".01\u0004unknown\u0004|1m=110\u00040\u0004unknown\u0004", row);
    }

    @Test
    public void testLine2() throws IOException {
        String row = getRow(1);
        assertEquals(",ac4ubIxzmW,898450,enC6anIcNY,chGcBJKHc5,,,agent.memory,,,true,1395941810616,3,27,2014," +
                "swap_free=108\u00040\u0004bytes\u0004|free=108\u000484488192\u0004bytes\u0004|swap_used=108\u00040\u0004bytes" +
                "\u0004|swap_total=108\u00040\u0004bytes\u0004|actual_used=108\u0004203902976\u0004bytes\u0004|swap_page_out" +
                "=108\u00040\u0004bytes\u0004|total=108\u00041039679488\u0004bytes\u0004|swap_page_in=108\u00040\u0004bytes\u0004" +
                "|ram=108\u0004992\u0004megabytes\u0004|actual_free=108\u0004835776512\u0004bytes\u0004|used=108\u0004955191296\u0004bytes" +
                "\u0004", row);
    }

    @Test
    public void testLine3() throws IOException {
        String row = getRow(2);
        assertEquals(",acNXpE0k6y,10031068,enu9ZykVRw,chzU8BElPc,,,agent.filesystem,,,true,1395941810627,3,27,2014," +
                "free_files=108\u00041210890\u0004other\u0004free_files|total=108\u000420641404\u0004kilobytes\u0004|avail=108\u000415778944" +
                "\u0004kilobytes\u0004|free=108\u000416827460\u0004kilobytes\u0004|files=108\u00041310720\u0004other\u0004files|used" +
                "=108\u00043813944\u0004kilobytes\u0004", row);
    }

    @Test
    public void testLine4() throws IOException {
        String row = getRow(3);
        assertEquals(",acdquarnEm,867184,enrVUtDImh,cht9WuZMV4,,,agent.cpu,,,true,1395941810628,3,27,2014," +
                "user_percent_average=110\u00040.26029500100113\u0004percent\u0004|wait_percent_average=110\u00040" +
                ".0066742307949009\u0004percent\u0004|sys_percent_average=110\u00040" +
                ".20022692384703\u0004percent\u0004|idle_percent_average=110\u000499" +
                ".526129613562\u0004percent\u0004|irq_percent_average=110\u00040\u0004percent\u0004|usage_average=110" +
                "\u00040.47387038643796\u0004percent\u0004|min_cpu_usage=110\u00040" +
                ".47387038643796\u0004percent\u0004|max_cpu_usage=110\u00040" +
                ".47387038643796\u0004percent\u0004|stolen_percent_average=110\u00040" +
                ".0066742307949009\u0004percent\u0004", row);
    }

    @Test
    public void testLine5() throws IOException {
        String row = getRow(4);
        assertEquals(",acVuvDppDE,827143,enfjfXBnXN,chdgPwjeKK,,,agent.cpu,,,true,1395941810662,3,27,2014," +
                        "user_percent_average=110\u00041" +
                        ".1834319526627\u0004percent\u0004|wait_percent_average=110\u00040\u0004percent\u0004" +
                        "|sys_percent_average=110\u00040" +
                        ".9861932938856\u0004percent\u0004|idle_percent_average=110\u000497" +
                        ".830374753452\u0004percent\u0004|irq_percent_average=110\u00040\u0004percent\u0004" +
                        "|usage_average=110" +
                        "\u00042.1696252465483\u0004percent\u0004|min_cpu_usage=110\u00040\u0004percent\u0004" +
                        "|max_cpu_usage" +
                        "=110\u00044.3392504930966\u0004percent\u0004|stolen_percent_average=110\u00040\u0004percent" +
                        "\u0004",
                row);
    }

    @Test
    public void testLine6() throws IOException {
        String row = getRow(5);
        assertEquals(",acakjNMm8y,870254,enQBXZMjxG,chu3oG0R8f,,,agent.mysql,,,true,1395941810730,3,27,2014," +
                "core.aborted_clients=76\u000413889\u0004other\u0004clients|core" +
                ".queries=108\u0004274316581\u0004other\u0004queries|core" +
                ".connections=108\u00043042319\u0004other\u0004connections|core.uptime=76\u0004418072\u0004seconds\u0004|threads" +
                ".created=76\u000488\u0004other\u0004threads|threads.running=76\u00041\u0004other\u0004threads|threads" +
                ".connected=76\u00047\u0004other\u0004threads|qcache" +
                ".queries_in_cache=76\u00041633\u0004other\u0004queries|qcache" +
                ".lowmem_prunes=108\u000497869\u0004other\u0004prunes|qcache.free_memory=76\u0004129261976\u0004bytes\u0004|qcache" +
                ".total_blocks=76\u00043908\u0004other\u0004blocks|qcache" +
                ".inserts=108\u000417146389\u0004other\u0004inserts|qcache.hits=108\u0004235035617\u0004other\u0004hits|qcache" +
                ".not_cached=108\u00046392954\u0004other\u0004queries|qcache" +
                ".free_blocks=76\u0004581\u0004other\u0004blocks|innodb" +
                ".buffer_pool_pages_total=76\u0004153599\u0004other\u0004pages|innodb" +
                ".buffer_pool_pages_flushed=76\u0004345791\u0004other\u0004pages|innodb" +
                ".row_lock_time_avg=76\u00043343\u0004milliseconds\u0004|innodb" +
                ".rows_inserted=108\u000423647\u0004other\u0004rows|innodb" +
                ".row_lock_time_max=76\u00047213\u0004milliseconds\u0004|innodb" +
                ".rows_updated=108\u0004411620\u0004other\u0004rows|innodb" +
                ".buffer_pool_pages_free=76\u0004153405\u0004other\u0004pages|innodb" +
                ".buffer_pool_pages_dirty=76\u00046\u0004other\u0004pages|innodb" +
                ".row_lock_time=76\u000456845\u0004milliseconds\u0004|innodb" +
                ".rows_deleted=108\u0004104\u0004other\u0004rows|innodb.rows_read=108\u00042471063102\u0004other\u0004rows" +
                "", row);
    }

    @Test
    public void testLine7() throws IOException {
        String row = getRow(6);
        assertEquals(",aci8387voR,751305,enKTX4XaA0,chBSBlSJWF,,,agent.load_average,,,true,1395941810800,3,27,2014," +
                "15m=110\u00040\u0004unknown\u0004|5m=110\u00040\u0004unknown\u0004|1m=110\u00040\u0004unknown\u0004" +
                "", row);
    }

    @Test
    public void testLine8() throws IOException {
        String row = getRow(7);
        assertEquals(",aci8387voR,751305,enKTX4XaA0,chDnDxCHds,,,agent.network,,,true,1395941810800,3,27,2014," +
                "tx_dropped=108\u00040\u0004other\u0004packets|tx_errors=108\u00040\u0004other\u0004errors|tx_overruns" +
                "=108\u00040\u0004other\u0004overruns|rx_errors=108\u00040\u0004other\u0004errors|rx_frame=108\u0004" +
                "0\u0004other\u0004frames|tx_collisions=108\u00040\u0004other\u0004collisions|tx_bytes=108\u0004" +
                "15470953063\u0004bytes\u0004|rx_overruns=108\u00040\u0004other\u0004overruns|rx_packets=108\u000421231266\u0004other" +
                "\u0004packets|tx_carrier=108\u00040\u0004other\u0004errors|tx_packets=108\u000415912569\u0004other" +
                "\u0004packets|rx_dropped=108\u00040\u0004other\u0004packets|rx_bytes=108\u00042656803013\u0004bytes\u0004", row);
    }

    @Test
    public void testLine9() throws IOException {
        String row = getRow(8);
        assertEquals(",aci8387voR,751305,enKTX4XaA0,chfeN7EGKi,,,agent.filesystem,,,true,1395941810802,3,27,2014," +
                "options=115\u0004rw\\," +
                "relatime\u0004other\u0004options|free_files=108\u0004655090\u0004other\u0004free_files|total=108\u0004" +
                "10321208\u0004kilobytes\u0004|avail=108\u00049676840\u0004kilobytes\u0004|free=108\u00049781696\u0004kilobytes\u0004|files" +
                "=108\u0004655360\u0004other\u0004files|used=108\u0004539512\u0004kilobytes\u0004", row);
    }

    @Test
    public void testLine10() throws IOException {
        String row = getRow(9);
        assertEquals(",aci8387voR,751305,enKTX4XaA0,chsI2EnnhJ,,,agent.memory,,,true,1395941810803,3,27,2014," +
                "swap_free=108\u00040\u0004bytes\u0004|free=108\u0004325873664\u0004bytes\u0004|swap_used=108\u00040\u0004bytes" +
                "\u0004|swap_total=108\u00040\u0004bytes\u0004|actual_used=108\u0004650113024\u0004bytes\u0004|swap_page_out" +
                "=108\u000447\u0004bytes\u0004|total=108\u00041073741824\u0004bytes\u0004|swap_page_in=108\u00040\u0004bytes\u0004" +
                "|ram=108\u00041024\u0004megabytes\u0004|actual_free=108\u0004423628800\u0004bytes\u0004|used=108\u0004747868160\u0004bytes" +
                "\u0004", row);
    }
}
