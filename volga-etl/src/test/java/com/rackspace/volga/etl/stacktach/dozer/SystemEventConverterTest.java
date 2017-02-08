package com.rackspace.volga.etl.stacktach.dozer;

import com.google.common.io.Files;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.transform.JacksonConverter;
import com.rackspace.volga.etl.stacktach.dto.json.SystemEvent;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Time: 12:37 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class SystemEventConverterTest {
    private SystemEventConverter svt = new SystemEventConverter();

    @Test
    public void testConvert() throws IOException {
        String json = Files.readFirstLine(new ClassPathResource("stacktach/stacktach-single.json").getFile(),
                Charset.defaultCharset());
        SystemEvent c = (SystemEvent) new JacksonConverter(SystemEvent.class).convertFrom(json);
        MappedRows m = (MappedRows) svt.convert(null, c, SystemEvent.class, MappedRows.class);
        assertEquals(m.getRows().size(), 1);
        assertEquals("req-c6cebdfc-67a9-43e4-a4aa-45101867cac6,null,compute.instance.update," +
                "state_description=deleting|terminated_at=|ephemeral_gb=0|instance_type_id=2|deleted_at" +
                "=|reservation_id=r-rz0bumed|memory_mb=512|display_name=testservercc754701|hostname" +
                "=testservercc754701|state=active|old_state=active|progress=|launched_at=2015-02-19 " +
                "18:32:47|node=13-135-17-329934|ramdisk_id=|access_ip_v6=2001:4801:7808:52:67b9:f177:5b72:2f8|disk_gb" +
                "=20|access_ip_v4=10.23.193" +
                ".167|kernel_id=|host=c-10-13-137-17|user_id=c0d405953f374ee1b72e5a012fa21143|image_ref_url=http://10" +
                ".23.244.73:9292/images/de3414ba-a9a1-4b59-ae42-9b966040d75c|cell_name=preprod-ord!c0001" +
                "|audit_period_beginning=2015-02-19 00:00:00|root_gb=20|tenant_id=6093664|created_at=2015-02-19 " +
                "18:28:51|old_task_state=deleting|instance_id=d4bd46e8-0ace-49c8-b9b9-a66491b2254a|instance_type" +
                "=512mb standard instance|vcpus=1|architecture=x64|new_task_state=deleting|audit_period_ending=2015" +
                "-02-19 19:25:31|os_type=linux|instance_flavor_id=2,,," +
                "com.rackspace__1__options=0|container_format=ovf|min_ram=512|com" +
                ".rackspace__1__build_rackconnect=1|com" +
                ".rackspace__1__build_core=1|base_image_ref=de3414ba-a9a1-4b59-ae42-9b966040d75c|os_distro=centos|org" +
                ".openstack__1__os_distro=org.centos|com.rackspace__1__release_id=201|image_type=base|com" +
                ".rackspace__1__source=kickstart|disk_format=vhd|com.rackspace__1__build_managed=1|org" +
                ".openstack__1__architecture=x64|com.rackspace__1__visible_core=0|com" +
                ".rackspace__1__release_build_date=2013-05-28_09-01-31|com" +
                ".rackspace__1__visible_rackconnect=0|min_disk=20|com.rackspace__1__release_version=3|com" +
                ".rackspace__1__visible_managed=0|cache_in_nova=true|auto_disk_config=true|os_type=linux|org" +
                ".openstack__1__os_version=5.8,INFO,false,1424373931512,None.nova-api03-r2961.global.preprod-ord" +
                ".ohthree.com,8ff87ea9-98bb-4343-a526-8e0dbba8a6a1," +
                "object-store:default|compute:default|identity:user-admin,1424373931675,2,19,2015,6093664,no,6093664," +
                "false,6093664,dwqx87505", ((List) m.getRows().get("event")).get(0));
    }
}

