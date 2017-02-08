package com.rackspace.volga.etl.maas.transform.dozer.hive;

import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.maas.dto.json.*;

/**
 * User: alex.silva
 * Date: 4/14/14
 * Time: 1:42 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class ConfigurationMappedRowsConverter extends MaasMappedRowsConverter<Configuration> {

    public ConfigurationMappedRowsConverter() {
        super(MappedRows.class, Configuration.class);
    }


    @Override
    public MappedRows convertFrom(Configuration cfg, MappedRows rows) {
        if (rows == null) {
            rows = new MappedRows();
        }
        rows.addRow(CONFIG_KEY, getConfigurationFields(cfg));

        if (cfg.getContacts() != null) {
            String date = cfg.getContacts().getDate();
            for (Contact contact : cfg.getContacts().getContacts()) {
                rows.addRow(CONTACT_KEY, getContactFields(contact, date));
            }
        }

        for (Entity entity : cfg.getEntities()) {
            rows.addRow(ENTITY_KEY, getEntityRow(entity, cfg));
            for (Check check : entity.getChecks()) {
                rows.addRow(CHECK_KEY, getCheckRow(check, entity, cfg.getId()));
            }
            for (Alarm alarm : entity.getAlarms()) {
                rows.addRow(ALARM_KEY, getAlarmRow(alarm, entity, cfg.getId()));
            }
        }

        return rows;
    }


}
