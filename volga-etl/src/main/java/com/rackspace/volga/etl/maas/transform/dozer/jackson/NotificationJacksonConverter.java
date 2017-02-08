package com.rackspace.volga.etl.maas.transform.dozer.jackson;

import com.rackspace.volga.etl.common.transform.JacksonConverter;
import com.rackspace.volga.etl.maas.dto.json.Notification;

/**
 * User: alex.silva
 * Date: 4/14/15
 * Time: 9:37 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class NotificationJacksonConverter extends JacksonConverter<Notification> {
    public NotificationJacksonConverter() {
        super(Notification.class);
    }
}
