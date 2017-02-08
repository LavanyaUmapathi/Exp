package com.rackspace.volga.etl.spring.reporting;

import biz.neustar.pagerduty.PagerDutyClient;
import biz.neustar.pagerduty.model.EventResponse;
import com.rackspace.volga.etl.common.reporting.IncidentReporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 2:04 PM
 * Copyright Rackspace Hosting, Inc.
 */
@Component
public class PagerDutyIncidentReporter implements IncidentReporter {
    private static final Logger log = LogManager.getLogger(PagerDutyIncidentReporter.class);

    @Value("${volga.pagerduty.servicekey}")
    private String pagerDutyServiceKey;

    @Value("${volga.pagerduty.sudomain}")
    private String pagerDutySubdomain;

    @Value("${volga.pagerduty.user}")
    private String pagerDutyUsername;

    @Value("${volga.pagerduty.password}")
    private String pagerDutyPassword;

    private PagerDutyClient client;

    @PostConstruct
    public void init() {
        client = new PagerDutyClient(pagerDutySubdomain, pagerDutyUsername, pagerDutyPassword);

    }

    public void createIncident(String jobId, Map<String, String> details) {
        try {
            EventResponse response = client.trigger(pagerDutyServiceKey, "Job Error", jobId, details);
            String incidentKey = response.getIncidentKey();
            log.info("Logged incident into PagerDuty with incident key " + incidentKey);
        } catch (Exception e) {
            log.error(e);
        }
    }
}
