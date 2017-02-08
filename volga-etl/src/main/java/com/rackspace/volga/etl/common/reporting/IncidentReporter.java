package com.rackspace.volga.etl.common.reporting;

import java.util.Map;

/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 2:06 PM
 * Copyright Rackspace Hosting, Inc.
 */
public interface IncidentReporter {
    public void createIncident(String jobId, Map<String, String> details);
}
