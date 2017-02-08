package com.rackspace.volga.etl.spring.batch.listeners.job;

import com.google.common.collect.Maps;
import com.rackspace.volga.etl.common.reporting.IncidentReporter;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 9:02 AM
 * Copyright Rackspace Hosting, Inc.
 */
@Component
public class VolgaETLJobListener extends ETLJobListener {

    private static final Logger log = LogManager.getLogger(VolgaETLJobListener.class);

    @Autowired
    private IncidentReporter pagerDuty;

    @Override
    public void afterJob(JobExecution exec) {
        //did any of the steps report incidents?
        Map<String, String> details = Maps.newLinkedHashMap();
        for (StepExecution stepExecution : exec.getStepExecutions()) {
            for (Map.Entry<String, Object> entry : stepExecution.getExecutionContext().entrySet()) {
                if (entry.getKey().startsWith(ETLJobListener.INCIDENT_KEY_PREFIX)) {
                    details.put("Step Reporting Error", entry.getKey().substring(ETLJobListener.INCIDENT_KEY_PREFIX
                            .length()));
                    details.putAll((Map<? extends String, ? extends String>) entry.getValue());
                }
            }
        }

        if (details.size() > 1 || exec.getExitStatus().equals(ExitStatus.FAILED)) {
            details.putAll(buildIncidentMap(exec));
            log.debug(details);
            pagerDuty.createIncident("Volga ETL" + "::" + exec.getJobInstance().getJobName(), details);
        }


    }

    IncidentReporter getIncidentReporter() {
        return pagerDuty;
    }

    private Map<String, String> buildIncidentMap(JobExecution exec) {
        Map<String, String> details = Maps.newHashMap();
        details.put("Job Name", exec.getJobInstance().getJobName());
        details.put("Job Start Time", new DateTime(exec.getStartTime()).withZone(DateTimeZone.UTC).toString());
        details.put("Job End Time", new DateTime(exec.getEndTime()).withZone(DateTimeZone.UTC).toString());
        int i = 0;
        Map<String, JobParameter> params = exec.getJobParameters().getParameters();
        for (Map.Entry<String, JobParameter> param : params.entrySet()) {
            details.put("Job Parameter " + param.getKey(), ObjectUtils.toString(param.getValue()));
        }
        details.put("Job Configuration Name", exec.getJobConfigurationName());
        details.put("Job Exit Code", exec.getExitStatus().getExitCode());
        details.put("Job Exit Description", exec.getExitStatus().getExitDescription());
        i = 0;
        for (Throwable t : exec.getFailureExceptions()) {
            details.put("Job Exception " + (++i), t.getMessage());
        }

        return details;
    }

}
