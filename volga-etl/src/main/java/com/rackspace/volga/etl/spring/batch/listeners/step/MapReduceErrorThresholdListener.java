package com.rackspace.volga.etl.spring.batch.listeners.step;

import com.google.common.collect.Maps;
import com.rackspace.volga.etl.spring.batch.listeners.job.ETLJobListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class MapReduceErrorThresholdListener extends StepExecutionListenerSupport {

    private static final Logger log = LogManager.getLogger(MapReduceErrorThresholdListener.class);

    @Value("${volga.etl.error.threshold:.1}")
    private double errorThreshold;

    @Value("${volga.etl.fail.on.threshold:true}")
    private boolean shouldFailJobWhenThresholdExceeded;

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (stepExecution.getStepName().contains("MapReduce")) {
            inspectCounters(stepExecution);
        }

        return stepExecution.getExitStatus();
    }

    private void inspectCounters(StepExecution mapReduceStep) {
        ExecutionContext ctx = mapReduceStep.getExecutionContext();
        long mapInputRecords = ctx.getLong(ETLJobListener.MAP_INPUT_RECORDS_KEY, 1);
        long mapOutputRecords = ctx.getLong(ETLJobListener.MAP_OUTPUT_RECORDS_KEY, 0);
        double errorRatio = 1 - ((double) mapOutputRecords / mapInputRecords);
        if (errorRatio >= errorThreshold) {
            if (shouldFailJobWhenThresholdExceeded) {
                mapReduceStep.setExitStatus(ExitStatus.FAILED);
            }
            Map<String, String> details = Maps.newLinkedHashMap();
            details.put(mapReduceStep.getStepName(), "Error Threshold Exceeded");
            details.put("Current Error Threshold", String.valueOf(errorThreshold));
            details.put("Error Ratio", String.valueOf(errorRatio));
            details.put("Map Input Records", String.valueOf(ctx.getLong(ETLJobListener.MAP_INPUT_RECORDS_KEY)));
            details.put("Map Output Records", String.valueOf(ctx.getLong(ETLJobListener.MAP_OUTPUT_RECORDS_KEY)));
            details.put("Reduce Input Records", String.valueOf(ctx.getLong(ETLJobListener.REDUCE_INPUT_RECORDS_KEY)));
            details.put("Step Exit Code", mapReduceStep.getExitStatus().getExitCode());
            mapReduceStep.getExecutionContext().put(ETLJobListener.INCIDENT_KEY_PREFIX + mapReduceStep.getStepName(),
                    details);
            log.debug("Details Map: " + details);

        }
    }


}