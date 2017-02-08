package com.rackspace.volga.etl.spring.batch.listeners.step;

import com.rackspace.volga.etl.spring.batch.listeners.job.ETLJobListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * User: alex.silva
 * Date: 8/7/14
 * Time: 9:02 AM
 * Copyright Rackspace Hosting, Inc.
 */
@Component
public class MapReduceStatsListener extends StepExecutionListenerSupport {

    @Autowired
    JdbcTemplate statsListenerTemplate;

    @Value("${volga.etl.listeners.stats.collect:false}")
    private boolean shouldRun;

    private static final String SQL = "insert into acumen_etl_metrics (job_name,start_time,end_time,status,exit_code," +
            "input_records,output_records,valid_records,invalid_records) values (?,?,?,?,?,?,?,?,?);";


    @Override
    public ExitStatus afterStep(final StepExecution mapReduceStep) {
        if (shouldRun && mapReduceStep.getStepName().contains("MapReduce")) {
            statsListenerTemplate.update(new PreparedStatementCreator() {
                @Override
                public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                    PreparedStatement pstmt = connection.prepareStatement(SQL);
                    pstmt.setString(1, mapReduceStep.getStepName());
                    pstmt.setDate(2, new java.sql.Date(mapReduceStep.getStartTime().getTime()));
                    pstmt.setDate(3, new java.sql.Date(mapReduceStep.getEndTime().getTime()));
                    pstmt.setString(4, mapReduceStep.getStatus().name());
                    pstmt.setString(5, mapReduceStep.getExitStatus().getExitCode());
                    pstmt.setLong(6, mapReduceStep.getExecutionContext().getLong(ETLJobListener
                            .MAP_INPUT_RECORDS_KEY, 0));
                    pstmt.setLong(7, mapReduceStep.getExecutionContext().getLong(ETLJobListener
                            .MAP_OUTPUT_RECORDS_KEY, 0));
                    pstmt.setLong(8, mapReduceStep.getExecutionContext().getLong(ETLJobListener.VALID_RECORDS_KEY, 0));
                    pstmt.setLong(9, mapReduceStep.getExecutionContext().getLong(ETLJobListener.INVALID_RECORDS_KEY,
                            0));
                    return pstmt;
                }
            });
        }

        return mapReduceStep.getExitStatus();
    }


}
