package com.rackspace.volga.etl.spring.run;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.rackspace.volga.etl.mapreduce.args.CommandLineBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.BeansException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.Map;

/**
 * User: alex.silva
 * Date: 8/6/14
 * Time: 1:39 PM
 * Copyright Rackspace Hosting, Inc.
 */
@Configuration
@ImportResource("classpath:/META-INF/spring/application-context.xml")
public class JobRunner implements CommandLineRunner, ApplicationContextAware {

    private ApplicationContext context;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(JobRunner.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Map<String, JobLauncher> launchers = context.getBeansOfType(JobLauncher.class);
        CommandLine cmd = new CommandLineBuilder().buildCommonOptions(args);
        String launcherName = cmd.getOptionValue("job.launcher");

        JobLauncher jobLauncher = StringUtils.isEmpty(launcherName) ? Iterables.getFirst(launchers.values(),
                null) : launchers.get(launcherName);

        String[] parsedArgs = cmd.getArgs();

        Preconditions.checkArgument(parsedArgs.length > 0, "JSON entity name ('metrics', 'notifications', or " +
                "'accounts') is required.");

        Job job = context.getBean(cmd.getArgs()[0], Job.class);

        Map<String, JobParameter> parameterMap = Maps.newHashMap();
        for (Option o : cmd.getOptions()) {
            parameterMap.put(o.getLongOpt(), new JobParameter(o.getValue()));
        }
        JobParameters params = new JobParameters(parameterMap);

        jobLauncher.run(job, params);

        ((AbstractApplicationContext) context).registerShutdownHook();
    }

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        this.context = context;
    }


}
