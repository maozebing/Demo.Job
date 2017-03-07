package com.demo.job;

import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import com.demo.common.Constants;

public class JobAdapter implements InterruptableJob {
	private IJob job = null;
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		Object jobObj = context.getJobDetail().getJobDataMap().get(Constants.JOB_NAME);
		System.out.println("job类型:" + jobObj);
		if (jobObj instanceof IJob) {
			job = (IJob) jobObj;
			job.execute();
		} else {
			System.out.println("未知的job类型:" + jobObj.getClass());
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		job.interrupt();
	}

}
