package com.demo.job;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.demo.common.Constants;
import com.demo.entity.JobConfig;

public class JobManager {

	private static Scheduler scheduler = null;
	//任务id
	private static int jobID = 0;

	public static void init() throws SchedulerException {
		if (scheduler == null) {
			SchedulerFactory sf = new StdSchedulerFactory();
			scheduler = sf.getScheduler();
		}
	}
	
	/**
	 * 调度一个任务
	 * @throws SchedulerException 
	 */
	public static void addJob(JobConfig config) throws SchedulerException {
		jobID++;
		IJob deliveryJob = JobFactory.createJob(config);
		
		String job_id=config.getName()+"_job"+jobID;
		String trigger_id="trigger_"+jobID;
		//将具体的任务实例存储到jobdetail中，这样每次触发jobadapter时，都会调用我们声明的deliveryJob这个实例了。
		JobDetail job = JobBuilder.newJob(JobAdapter.class)
				.withIdentity(job_id, config.getGroup())
				.build(); 
		job.getJobDataMap().put(Constants.JOB_NAME, deliveryJob);  
		
		CronTrigger trigger = (CronTrigger)TriggerBuilder.newTrigger()
				.withIdentity(trigger_id, config.getGroup())
				.withSchedule(CronScheduleBuilder.cronSchedule(config.getScanPeriod()))
				.build();
        scheduler.scheduleJob(job, trigger);  
	}
	
	/**
	 * 启动调度服务
	 * @throws SchedulerException
	 */
	public static void start() throws SchedulerException{
		if(scheduler != null){
			scheduler.start();	
		}
	}

}
