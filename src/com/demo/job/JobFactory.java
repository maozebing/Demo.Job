package com.demo.job;

import com.demo.entity.JobConfig;

public class JobFactory {
	
	public static IJob createJob(JobConfig config) {
		String classname = config.getClassName();
		IJob task = null;
		try {
			task = (IJob) Class.forName(classname).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return task;
	}

}
