package com.demo.job;

/**
 * 任务调度接口
 */
public interface IJob {
	/**
	 * 所有都要实现该执行方法，任务被调度时会调用
	 */
	void execute();

	/**
	 * 打断执行方法
	 */
	void interrupt();
}
