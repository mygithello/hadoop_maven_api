
package com.demo.hadoop.common.hive.server;

import java.text.ParseException;

import com.demo.hadoop.common.hive.quartz.StatsJobQuartz;
import com.demo.hadoop.conf.SystemConfig;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 定时任务统计指标入口.
 */
public class StatsServer {

	/** 申明日志打印对象. */
	private static final Logger LOG = LoggerFactory.getLogger(StatsServer.class);

	private StatsServer() {

	}

	/** 主函数入口. */
	public static void main(final String[] args) {
		//args参数设置：hivetest.xml
		try {
			if (args.length != 1) {
				LOG.info("Stats name has error,please check input.");
				return;
			}
			//final String jobPath = System.getProperty("user.dir") + "/conf/" + args[0];
			System.out.println("arg[0]:"+args[0]);
			final String jobPath = System.getProperty("user.dir") + "/src/main/resources/" + args[0];
			jobStart(jobPath);
		} catch (Exception ex) {
			LOG.error("Run stats job has error, msg is " + ex.getMessage());
		}

	}

	/** 启动定时任务. */
	private static void jobStart(final String jobPath) throws ParseException, SchedulerException {

		final JobDetail job = new JobDetail("job_daily_" + System.currentTimeMillis(), StatsJobQuartz.class);
		final JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put("task", jobPath);
		job.setJobDataMap(jobDataMap);
		final CronTrigger cron = new CronTrigger("cron_daily_" + System.currentTimeMillis(), "cron_daily_" + System.currentTimeMillis(), SystemConfig.getProperty("com.demo.hadoop.testcron.crontab"));
		final SchedulerFactory schedulerFactory = new StdSchedulerFactory();
		final Scheduler scheduler = schedulerFactory.getScheduler();
		scheduler.scheduleJob(job, cron);
		scheduler.start();
	}
}
