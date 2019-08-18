package com.demo.hadoop.does.kafka.server;


import java.util.Date;

import com.demo.hadoop.conf.SystemConfig;
import com.demo.hadoop.does.kafka.consant.JConstants;
import com.demo.hadoop.does.kafka.executor.KafkaSpout;
import com.demo.hadoop.does.kafka.executor.MessageBolts;
import com.demo.hadoop.does.kafka.executor.StatsBolts;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费Kafka集群中Topic的数据,提交任务到Storm集群.
 *
 */
public class KafkaTopology2 {
	private static Logger LOG = LoggerFactory.getLogger(KafkaTopology2.class);

	/** Storm集群通过Storm命令进行提交任务. */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout());
		builder.setBolt("bolts", new MessageBolts()).shuffleGrouping("spout");
		builder.setBolt("stats", new StatsBolts(), 2).fieldsGrouping("bolts", new Fields("attribute"));
		Config config = new Config();
		config.setDebug(true);
		if (JConstants.StormParam.CLUSTER.equals(SystemConfig.getProperty("com.demo.hadoop.storm.mode"))) {
			LOG.info("Cluster Mode Submit.");
			config.setNumWorkers(2);
			try {
				StormSubmitter.submitTopologyWithProgressBar(KafkaTopology.class.getSimpleName() + "_" + new Date().getTime(), config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LOG.info("Local Mode Submit.");
			config.setMaxTaskParallelism(1);
			LocalCluster local = new LocalCluster();
			local.submitTopology("stats", config, builder.createTopology());
			// try {
			// Thread.sleep(5000);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			// local.shutdown();
		}
	}
}
