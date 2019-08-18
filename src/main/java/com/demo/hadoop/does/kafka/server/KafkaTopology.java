package com.demo.hadoop.does.kafka.server;


import java.util.Arrays;
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
public class KafkaTopology {
	private static Logger LOG = LoggerFactory.getLogger(KafkaTopology.class);

	/** 采用Storm Thrift接口进行提交任务. */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout());
		builder.setBolt("bolts", new MessageBolts()).shuffleGrouping("spout");
		builder.setBolt("stats", new StatsBolts(), 2).fieldsGrouping("bolts", new Fields("attribute"));
		Config config = new Config();
		config.setDebug(true);
		if (JConstants.StormParam.CLUSTER.equals(SystemConfig.getProperty("com.demo.hadoop.storm.mode"))) {
			LOG.info("Cluster Mode Submit.");

			String path = SystemConfig.getProperty("com.demo.hadoop.storm.jar.path");
			config.put(Config.NIMBUS_SEEDS, Arrays.asList("dn1")); //
			config.put(Config.NIMBUS_THRIFT_PORT, 6627);
			config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("dn1", "dn2", "dn3"));
			config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
			config.setNumWorkers(2);

			System.setProperty("storm.jar", path);

			try {
				StormSubmitter.submitTopology(KafkaTopology.class.getSimpleName() + "_" + new Date().getTime(), config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LOG.info("Local Mode Submit.");
			config.setMaxTaskParallelism(1);
			LocalCluster local = new LocalCluster();
			local.submitTopology("stats", config, builder.createTopology());
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			local.shutdown();
		}
	}
}
