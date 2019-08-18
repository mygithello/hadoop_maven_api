package com.demo.hadoop.does.kafka.executor;


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.demo.hadoop.common.utils.CalendarUtils;
import com.demo.hadoop.common.utils.JedisUtils;
import com.demo.hadoop.does.kafka.consant.JConstants.StormParam;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartloli.game.x.m.book_12.util.InetAddressUtils;

import redis.clients.jedis.Jedis;

/**
 * 实现IRichBolt接口, 减少编码过程.
 */
public class StatsBolts implements IRichBolt {

	/** 序列化ID. */
	private static final long serialVersionUID = 1L;
	/** 创建一个日志对象. */
	private Logger LOG = LoggerFactory.getLogger(StatsBolts.class);

	/** 输出收集器. */
	private OutputCollector collector;
	/** 计数器. */
	private Map<String, Integer> counter;

	@Override
	public void cleanup() {

	}

	/** 统计指标. */
	@Override
	public void execute(Tuple input) {
		String key = input.getString(0);
		// 启动统计指标
		if (!InetAddressUtils.isIPv4(key) && !key.contains(StormParam.UID)) {
			Integer integer = this.counter.get(StormParam.OTHER);
			if (integer != null) {
				integer += 1;
				this.counter.put(StormParam.OTHER, integer);
			} else {
				this.counter.put(StormParam.OTHER, 1);
			}
		}

		// 统计IP
		if (InetAddressUtils.isIPv4(key)) {
			Integer pvInt = this.counter.get(StormParam.PV);
			if (pvInt != null) {
				pvInt += 1;
				this.counter.put(StormParam.PV, pvInt);
			} else {
				this.counter.put(StormParam.PV, 1);
			}
		}

		// 统计用户ID
		if (key.contains(StormParam.UID)) {
			Integer uidVal = this.counter.get(StormParam.UID);
			if (uidVal != null) {
				uidVal += 1;
				this.counter.put(StormParam.UID, uidVal);
			} else {
				this.counter.put(StormParam.UID, 1);
			}
		}

		try {
			Jedis jedis = JedisUtils.getJedisInstance("com.demo.hadoop");
			for (Entry<String, Integer> entry : this.counter.entrySet()) {
				LOG.info("Bolt stats kpi is [" + entry.getKey() + "|" + entry.getValue().toString() + "]");
				// write result to redis
				jedis.set(CalendarUtils.today() + "_" + entry.getKey(), entry.getValue().toString());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			LOG.error("Jedis error, msg is " + ex.getMessage());
		}
		this.collector.ack(input);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
		this.counter = new HashMap<String, Integer>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
