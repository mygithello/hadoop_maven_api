package com.demo.hadoop.does.kafka.executor;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 实现IRichBolt接口,做数据预处理操作.
 */
public class MessageBolts implements IRichBolt {

	/** 序列化ID. */
	private static final long serialVersionUID = 1L;

	/** 输出收集器. */
	private OutputCollector collector;

	@Override
	public void cleanup() {

	}

	/** 预处理数据. */
	@Override
	public void execute(Tuple input) {
		JSONObject json = JSON.parseObject(input.getString(0));
		String tm = json.getString("_tm");
		String uid = json.getString("uid");
		String plat = json.getString("plat");
		String ip = json.getString("ip");

		String[] line = new String[] { "uid_" + uid, plat, ip, tm };
		for (int i = 0; i < line.length; i++) {
			List<Tuple> a = new ArrayList<Tuple>();
			a.add(input);
			switch (i) {
			case 0:// uid
				this.collector.emit(a, new Values(line[i]));
				break;
			case 1:// plat
				this.collector.emit(a, new Values(line[i]));
				break;
			case 2:// ip
				this.collector.emit(a, new Values(line[i]));
				break;
			case 3:// tm
				this.collector.emit(a, new Values(line[i]));
				break;
			default:
				break;
			}
		}
		this.collector.ack(input);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("attribute"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
