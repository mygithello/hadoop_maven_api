
package com.demo.hadoop.common.kafka.api10;

import java.util.Date;
import java.util.Properties;

import com.demo.hadoop.conf.SystemConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka 0.10.x 版本生产者实现.
 * 
 * @author smartloli.
 *
 *         Created by Jan 5, 2018
 */
public class JProducers extends Thread {

	/** 创建一个配置对象实例. */
	private Properties props = new Properties();
	/** 声明生产者对象. */
	private Producer<String, String> producer = null;

	/** 构造函数初始化. */
	public JProducers() {
		props.put("bootstrap.servers", SystemConfig.getProperty("com.demo.hadoop.kafka.brokers")); // Kafka Brokers 信息
		props.put("acks", "1"); // 写入成功确认
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("partitioner.class", "com.demo.hadoop.common.kafka.api10.JPartitioner"); // 自定义分区实现类
		producer = new KafkaProducer<>(props);
	}

	/** 线程生产者业务逻辑实现. */
	@Override
	public void run() {
		for (int i = 0; i < 100; i++) {
			String json = "{\"id\":" + i + ",\"ip\":\"192.168.0." + i + "\",\"date\":" + new Date().toString() + "}";
			String k = "key" + i;
			producer.send(new ProducerRecord<String, String>("kv_test", k, json)); // 发送消息到指定的Topic
		}
		producer.close();// 生产者对象关闭
	}

	/** 开启生产者线程. */
	public static void main(String[] args) {
		JProducers producer = new JProducers(); // 创建一个生产者实例
		producer.start(); // 启动生产者线程
	}

}
