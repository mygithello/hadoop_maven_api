/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.demo.hadoop.common.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import com.demo.hadoop.conf.SystemConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wordcount的例子是一个比较经典的mapreduce例子，可以叫做Hadoop版的hello world。
 * 它将文件中的单词分割取出，然后shuffle，sort（map过程）。 
 * 接着进入到汇总统计 （reduce过程），最后写到hdfs中。
 * 该种配置需要在配置文件对应特定的active节点，可用性不高
 */

public class WordCount {

	private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

	/**
	 * 集成mapper类，实现map函数
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		/**
		 * LongWritable，IntWritable,Text,均是hadoop中实现的用于封装java数据类型的类
		 * 这些都实现了WriableComparable接口，能够被串行化，便于在分布式文件系统中做数据交换
		 * 可以分别视为long,int,String的替代品
		 */
		private final static IntWritable one = new IntWritable(1);
		//Text实现BubartConparable类，可以作为key值
		private Text word = new Text();

		/**
		 * 源文件：a b b
		 * map之后：
		 * a 1
		 * b 1
		 * b 1
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//Text的值转换为String
			StringTokenizer itr = new StringTokenizer(value.toString());	// 整行读取
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());								// 按空格分割单词
				context.write(word, one);								// 每次统计出来的单词+1
			}
		}
	}

	/**
	 * reduce之前：
	 * a 1
	 * b 1
	 * b 1
	 * reduce之后:
	 * a 1  ----reduce解析数据--- （a [1]）
	 * b 2  ----reduce解析数据---  (b [1,1])
	 */
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			/**
			 * 形成数据形式并存储
			 */
			for (IntWritable val : values) {
				sum += val.get();		// 分组累加//相同key进行累加聚合
			}
			result.set(sum);
			System.out.println(key+","+sum); // 按相同的key输出//输出当前key的累加值
			context.write(key, result);	  //Reduce后的结果，会写入分布式文件系统（HDFS）中
		}
	}

	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME","hadoop");
		try {
			if (args.length < 1) {
				LOG.info("args length is 0");
				run("test.txt");
			} else {
				run(args[0]);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private static void run(String name) throws Exception {
		Configuration conf = new Configuration();    //申请操作配置文件对象
		Job job = Job.getInstance(conf);				// 创建一个任务提交对象
		job.setJarByClass(WordCount.class);				//设置执行的jar类
		job.setMapperClass(TokenizerMapper.class);	// 指定Map计算的类
		job.setCombinerClass(IntSumReducer.class);	// 执行合并的类
		job.setReducerClass(IntSumReducer.class);	// 指定Reduce的类
		job.setOutputKeyClass(Text.class);			// 输出Key类型
		job.setOutputValueClass(IntWritable.class);	// 输出值类型

		// 设置统计文件在分布式文件系统中的路径
		String tmpLocalIn = SystemConfig.getProperty("com.demo.hadoop.standlone.input.path");
        System.out.println(tmpLocalIn);
		String inPath = String.format(tmpLocalIn, name);
		// 设置输出结果在分布式文件系统中的路径
		String tmpLocalOut = SystemConfig.getProperty("com.demo.hadoop.standlone.output.path");
		String outPath = String.format(tmpLocalOut, name);
        System.out.println(tmpLocalOut);

		FileInputFormat.addInputPath(job, new Path(inPath));		// 指定输入路径//该配置可以路径配置到配置文件中，便于动态修改
		FileOutputFormat.setOutputPath(job, new Path(outPath));	// 指定输出路径//该配置可以路径配置到配置文件中，便于动态修改

		int status = job.waitForCompletion(true) ? 0 : 1;

		System.exit(status);										// 执行完MR任务后退出应用
	}
}
