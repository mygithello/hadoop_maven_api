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
package com.demo.hadoop.common.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * HDFS java api
 */
public class HDFSUtil {

	private static Configuration conf = null;// 申明配置属性值对象

//	static {
//		conf = new Configuration();
//		// 指定hdfs的nameservice为cluster1,是NameNode的URI
//		conf.set("fs.defaultFS", "hdfs://cluster1");
//		// 指定hdfs的nameservice为cluster1
//		conf.set("dfs.nameservices", "cluster1");
//		// cluster1下面有两个NameNode，分别是nna节点和nns节点
//		conf.set("dfs.ha.namenodes.cluster1", "nna,nns");
//		// nna节点下的RPC通信地址
//		conf.set("dfs.namenode.rpc-address.cluster1.nna", "nna:9000");
//		// nns节点下的RPC通信地址
//		conf.set("dfs.namenode.rpc-address.cluster1.nns", "nns:9000");
//		// 实现故障自动转移方式
//		conf.set("dfs.client.failover.proxy.provider.cluster1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//	}

	static {
		conf = new Configuration();
		// 指定hdfs的nameservice为cluster1,是NameNode的URI
		conf.set("fs.defaultFS", "hdfs://nna:9000");
		// 指定hdfs的nameservice为cluster1
//		conf.set("dfs.nameservices", "cluster1");
//		// cluster1下面有两个NameNode，分别是nna节点和nns节点
//		conf.set("dfs.ha.namenodes.cluster1", "nna");
//		// nna节点下的RPC通信地址
//		conf.set("dfs.namenode.rpc-address.cluster1.nna", "nna:9000");
//		// nns节点下的RPC通信地址
////		conf.set("dfs.namenode.rpc-address.cluster1.nns", "nns:9000");
//		// 实现故障自动转移方式
//		conf.set("dfs.client.failover.proxy.provider.cluster1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
	}

	/**
	 * 上传
	 */
	public static void put(String remotePath, String localPath) throws IOException {
		FileSystem fs = FileSystem.get(conf); // 申明一个分布式文件系统对象
		Path src = new Path(localPath);
		Path dst = new Path(remotePath); // 得到操作分布式文件系统（HDFS）文件的路径对象
		fs.copyFromLocalFile(src, dst); //上传文件到目标服务器
		fs.close();  //关闭分布文件操作对象
	}

	/**
	 * 下载
	 */
	public static void get(String remotePath, String localPath) throws IOException {
		FileSystem fs = FileSystem.get(conf); // 申明一个分布式文件系统对象
		Path src = new Path(remotePath);
		Path dst = new Path(localPath); // 得到操作分布式文件系统（HDFS）文件的路径对象
		fs.copyToLocalFile(src, dst); //上传文件到目标服务器
		fs.close();  //关闭分布文件操作对象
	}

	/**
	 * 读取
	 */
	public static void cat(String remotePath) throws IOException {
		FileSystem fs = FileSystem.get(conf); // 申明一个分布式文件系统对象
		Path path = new Path(remotePath);
		if (fs.exists(path)) { //判断目标位置是否存在
			FSDataInputStream is = fs.open(path);  //打开分布式文件操作对象
			FileStatus status = fs.getFileStatus(path);//获取文件状态
			byte[] buffer = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
			is.readFully(0, buffer);
			is.close();
			is.close();
			System.out.println(buffer.toString()); //打印文件流中数据

		}
		fs.close();  //关闭分布文件操作对象
	}

	/**
	 * 删除
	 */
	public static void rmr(String remotePath) throws IOException {
		FileSystem fs = FileSystem.get(conf); // 申明一个分布式文件系统对象
		Path path = new Path(remotePath);
		fs.delete(path,true);
		fs.close();
	}


	/** 目录列表操作，展示分布式文件系统（HDFS）的目录结构 */
	public static void ls(String remotePath) throws IOException {
		FileSystem fs = FileSystem.get(conf); // 申明一个分布式文件系统对象
		Path path = new Path(remotePath); // 得到操作分布式文件系统（HDFS）文件的路径对象
		FileStatus[] status = fs.listStatus(path); // 得到文件状态数组
		Path[] listPaths = FileUtil.stat2Paths(status);
		for (Path p : listPaths) {
			System.out.println(p); // 循环打印目录结构
		}
	}

	/**
	 * 创建文件夹
	 */
	public static void mkdir(String remotePath) throws IOException {
		FileSystem fs = FileSystem.get(conf); // 申明一个分布式文件系统对象
		Path path = new Path(remotePath);
		fs.create(path);
		fs.close();
	}

}
