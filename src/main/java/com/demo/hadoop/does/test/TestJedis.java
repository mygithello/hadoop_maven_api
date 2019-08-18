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
package com.demo.hadoop.does.test;


import com.demo.hadoop.common.utils.JedisUtils;
import com.demo.hadoop.conf.SystemConfig;
import redis.clients.jedis.Jedis;

import java.util.Properties;

/**
 * Test Jedis.
 * 
 * @author smartloli.
 *
 *         Created by Jan 2, 2018
 */
public class TestJedis {
	public static void main(String[] args) {
		Jedis jedis = JedisUtils.getJedisInstance("com.demo.hadoop");
		System.out.println(jedis.get("20190816_other"));

		jedis.set("ceshi","123444");
	}
}
