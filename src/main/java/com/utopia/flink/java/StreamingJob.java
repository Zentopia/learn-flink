/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.utopia.flink.java;

import com.alibaba.fastjson.JSONObject;
import com.utopia.flink.java.connector.kafka.KafkaUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		FlinkKafkaConsumer011<String> kafkaSource = KafkaUtils.createKafkaConsumer();
		DataStream<Tuple2<String, String>> dataStream = env
				.addSource(kafkaSource)
				.setParallelism(1)
				.map(e -> {
					System.out.println(e);
					return e;
				})
				.map(e -> new Tuple2<>("user_id", JSONObject.parseObject(e).getString("user_id")))
				.returns( TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}));

		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.10.24").setPort(6379).build();

		dataStream.addSink(new RedisSink<>(conf, new RedisExampleMapper()));

		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {

		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.HSET, "test_hash");
		}

		@Override
		public String getKeyFromData(Tuple2<String, String> data) {
			return data.f0;
		}

		@Override
		public String getValueFromData(Tuple2<String, String> data) {
			return data.f1;
		}
	}

}
