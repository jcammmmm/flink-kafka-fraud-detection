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

package com.camilo.spendreport;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		// properties.setProperty("group.id", "pokemon");
		
		// KafkaStream
		DataStream<String> streamData = env
			.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));
		
		// Translating the stream to TaxiRide objects
		DataStream<TaxiRide> taxiRideStream = streamData
				.map(new MapFunction<String, TaxiRide>() {
					private static final long serialVersionUID = 1L;

					@Override
					public TaxiRide map(String line) throws Exception {
						return TaxiRide.fromString(line);
					}
				});
		
		DataStream<Alert> taxiRideAlerts = taxiRideStream
				.keyBy(TaxiRide::getLicenseId)
				.process(new TaxiRideProcessor())
				.name("taxi-ride-stats");
		
		taxiRideAlerts
			.addSink(new AlertSink())
			.name("taxi-ride-alerts");
		
		/*
		DataStream<Transaction> transactions = env
			.addSource(new TransactionSource())
			.name("transactions");

		DataStream<Alert> alerts = transactions
			.keyBy(Transaction::getAccountId)
			.process(new FraudDetector())
			.name("fraud-detector");

		alerts
			.addSink(new AlertSink())
			.name("send-alerts");
		*/
		
		env.execute("Taxi Ride Analytics");
	}
}
