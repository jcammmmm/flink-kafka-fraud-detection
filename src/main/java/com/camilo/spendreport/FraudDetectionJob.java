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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * Skeleton code for the datastream walkthrough
 */
/**
 * For those of you not already familiar with the Lambda Architecture, the basic idea is 
 * that you run a streaming system alongside a batch system, both performing essentially 
 * the same calculation. The streaming system gives you low-latency, inaccurate results 
 * (either because of the use of an approximation algorithm, or because the streaming 
 * system itself does not provide correctness), and some time later a batch system rolls 
 * along and provides you with correct output. Originally proposed by Twitter’s Nathan Marz 
 * (creator of Storm), it ended up being quite successful because it was, in fact, a 
 * fantastic idea for the time; streaming engines were a bit of a letdown in the correctness 
 * department, and batch engines were as inherently unwieldy as you’d expect, so Lambda 
 * gave you a way to have your proverbial cake and eat it, too. Unfortunately, maintaining 
 * a Lambda system is a hassle: you need to build, provision, and maintain two independent 
 * versions of your pipeline, and then also somehow merge the results from the two pipelines 
 * at the end.
 * @from: https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/
 *
 */
public class FraudDetectionJob {
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // Processing time refers to the system time of the machine that is executing the respective operation.
		
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
		
		/*
		// Applying window streaming
		DataStream<TaxiRide> reducedStream = taxiRideStream
				.keyBy(TaxiRide::getLicenseId)
				.window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
				.reduce(
					new ReduceFunction<TaxiRide>() {

						private static final long serialVersionUID = 2179158058383632366L;

						@Override
						public TaxiRide reduce(TaxiRide tr1, TaxiRide tr2) throws Exception {
							tr1.total += tr2.total;
							return tr1;
						}
						
					}
				);
				
		*/
		
		DataStream<TaxiRideStats> taxiRideStatsStream = taxiRideStream
				.keyBy(TaxiRide::getLicenseId)
				.window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
				.aggregate(new AverageIncomeRide());
		
		taxiRideStatsStream.print();
		
		/*
		DataStream<TaxiRide> filteredStream = reducedStream
				
				.filter(new FilterFunction<TaxiRide>() {

					private static final long serialVersionUID = -3802700265716996691L;

					@Override
					public boolean filter(TaxiRide value) throws Exception {
						if (value.licenseId.equals("E7750A37CAB07D0DFF0AF7E3573AC141") ||
							value.licenseId.equals("3FF2709163DE7036FCAA4E5A3324E4BF"))
							return true;
						else
							return false;
					}


				});

		filteredStream.print();
		
		*/
				
				
		/*
		.process(new TaxiRideProcessor())
		.name("taxi-ride-stats");
		*/
		
		
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

	
	private static class AverageIncomeRide implements AggregateFunction<TaxiRide, TaxiRideStats, TaxiRideStats> {

		private static final long serialVersionUID = 2321231297871023309L;

		@Override
		public TaxiRideStats createAccumulator() {
			return new TaxiRideStats();
		}

		@Override
		public TaxiRideStats add(TaxiRide ride, TaxiRideStats accumulator) {
			if (accumulator.getDriverId().equals(""))
				accumulator.setDriverId(ride.licenseId);

			accumulator.updateTotalAggregate(ride.total);
				
			return accumulator;
		}

		@Override
		public TaxiRideStats getResult(TaxiRideStats accumulator) {
			return accumulator;
		}

		@Override
		public TaxiRideStats merge(TaxiRideStats a, TaxiRideStats b) {
			a.setTotalAggregate(a.getTotalAggregate() + b.getTotalAggregate());
			return a;
		}
	}
}
