package com.camilo.spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

public class TaxiRideStats extends KeyedProcessFunction<String, TaxiRide, Alert>{
	
	private static final long serialVersionUID = -1907693876949003097L;
	private transient ValueState<Double> totalAggregate;
	
	@Override
	public void processElement(TaxiRide ride,
					Context ctx,
					Collector<Alert> collector) throws Exception {
		
		if (totalAggregate.value() == null) {
			totalAggregate.update(new Double(0));
		}
		
		totalAggregate.update(totalAggregate.value() + ride.total);
		System.out.println(Thread.currentThread().getName() + " | Taxi Ride: " + ride.toString() + " ~ " + totalAggregate.value());
		
	}
	
	@Override
	public void open(Configuration params) {
		ValueStateDescriptor<Double> totalDescriptor = new ValueStateDescriptor<Double>(
				"account-aggregate", 
				Types.DOUBLE);
		totalAggregate = getRuntimeContext().getState(totalDescriptor);
	}
}
