package com.camilo.spendreport;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoNonReactiveClient {
	public static void main(String[] args) {
		CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
		CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
		
		MongoClient mongoClient = MongoClients.create(
		        MongoClientSettings.builder()
								.applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress("localhost", 27017))))
								.build());

		
		/**
		 * DB: taxirides
		 * CL: driverTotalAggregates
		 */
		MongoDatabase database = mongoClient.getDatabase("taxirides");
		MongoCollection<TaxiRideStats> driverAggregatesColl = database.getCollection("driverTotalAggregates", TaxiRideStats.class);
		driverAggregatesColl = driverAggregatesColl.withCodecRegistry(codecRegistry);
		
		TaxiRideStats stat = new TaxiRideStats("XXXXXXXXXXXXXXXXXX", 323233.9);
		driverAggregatesColl.insertOne(stat);
		
		
		Block<TaxiRideStats> printBlock = new Block<TaxiRideStats>() {
			@Override
			public void apply(final TaxiRideStats stats) {
				System.out.println(stats);
			}
		};
		
		driverAggregatesColl.find().forEach(printBlock);
	}
}
