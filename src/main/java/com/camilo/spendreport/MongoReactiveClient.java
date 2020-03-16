package com.camilo.spendreport;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;

public class MongoReactiveClient {
	public static void main(String[] args) {
		MongoClient mongoClient = MongoClients.create(
		        MongoClientSettings.builder()
		                .applyToClusterSettings(builder ->
		                        builder.hosts(Arrays.asList(new ServerAddress("localhost", 27017))))
		                .build());
		
		/**
		 * DB: taxirides
		 * CL: driverTotalAggreates
		 */
		MongoDatabase database = mongoClient.getDatabase("taxirides");
		MongoCollection<Document> driverTotalAggregates = database.getCollection("driverTotalAggregates");
		
		Document doc = new Document("name", "MongoDB")
				.append("type", "database")
				.append("count", 1)
				.append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
				.append("info", new Document("x", 203)
				.append("y", 102));
		
		driverTotalAggregates.insertOne(doc);
	}
}
