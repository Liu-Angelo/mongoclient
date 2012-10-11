package com.aug3.storage.mongoclient;

import com.aug3.storage.mongoclient.config.MongoConfig;
import com.aug3.storage.mongoclient.exception.BadConfigException;
import com.mongodb.DB;
import com.mongodb.Mongo;

public class MongoAdaptor {

	private static Mongo mongo = null;
	private static MongoConfig config = new MongoConfig();

	/**
	 * Creates a Mongo based on a list of replica set members or a list of
	 * mongos. If you have a standalone server, it will use the
	 * Mongo(ServerAddress) constructor.
	 * 
	 * @return Mongo A database connection with internal connection pooling. For
	 *         most applications, you should have one Mongo instance for the
	 *         entire JVM.
	 * 
	 * @throws BadConfigException
	 */
	public synchronized static Mongo newMongoInstance() {

		if (mongo == null) {
			mongo = new MongoFactory().newMongoInstance();
		}

		return mongo;

	}

	public static DB getDB(String dbname) {

		if (mongo == null) {
			newMongoInstance();
		}

		return mongo.getDB(dbname);

	}

	public static DB getDB() {

		if (mongo == null) {
			newMongoInstance();
		}

		String dbName = config.getProperty("mongo.db.name", "ada");
		return mongo.getDB(dbName);

	}

	/**
	 * closes the underlying connector, which in turn closes all open
	 * connections. Once called, this Mongo instance can no longer be used.
	 * 
	 * we close this for now as it will get developers confused.
	 */
	// public static void close() {
	// if (mongo != null) {
	// mongo.close();
	// }
	// }

}
