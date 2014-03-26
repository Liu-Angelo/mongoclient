package com.aug3.storage.mongoclient;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.aug3.storage.mongoclient.config.MongoConfig;
import com.aug3.storage.mongoclient.exception.BadConfigException;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

/**
 * This MongoFactory is used to new a mongo instance.
 * 
 * If you want to keep only one mongo instance in jvm, please refer to
 * {MongoAdaptor.newMongoInstance()};
 * 
 * If you want to init mongo instance in your spring configure file, do it like
 * this:
 * 
 * <code>
 * <bean id="MongoFactory" class="com.aug3.storage.mongoclient.MongoFactory" />
 * <bean id="mongo" factory-bean="MongoFactory" factory-method="newMongoInstance" />
 * </code>
 * 
 * @author Roger.xia
 * 
 */
public class MongoFactory {

	private MongoConfig config = new MongoConfig();

	private Mongo mongo = null;

	private String serverAddressUrls = null;

	public MongoFactory() {

	}

	public MongoFactory(String serverAddress) {
		serverAddressUrls = serverAddress;
	}

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
	public Mongo newMongoInstance() {

		List<ServerAddress> seeds = getSeeds();

		MongoClientOptions options = null;

		if (config.getBooleanProperty("mongo.options.on", false)) {
			options = initMongoOptions();
		}

		if (seeds.size() > 1) {
			if (options == null)
				mongo = new MongoClient(seeds);
			else
				mongo = new MongoClient(seeds, options);
		} else {
			if (options == null)
				mongo = new MongoClient(seeds.get(0));
			else
				mongo = new MongoClient(seeds.get(0), options);
		}

		return mongo;
	}

	private List<ServerAddress> getSeeds() {
		String servers = null;
		if (serverAddressUrls == null) {
			servers = config.getProperty("mongo.db.address");
		} else {
			servers = serverAddressUrls;
		}
		if (servers != null && servers.length() > 0) {
			String[] serverArray = servers.split(",");
			String[] host_port = null;
			List<ServerAddress> seeds = new ArrayList<ServerAddress>();
			for (String server : serverArray) {
				host_port = server.split(":");
				try {
					if (host_port.length == 2) {
						seeds.add(new ServerAddress(host_port[0], Integer.parseInt(host_port[1])));
					} else {
						seeds.add(new ServerAddress(host_port[0], 27017));
					}
				} catch (NumberFormatException e) {
					throw new BadConfigException("Bad mongodb port defined : " + host_port[1]);
				} catch (UnknownHostException e) {
					throw new BadConfigException("Unknown mongodb host defined : " + host_port);
				}
			}
			return seeds;

		} else {
			throw new BadConfigException("No mongodb host defined!");
		}

	}

	private MongoClientOptions initMongoOptions() {

		MongoClientOptions.Builder builder = new MongoClientOptions.Builder()
				.socketKeepAlive(true)
				.autoConnectRetry(true)
				.cursorFinalizerEnabled(true)
				.connectionsPerHost(config.getIntProperty("mongo.options.connectionsPerHost", 10))
				.threadsAllowedToBlockForConnectionMultiplier(
						config.getIntProperty("mongo.options.threadsAllowedToBlockForConnectionMultiplier", 5))
				.connectTimeout(config.getIntProperty("mongo.options.connectTimeout", 10000))
				.socketTimeout(config.getIntProperty("mongo.options.socketTimeout", 0));

		/**
		 * By default, all read and write operations will be made on the
		 * primary, but it's possible to read from secondaries by changing the
		 * read preference:
		 **/
		if (config.getBooleanProperty("mongo.options.readReference.secondary", false)) {
			builder.readPreference(ReadPreference.secondaryPreferred());
		}

		return builder.build();

	}

}
