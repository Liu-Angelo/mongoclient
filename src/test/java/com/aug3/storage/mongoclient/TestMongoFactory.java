package com.aug3.storage.mongoclient;

import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class TestMongoFactory {

	@Test
	@Ignore
	public void test() {

		MongoFactory mf = new MongoFactory("54.251.56.169:27017,54.251.56.170:27017,54.251.56.179:27017");
		Mongo m = mf.newMongoInstance();
		DB db = m.getDB("ada");
		DBObject dbObj = new BasicDBObject();
		dbObj.put("name", "testbyroger");
		dbObj.put("value", 1000l);
		db.getCollection("ids").save(dbObj);
	}

}
