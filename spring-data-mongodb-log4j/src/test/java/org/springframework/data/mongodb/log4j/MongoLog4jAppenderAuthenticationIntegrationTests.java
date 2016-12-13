/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.log4j;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Collections;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

/**
 * Integration tests for {@link MongoLog4jAppender} using authentication.
 *
 * @author Mark Paluch
 */
public class MongoLog4jAppenderAuthenticationIntegrationTests {

	private final static String username = "admin";
	private final static String password = "test";
	private final static String authenticationDatabase = "logs";

	MongoClient mongo;
	DB db;
	String collection;
	ServerAddress serverLocation;

	Logger log;

	@Before
	public void setUp() throws Exception {

		serverLocation = new ServerAddress("localhost", 27017);

		mongo = new MongoClient(serverLocation);
		db = mongo.getDB("logs");

		BasicDBList roles = new BasicDBList();
		roles.add("dbOwner");
		db.command(new BasicDBObjectBuilder().add("createUser", username).add("pwd", password).add("roles", roles).get());
		mongo.close();

		mongo = new MongoClient(serverLocation, Collections
				.singletonList(MongoCredential.createCredential(username, authenticationDatabase, password.toCharArray())));
		db = mongo.getDB("logs");

		Calendar now = Calendar.getInstance();
		collection = String.valueOf(now.get(Calendar.YEAR)) + String.format("%1$02d", now.get(Calendar.MONTH) + 1);

		LogManager.resetConfiguration();
		PropertyConfigurator.configure(getClass().getResource("/log4j-with-authentication.properties"));

		log = Logger.getLogger(MongoLog4jAppenderIntegrationTests.class.getName());
	}

	@After
	public void tearDown() {

		if (db != null) {
			db.getCollection(collection).remove(new BasicDBObject());
			db.command(new BasicDBObject("dropUser", username));
		}

		LogManager.resetConfiguration();
		PropertyConfigurator.configure(getClass().getResource("/log4j.properties"));
	}

	@Test
	public void testLogging() {

		log.debug("DEBUG message");
		log.info("INFO message");
		log.warn("WARN message");
		log.error("ERROR message");

		DBCursor msgs = db.getCollection(collection).find();
		assertThat(msgs.count(), is(4));
	}

	@Test
	public void testProperties() {
		MDC.put("property", "one");
		log.debug("DEBUG message");
	}
}
