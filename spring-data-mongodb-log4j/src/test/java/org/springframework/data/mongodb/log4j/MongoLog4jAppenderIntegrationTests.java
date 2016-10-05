/*
 * Copyright 2011-2016 the original author or authors.
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

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

/**
 * Integration tests for {@link MongoLog4jAppender}.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class MongoLog4jAppenderIntegrationTests {

	static final String NAME = MongoLog4jAppenderIntegrationTests.class.getName();

	private static final Logger log = Logger.getLogger(NAME);
	MongoClient mongo;
	DB db;
	String collection;

	@Before
	public void setUp() throws Exception {

		mongo = new MongoClient("localhost", 27017);
		db = mongo.getDB("logs");

		Calendar now = Calendar.getInstance();
		collection = String.valueOf(now.get(Calendar.YEAR)) + String.format("%1$02d", now.get(Calendar.MONTH) + 1);
	}

	@After
	public void tearDown() {
		db.getCollection(collection).remove(new BasicDBObject());
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
