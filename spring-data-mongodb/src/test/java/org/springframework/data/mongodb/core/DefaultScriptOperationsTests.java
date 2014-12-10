/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.script.CallableMongoScript;
import org.springframework.data.mongodb.core.script.ExecutableMongoScript;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class DefaultScriptOperationsTests {

	@Configuration
	static class Config {

		private static final String DB_NAME = "script-tests";

		@Bean
		public Mongo mongo() throws Exception {
			return new MongoClient();
		}

		@Bean
		public MongoTemplate template() throws Exception {
			return new MongoTemplate(mongo(), DB_NAME);
		}

	}

	static final String SCRIPT_NAME = "echo";
	static final String JS_FUNCTION = "function(x) { return x; }";
	static final ExecutableMongoScript EXECUTABLE_SCRIPT = new ExecutableMongoScript(JS_FUNCTION);
	static final CallableMongoScript CALLABLE_SCRIPT = new CallableMongoScript(SCRIPT_NAME, JS_FUNCTION);

	@Autowired MongoTemplate template;
	DefaultScriptOperations scriptOps;

	@Before
	public void setUp() {

		template.getCollection("system.js").remove(new BasicDBObject());
		this.scriptOps = new DefaultScriptOperations(template);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void executeShouldDirectlyRunExecutableMongoScript() {

		Object result = scriptOps.execute(EXECUTABLE_SCRIPT, 10);

		assertThat(result, Is.<Object> is(10D));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = DataAccessException.class)
	public void executeThowsDataAccessExceptionWhenRunningCallableScriptThatHasNotBeenSavedBefore() {
		scriptOps.execute(CALLABLE_SCRIPT, 10);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void saveShouldStoreCallableScriptCorrectly() {

		Query query = query(where("_id").is(SCRIPT_NAME));
		assumeThat(template.exists(query, "system.js"), is(false));

		scriptOps.save(CALLABLE_SCRIPT);

		assumeThat(template.exists(query, "system.js"), is(true));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void saveShouldStoreExecutableScriptCorrectly() {

		CallableMongoScript script = scriptOps.save(EXECUTABLE_SCRIPT);

		Query query = query(where("_id").is(script.getName()));
		assumeThat(template.exists(query, "system.js"), is(true));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void executeShouldRunCallableScriptThatHasBeenSavedBefore() {

		scriptOps.save(CALLABLE_SCRIPT);

		Query query = query(where("_id").is(SCRIPT_NAME));
		assumeThat(template.exists(query, "system.js"), is(true));

		Object result = scriptOps.execute(CALLABLE_SCRIPT, 10);

		assertThat(result, Is.<Object> is(10D));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void loadShouldFindScriptByItsName() {

		scriptOps.save(CALLABLE_SCRIPT);

		CallableMongoScript loaded = scriptOps.load(SCRIPT_NAME);

		assertThat(loaded.getName(), is(CALLABLE_SCRIPT.getName()));
		assertThat(loaded.getCode(), is(CALLABLE_SCRIPT.getCode()));
	}
}
