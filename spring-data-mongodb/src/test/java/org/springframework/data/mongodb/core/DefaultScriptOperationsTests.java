/*
 * Copyright 2014-2016 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.script.ExecutableMongoScript;
import org.springframework.data.mongodb.core.script.NamedMongoScript;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * Integration tests for {@link DefaultScriptOperations}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
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

	static final String JAVASCRIPT_COLLECTION_NAME = "system.js";
	static final String SCRIPT_NAME = "echo";
	static final String JS_FUNCTION = "function(x) { return x; }";
	static final ExecutableMongoScript EXECUTABLE_SCRIPT = new ExecutableMongoScript(JS_FUNCTION);
	static final NamedMongoScript CALLABLE_SCRIPT = new NamedMongoScript(SCRIPT_NAME, JS_FUNCTION);

	@Autowired MongoTemplate template;
	DefaultScriptOperations scriptOps;

	@Before
	public void setUp() {

		template.getCollection(JAVASCRIPT_COLLECTION_NAME).deleteMany(new Document());
		this.scriptOps = new DefaultScriptOperations(template);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void executeShouldDirectlyRunExecutableMongoScript() {
		assertThat(scriptOps.execute(EXECUTABLE_SCRIPT, 10), is((Object) 10D));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void saveShouldStoreCallableScriptCorrectly() {

		Query query = query(where("_id").is(SCRIPT_NAME));
		assumeThat(template.exists(query, JAVASCRIPT_COLLECTION_NAME), is(false));

		scriptOps.register(CALLABLE_SCRIPT);

		assumeThat(template.exists(query, JAVASCRIPT_COLLECTION_NAME), is(true));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void saveShouldStoreExecutableScriptCorrectly() {

		NamedMongoScript script = scriptOps.register(EXECUTABLE_SCRIPT);

		Query query = query(where("_id").is(script.getName()));
		assumeThat(template.exists(query, JAVASCRIPT_COLLECTION_NAME), is(true));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void executeShouldRunCallableScriptThatHasBeenSavedBefore() {

		scriptOps.register(CALLABLE_SCRIPT);

		Query query = query(where("_id").is(SCRIPT_NAME));
		assumeThat(template.exists(query, JAVASCRIPT_COLLECTION_NAME), is(true));

		Object result = scriptOps.call(CALLABLE_SCRIPT.getName(), 10);

		assertThat(result, is((Object) 10D));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void existsShouldReturnTrueIfScriptAvailableOnServer() {

		scriptOps.register(CALLABLE_SCRIPT);

		assertThat(scriptOps.exists(SCRIPT_NAME), is(true));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void existsShouldReturnFalseIfScriptNotAvailableOnServer() {
		assertThat(scriptOps.exists(SCRIPT_NAME), is(false));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void callShouldExecuteExistingScript() {

		scriptOps.register(CALLABLE_SCRIPT);

		Object result = scriptOps.call(SCRIPT_NAME, 10);

		assertThat(result, is((Object) 10D));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = UncategorizedDataAccessException.class)
	public void callShouldThrowExceptionWhenCallingScriptThatDoesNotExist() {
		scriptOps.call(SCRIPT_NAME, 10);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void scriptNamesShouldContainNameOfRegisteredScript() {

		scriptOps.register(CALLABLE_SCRIPT);

		assertThat(scriptOps.getScriptNames(), hasItems("echo"));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void scriptNamesShouldReturnEmptySetWhenNoScriptRegistered() {
		assertThat(scriptOps.getScriptNames(), is(empty()));
	}

	/**
	 * @see DATAMONGO-1465
	 */
	@Test
	public void executeShouldNotQuoteStrings() {
		assertThat(scriptOps.execute(EXECUTABLE_SCRIPT, "spring-data"), is((Object) "spring-data"));
	}
}
