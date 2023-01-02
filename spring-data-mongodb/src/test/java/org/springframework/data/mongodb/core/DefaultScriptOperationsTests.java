/*
 * Copyright 2014-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.script.ExecutableMongoScript;
import org.springframework.data.mongodb.core.script.NamedMongoScript;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for {@link DefaultScriptOperations}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@EnableIfMongoServerVersion(isLessThan = "4.1.0")
@ContextConfiguration
public class DefaultScriptOperationsTests {

	static @Client MongoClient mongoClient;

	@Configuration
	static class Config {

		private static final String DB_NAME = "script-tests";

		@Bean
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Bean
		public MongoTemplate template() throws Exception {
			return new MongoTemplate(mongoClient(), DB_NAME);
		}
	}

	static final String JAVASCRIPT_COLLECTION_NAME = "system.js";
	static final String SCRIPT_NAME = "echo";
	static final String JS_FUNCTION = "function(x) { return x; }";
	static final ExecutableMongoScript EXECUTABLE_SCRIPT = new ExecutableMongoScript(JS_FUNCTION);
	static final NamedMongoScript CALLABLE_SCRIPT = new NamedMongoScript(SCRIPT_NAME, JS_FUNCTION);

	@Autowired MongoTemplate template;
	DefaultScriptOperations scriptOps;

	@BeforeEach
	public void setUp() {

		template.getCollection(JAVASCRIPT_COLLECTION_NAME).deleteMany(new Document());
		this.scriptOps = new DefaultScriptOperations(template);
	}

	@Test // DATAMONGO-479
	public void executeShouldDirectlyRunExecutableMongoScript() {
		assertThat(scriptOps.execute(EXECUTABLE_SCRIPT, 10)).isEqualTo((Object) 10D);
	}

	@Test // DATAMONGO-479
	public void saveShouldStoreCallableScriptCorrectly() {

		Query query = query(where("_id").is(SCRIPT_NAME));
		assertThat(template.exists(query, JAVASCRIPT_COLLECTION_NAME)).isFalse();

		scriptOps.register(CALLABLE_SCRIPT);

		assertThat(template.exists(query, JAVASCRIPT_COLLECTION_NAME)).isTrue();
	}

	@Test // DATAMONGO-479
	public void saveShouldStoreExecutableScriptCorrectly() {

		NamedMongoScript script = scriptOps.register(EXECUTABLE_SCRIPT);

		Query query = query(where("_id").is(script.getName()));
		assertThat(template.exists(query, JAVASCRIPT_COLLECTION_NAME)).isTrue();
	}

	@Test // DATAMONGO-479
	public void executeShouldRunCallableScriptThatHasBeenSavedBefore() {

		scriptOps.register(CALLABLE_SCRIPT);

		Query query = query(where("_id").is(SCRIPT_NAME));
		assertThat(template.exists(query, JAVASCRIPT_COLLECTION_NAME)).isTrue();

		Object result = scriptOps.call(CALLABLE_SCRIPT.getName(), 10);

		assertThat(result).isEqualTo(10D);
	}

	@Test // DATAMONGO-479
	public void existsShouldReturnTrueIfScriptAvailableOnServer() {

		scriptOps.register(CALLABLE_SCRIPT);

		assertThat(scriptOps.exists(SCRIPT_NAME)).isTrue();
	}

	@Test // DATAMONGO-479
	public void existsShouldReturnFalseIfScriptNotAvailableOnServer() {
		assertThat(scriptOps.exists(SCRIPT_NAME)).isFalse();
	}

	@Test // DATAMONGO-479
	public void callShouldExecuteExistingScript() {

		scriptOps.register(CALLABLE_SCRIPT);

		Object result = scriptOps.call(SCRIPT_NAME, 10);

		assertThat(result).isEqualTo((Object) 10D);
	}

	@Test // DATAMONGO-479
	public void callShouldThrowExceptionWhenCallingScriptThatDoesNotExist() {
		assertThatExceptionOfType(UncategorizedDataAccessException.class).isThrownBy(() -> scriptOps.call(SCRIPT_NAME, 10));
	}

	@Test // DATAMONGO-479
	public void scriptNamesShouldContainNameOfRegisteredScript() {

		scriptOps.register(CALLABLE_SCRIPT);

		assertThat(scriptOps.getScriptNames()).contains("echo");
	}

	@Test // DATAMONGO-479
	public void scriptNamesShouldReturnEmptySetWhenNoScriptRegistered() {
		assertThat(scriptOps.getScriptNames()).isEmpty();
	}

	@Test // DATAMONGO-1465
	public void executeShouldNotQuoteStrings() {
		assertThat(scriptOps.execute(EXECUTABLE_SCRIPT, "spring-data")).isEqualTo((Object) "spring-data");
	}
}
