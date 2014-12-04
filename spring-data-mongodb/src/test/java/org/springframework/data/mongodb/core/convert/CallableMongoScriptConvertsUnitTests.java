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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import org.bson.types.Code;
import org.hamcrest.core.IsEqual;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.springframework.data.mongodb.core.convert.CallableMongoScriptConvertsUnitTests.CallableMongoScriptToDboConverterUnitTests;
import org.springframework.data.mongodb.core.convert.MongoConverters.CallableMongoScriptToDBObjectConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.DBObjectToCallableMongoScriptCoverter;
import org.springframework.data.mongodb.core.script.CallableMongoScript;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 */
@RunWith(Suite.class)
@SuiteClasses({ CallableMongoScriptToDboConverterUnitTests.class })
public class CallableMongoScriptConvertsUnitTests {

	static final String FUNCTION_NAME = "echo";
	static final String JS_FUNCTION = "function(x) { return x; }";
	static final CallableMongoScript ECHO_SCRIPT = new CallableMongoScript(FUNCTION_NAME, JS_FUNCTION);
	static final DBObject FUNCTION = new BasicDBObjectBuilder().add("_id", FUNCTION_NAME)
			.add("value", new Code(JS_FUNCTION)).get();

	/**
	 * @author Christoph Strobl
	 */
	public static class CallableMongoScriptToDboConverterUnitTests {

		CallableMongoScriptToDBObjectConverter converter = CallableMongoScriptToDBObjectConverter.INSTANCE;

		/**
		 * @see DATAMONGO-479
		 */
		@Test
		public void convertShouldReturnEmptyDboWhenScriptIsNull() {
			assertThat(converter.convert(null), IsEqual.<DBObject> equalTo(new BasicDBObject()));
		}

		/**
		 * @see DATAMONGO-479
		 */
		@Test
		public void convertShouldConvertScriptNameCorreclty() {

			DBObject dbo = converter.convert(ECHO_SCRIPT);

			Object id = dbo.get("_id");
			assertThat(id, instanceOf(String.class));
			assertThat(id, IsEqual.<Object> equalTo(FUNCTION_NAME));
		}

		/**
		 * @see DATAMONGO-479
		 */
		@Test
		public void convertShouldConvertScriptCodeCorreclty() {

			DBObject dbo = converter.convert(ECHO_SCRIPT);

			Object code = dbo.get("value");
			assertThat(code, instanceOf(Code.class));
			assertThat(code.toString(), equalTo(JS_FUNCTION));
		}

		/**
		 * @see DATAMONGO-479
		 */
		@Test
		public void convertShouldNotAddValueWhenCodeIsNull() {

			DBObject dbo = converter.convert(new CallableMongoScript("named"));

			assertThat(dbo.containsField("value"), is(false));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DboToCallableMongoScriptConverterUnitTests {

		DBObjectToCallableMongoScriptCoverter converter = DBObjectToCallableMongoScriptCoverter.INSTANCE;

		/**
		 * @see DATAMONGO-479
		 */
		@Test
		public void convertShouldReturnNullIfSourceIsNull() {
			assertThat(converter.convert(null), nullValue());
		}

		/**
		 * @see DATAMONGO-479
		 */
		@Test
		public void convertShouldConvertIdCorreclty() {

			CallableMongoScript script = converter.convert(FUNCTION);

			assertThat(script.getName(), equalTo(FUNCTION_NAME));
		}

		/**
		 * @see DATAMONGO-479
		 */
		@Test
		public void convertShouldConvertScriptValueCorreclty() {

			CallableMongoScript script = converter.convert(FUNCTION);

			assertThat(script.getCode(), notNullValue());
			assertThat(script.getCode().toString(), equalTo(JS_FUNCTION));
		}
	}

}
