/*
 * Copyright 2014-2017 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.bson.types.Code;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.convert.MongoConverters.DBObjectToNamedMongoScriptCoverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.NamedMongoScriptToDBObjectConverter;
import org.springframework.data.mongodb.core.convert.NamedMongoScriptConvertsUnitTests.DboToNamedMongoScriptConverterUnitTests;
import org.springframework.data.mongodb.core.convert.NamedMongoScriptConvertsUnitTests.NamedMongoScriptToDboConverterUnitTests;
import org.springframework.data.mongodb.core.script.NamedMongoScript;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link Converter} implementations for {@link NamedMongoScript}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
@RunWith(Suite.class)
@SuiteClasses({ NamedMongoScriptToDboConverterUnitTests.class, DboToNamedMongoScriptConverterUnitTests.class })
public class NamedMongoScriptConvertsUnitTests {

	static final String FUNCTION_NAME = "echo";
	static final String JS_FUNCTION = "function(x) { return x; }";
	static final NamedMongoScript ECHO_SCRIPT = new NamedMongoScript(FUNCTION_NAME, JS_FUNCTION);
	static final DBObject FUNCTION = new BasicDBObjectBuilder().add("_id", FUNCTION_NAME)
			.add("value", new Code(JS_FUNCTION)).get();

	/**
	 * @author Christoph Strobl
	 */
	public static class NamedMongoScriptToDboConverterUnitTests {

		NamedMongoScriptToDBObjectConverter converter = NamedMongoScriptToDBObjectConverter.INSTANCE;

		@Test // DATAMONGO-479
		public void convertShouldReturnEmptyDboWhenScriptIsNull() {
			assertThat(converter.convert(null), is((DBObject) new BasicDBObject()));
		}

		@Test // DATAMONGO-479
		public void convertShouldConvertScriptNameCorreclty() {

			DBObject dbo = converter.convert(ECHO_SCRIPT);

			Object id = dbo.get("_id");
			assertThat(id, is(instanceOf(String.class)));
			assertThat(id, is((Object) FUNCTION_NAME));
		}

		@Test // DATAMONGO-479
		public void convertShouldConvertScriptCodeCorreclty() {

			DBObject dbo = converter.convert(ECHO_SCRIPT);

			Object code = dbo.get("value");
			assertThat(code, is(instanceOf(Code.class)));
			assertThat(code, is((Object) new Code(JS_FUNCTION)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DboToNamedMongoScriptConverterUnitTests {

		DBObjectToNamedMongoScriptCoverter converter = DBObjectToNamedMongoScriptCoverter.INSTANCE;

		@Test // DATAMONGO-479
		public void convertShouldReturnNullIfSourceIsNull() {
			assertThat(converter.convert(null), is(nullValue()));
		}

		@Test // DATAMONGO-479
		public void convertShouldConvertIdCorreclty() {

			NamedMongoScript script = converter.convert(FUNCTION);

			assertThat(script.getName(), is(FUNCTION_NAME));
		}

		@Test // DATAMONGO-479
		public void convertShouldConvertScriptValueCorreclty() {

			NamedMongoScript script = converter.convert(FUNCTION);

			assertThat(script.getCode(), is(notNullValue()));
			assertThat(script.getCode(), is(JS_FUNCTION));
		}
	}

}
