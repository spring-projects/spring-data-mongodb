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
package org.springframework.data.mongodb.core.script;

import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author Christoph Strobl
 */
public class CallableMongoScriptUnitTests {

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionWhenScriptNameIsNull() {
		new CallableMongoScript(null);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionWhenScriptNameIsEmptyString() {
		new CallableMongoScript("");
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionWhenRawScriptIsEmptyString() {
		new CallableMongoScript("foo", "");
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void getCodeShouldReturnCodeRepresentationOfRawScript() {

		String jsFunction = "function(x) { return x; }";

		CallableMongoScript script = new CallableMongoScript("echo", jsFunction);

		assertThat(script.getCode(), notNullValue());
		assertThat(script.getCode().toString(), equalTo(jsFunction));
	}

}
