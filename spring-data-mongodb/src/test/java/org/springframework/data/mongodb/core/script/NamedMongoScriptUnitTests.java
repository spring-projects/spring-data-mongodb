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
package org.springframework.data.mongodb.core.script;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for {@link NamedMongoScript}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
public class NamedMongoScriptUnitTests {

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void shouldThrowExceptionWhenScriptNameIsNull() {
		new NamedMongoScript(null, "return 1;");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void shouldThrowExceptionWhenScriptNameIsEmptyString() {
		new NamedMongoScript("", "return 1");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void shouldThrowExceptionWhenRawScriptIsEmptyString() {
		new NamedMongoScript("foo", "");
	}

	@Test // DATAMONGO-479
	public void getCodeShouldReturnCodeRepresentationOfRawScript() {

		String jsFunction = "function(x) { return x; }";

		assertThat(new NamedMongoScript("echo", jsFunction).getCode(), is(jsFunction));
	}
}
