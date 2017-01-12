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

import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 */
public class ExecutableMongoScriptUnitTests {

	public @Rule ExpectedException expectedException = ExpectedException.none();

	@Test // DATAMONGO-479
	public void constructorShouldThrowExceptionWhenRawScriptIsNull() {

		expectException(IllegalArgumentException.class, "must not be", "null");

		new ExecutableMongoScript(null);
	}

	@Test // DATAMONGO-479
	public void constructorShouldThrowExceptionWhenRawScriptIsEmpty() {

		expectException(IllegalArgumentException.class, "must not be", "empty");

		new ExecutableMongoScript("");
	}

	@Test // DATAMONGO-479
	public void getCodeShouldReturnCodeRepresentationOfRawScript() {

		String jsFunction = "function(x) { return x; }";

		ExecutableMongoScript script = new ExecutableMongoScript(jsFunction);

		assertThat(script.getCode(), notNullValue());
		assertThat(script.getCode().toString(), equalTo(jsFunction));
	}

	private void expectException(Class<?> type, String... messageFragments) {

		expectedException.expect(IllegalArgumentException.class);

		if (!ObjectUtils.isEmpty(messageFragments)) {
			for (String fragment : messageFragments) {
				expectedException.expectMessage(fragment);
			}
		}
	}

}
