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
package org.springframework.data.mongodb.core.script;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NamedMongoScript}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
public class NamedMongoScriptUnitTests {

	@Test // DATAMONGO-479
	public void shouldThrowExceptionWhenScriptNameIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> new NamedMongoScript(null, "return 1;"));
	}

	@Test // DATAMONGO-479
	public void shouldThrowExceptionWhenScriptNameIsEmptyString() {
		assertThatIllegalArgumentException().isThrownBy(() -> new NamedMongoScript("", "return 1"));
	}

	@Test // DATAMONGO-479
	public void shouldThrowExceptionWhenRawScriptIsEmptyString() {
		assertThatIllegalArgumentException().isThrownBy(() -> new NamedMongoScript("foo", ""));
	}

	@Test // DATAMONGO-479
	public void getCodeShouldReturnCodeRepresentationOfRawScript() {

		String jsFunction = "function(x) { return x; }";

		assertThat(new NamedMongoScript("echo", jsFunction).getCode()).isEqualTo(jsFunction);
	}
}
