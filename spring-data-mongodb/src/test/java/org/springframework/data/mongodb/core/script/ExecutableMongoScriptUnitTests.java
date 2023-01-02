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
 * @author Christoph Strobl
 */
class ExecutableMongoScriptUnitTests {

	@Test // DATAMONGO-479
	void constructorShouldThrowExceptionWhenRawScriptIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ExecutableMongoScript(null))
				.withMessageContaining("must not be").withMessageContaining("null");
	}

	@Test // DATAMONGO-479
	void constructorShouldThrowExceptionWhenRawScriptIsEmpty() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ExecutableMongoScript(""))
				.withMessageContaining("must not be").withMessageContaining("empty");
	}

	@Test // DATAMONGO-479
	void getCodeShouldReturnCodeRepresentationOfRawScript() {

		String jsFunction = "function(x) { return x; }";

		ExecutableMongoScript script = new ExecutableMongoScript(jsFunction);

		assertThat(script.getCode()).isNotNull().hasToString(jsFunction);
	}
}
