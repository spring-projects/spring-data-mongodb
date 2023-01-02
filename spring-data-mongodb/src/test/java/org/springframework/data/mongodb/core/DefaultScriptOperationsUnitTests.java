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
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.core.script.ExecutableMongoScript;
import org.springframework.data.mongodb.core.script.NamedMongoScript;

/**
 * Unit tests for {@link DefaultScriptOperations}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
@ExtendWith(MockitoExtension.class)
class DefaultScriptOperationsUnitTests {

	private DefaultScriptOperations scriptOps;
	@Mock MongoOperations mongoOperations;

	@BeforeEach
	void setUp() {
		this.scriptOps = new DefaultScriptOperations(mongoOperations);
	}

	@Test // DATAMONGO-479
	void rejectsNullExecutableMongoScript() {
		assertThatIllegalArgumentException().isThrownBy(() -> scriptOps.register((ExecutableMongoScript) null));
	}

	@Test // DATAMONGO-479
	void rejectsNullNamedMongoScript() {
		assertThatIllegalArgumentException().isThrownBy(() -> scriptOps.register((NamedMongoScript) null));
	}

	@Test // DATAMONGO-479
	void saveShouldUseCorrectCollectionName() {

		scriptOps.register(new NamedMongoScript("foo", "function..."));

		verify(mongoOperations, times(1)).save(any(NamedMongoScript.class), eq("system.js"));
	}

	@Test // DATAMONGO-479
	void saveShouldGenerateScriptNameForExecutableMongoScripts() {

		scriptOps.register(new ExecutableMongoScript("function..."));

		ArgumentCaptor<NamedMongoScript> captor = ArgumentCaptor.forClass(NamedMongoScript.class);

		verify(mongoOperations, times(1)).save(captor.capture(), eq("system.js"));
		assertThat(captor.getValue().getName()).isNotNull();
	}

	@Test // DATAMONGO-479
	void executeShouldThrowExceptionWhenScriptIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> scriptOps.execute(null));
	}

	@Test // DATAMONGO-479
	void existsShouldThrowExceptionWhenScriptNameIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> scriptOps.exists(null));
	}

	@Test // DATAMONGO-479
	void existsShouldThrowExceptionWhenScriptNameIsEmpty() {
		assertThatIllegalArgumentException().isThrownBy(() -> scriptOps.exists(""));
	}

	@Test // DATAMONGO-479
	void callShouldThrowExceptionWhenScriptNameIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> scriptOps.call(null));
	}

	@Test // DATAMONGO-479
	void callShouldThrowExceptionWhenScriptNameIsEmpty() {
		assertThatIllegalArgumentException().isThrownBy(() -> scriptOps.call(""));
	}
}
