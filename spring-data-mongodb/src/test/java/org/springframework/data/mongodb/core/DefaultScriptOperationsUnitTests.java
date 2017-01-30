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
package org.springframework.data.mongodb.core;

import static org.hamcrest.core.IsNull.*;
import static org.mockito.Mockito.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.script.ExecutableMongoScript;
import org.springframework.data.mongodb.core.script.NamedMongoScript;

/**
 * Unit tests for {@link DefaultScriptOperations}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultScriptOperationsUnitTests {

	DefaultScriptOperations scriptOps;
	@Mock MongoOperations mongoOperations;

	@Before
	public void setUp() {
		this.scriptOps = new DefaultScriptOperations(mongoOperations);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void rejectsNullExecutableMongoScript() {
		scriptOps.register((ExecutableMongoScript) null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void rejectsNullNamedMongoScript() {
		scriptOps.register((NamedMongoScript) null);
	}

	@Test // DATAMONGO-479
	public void saveShouldUseCorrectCollectionName() {

		scriptOps.register(new NamedMongoScript("foo", "function..."));

		verify(mongoOperations, times(1)).save(any(NamedMongoScript.class), eq("system.js"));
	}

	@Test // DATAMONGO-479
	public void saveShouldGenerateScriptNameForExecutableMongoScripts() {

		scriptOps.register(new ExecutableMongoScript("function..."));

		ArgumentCaptor<NamedMongoScript> captor = ArgumentCaptor.forClass(NamedMongoScript.class);

		verify(mongoOperations, times(1)).save(captor.capture(), eq("system.js"));
		Assert.assertThat(captor.getValue().getName(), notNullValue());
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void executeShouldThrowExceptionWhenScriptIsNull() {
		scriptOps.execute(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void existsShouldThrowExceptionWhenScriptNameIsNull() {
		scriptOps.exists(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void existsShouldThrowExceptionWhenScriptNameIsEmpty() {
		scriptOps.exists("");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void callShouldThrowExceptionWhenScriptNameIsNull() {
		scriptOps.call(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-479
	public void callShouldThrowExceptionWhenScriptNameIsEmpty() {
		scriptOps.call("");
	}
}
