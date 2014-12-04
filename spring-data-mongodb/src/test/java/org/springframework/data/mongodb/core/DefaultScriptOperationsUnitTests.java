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
package org.springframework.data.mongodb.core;

import static org.hamcrest.core.IsNull.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.script.CallableMongoScript;
import org.springframework.data.mongodb.core.script.ExecutableMongoScript;

/**
 * @author Christoph Strobl
 * @since 1.7
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultScriptOperationsUnitTests {

	DefaultScriptOperations scriptOps;
	@Mock MongoOperations mongoOperationsMock;

	@Before
	public void setUp() {
		this.scriptOps = new DefaultScriptOperations(mongoOperationsMock);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void saveShouldThrowExceptionWhenCalledWithNullValue() {
		scriptOps.register(null);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void saveShouldUseCorrectCollectionName() {

		scriptOps.register(new CallableMongoScript("foo", "function..."));

		verify(mongoOperationsMock, times(1)).save(any(CallableMongoScript.class), eq("system.js"));
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test
	public void saveShouldGenerateScriptNameForExecutableMongoScripts() {

		scriptOps.register(new ExecutableMongoScript("function..."));

		ArgumentCaptor<CallableMongoScript> captor = ArgumentCaptor.forClass(CallableMongoScript.class);

		verify(mongoOperationsMock, times(1)).save(captor.capture(), eq("system.js"));
		Assert.assertThat(captor.getValue().getName(), notNullValue());
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void executeShouldThrowExceptionWhenScriptIsNull() {
		scriptOps.execute(null);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void existsShouldThrowExceptionWhenScriptNameIsNull() {
		scriptOps.exists(null);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void existsShouldThrowExceptionWhenScriptNameIsEmpty() {
		scriptOps.exists("");
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void callShouldThrowExceptionWhenScriptNameIsNull() {
		scriptOps.call(null);
	}

	/**
	 * @see DATAMONGO-479
	 */
	@Test(expected = IllegalArgumentException.class)
	public void callShouldThrowExceptionWhenScriptNameIsEmpty() {
		scriptOps.call("");
	}

}
