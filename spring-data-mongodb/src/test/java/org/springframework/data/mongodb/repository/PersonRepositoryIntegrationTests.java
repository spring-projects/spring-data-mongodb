/*
 * Copyright 2010-2015 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;
import org.springframework.data.mongodb.repository.SampleEvaluationContextExtension.SampleSecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@ContextConfiguration
public class PersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	/**
	 * @see DATAMONGO-990 
	 */
	@Test
	public void shouldFindByFirstnameForSpELExpressionWithParameterIndexOnly() {

		List<Person> users = repository.findWithSpelByFirstnameForSpELExpressionWithParameterIndexOnly("Dave");

		assertThat(users, hasSize(1));
		assertThat(users.get(0), is(dave));
	}
	
	/**
	 * @see DATAMONGO-990 
	 */
	@Test
	public void shouldFindByFirstnameAndCurrentUserWithCustomQuery() {

		SampleSecurityContextHolder.getCurrent().setPrincipal(dave);
		List<Person> users = repository.findWithSpelByFirstnameAndCurrentUserWithCustomQuery("Dave");

		assertThat(users, hasSize(1));
		assertThat(users.get(0), is(dave));
	}
	
	/**
	 * @see DATAMONGO-990 
	 */
	@Test
	public void shouldFindByFirstnameForSpELExpressionWithParameterVariableOnly() {

		List<Person> users = repository.findWithSpelByFirstnameForSpELExpressionWithParameterVariableOnly("Dave");

		assertThat(users, hasSize(1));
		assertThat(users.get(0), is(dave));
	}	

}
