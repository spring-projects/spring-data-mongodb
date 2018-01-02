/*
 * Copyright 2013-2018 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@ContextConfiguration("config/MongoNamespaceIntegrationTests-context.xml")
public class RedeclaringRepositoryMethodsTests extends AbstractPersonRepositoryIntegrationTests {

	@Autowired RedeclaringRepositoryMethodsRepository repository;

	@Test // DATAMONGO-760
	public void adjustedWellKnownPagedFindAllMethodShouldReturnOnlyTheUserWithFirstnameOliverAugust() {

		Page<Person> page = repository.findAll(PageRequest.of(0, 2));

		assertThat(page.getNumberOfElements(), is(1));
		assertThat(page.getContent().get(0).getFirstname(), is(oliver.getFirstname()));
	}

	@Test // DATAMONGO-760
	public void adjustedWllKnownFindAllMethodShouldReturnAnEmptyList() {

		List<Person> result = repository.findAll();

		assertThat(result.isEmpty(), is(true));
	}
}
