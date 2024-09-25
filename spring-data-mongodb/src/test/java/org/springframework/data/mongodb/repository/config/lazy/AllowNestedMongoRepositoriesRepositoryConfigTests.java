/*
 * Copyright 2013-2024 the original author or authors.
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
package org.springframework.data.mongodb.repository.config.lazy;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.repository.config.lazy.ClassWithNestedRepository.NestedUserRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration test for repository namespace configuration with nested repositories.
 *
 * @author Thomas Darimont
 */
@RunWith(SpringRunner.class)
@ContextConfiguration("AllowNestedMongoRepositoriesRepositoryConfigTests-context.xml")
public class AllowNestedMongoRepositoriesRepositoryConfigTests {

	@Autowired NestedUserRepository fooRepository;

	@Test // DATAMONGO-780
	public void shouldFindNestedRepository() {
		assertThat(fooRepository).isNotNull();
	}
}
