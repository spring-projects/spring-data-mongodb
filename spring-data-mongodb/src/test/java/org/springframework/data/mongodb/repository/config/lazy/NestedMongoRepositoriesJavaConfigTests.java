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
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.mongodb.repository.config.lazy.ClassWithNestedRepository.NestedUserRepository;
import org.springframework.data.repository.support.Repositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration test for the combination of JavaConfig and an {@link Repositories} wrapper.
 *
 * @author Thomas Darimont
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class NestedMongoRepositoriesJavaConfigTests {

	@Configuration
	@EnableMongoRepositories(considerNestedRepositories = true)
	@ImportResource("classpath:infrastructure.xml")
	static class Config {}

	@Autowired NestedUserRepository nestedUserRepository;

	@Test // DATAMONGO-780
	public void shouldSupportNestedRepositories() {
		assertThat(nestedUserRepository).isNotNull();
	}
}
