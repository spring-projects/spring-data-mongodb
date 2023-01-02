/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.custom;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration tests for custom reactive Repository implementations.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class CustomReactiveRepositoryImplementationTests {

	@Configuration
	@EnableReactiveMongoRepositories(includeFilters=@Filter(type = FilterType.ASSIGNABLE_TYPE, classes = CustomReactiveMongoRepository.class))
	@ImportResource("classpath:reactive-infrastructure.xml")
	static class Config {}

	@Autowired CustomReactiveMongoRepository customMongoRepository;

	@Test // DATAMONGO-1444
	public void shouldExecuteMethodOnCustomRepositoryImplementation() {

		String username = "bubu";
		List<User> users = customMongoRepository.findByUsernameCustom(username);

		assertThat(users.size()).isEqualTo(1);
		assertThat(users.get(0)).isNotNull();
		assertThat(users.get(0).getUsername()).isEqualTo(username);
	}
}
