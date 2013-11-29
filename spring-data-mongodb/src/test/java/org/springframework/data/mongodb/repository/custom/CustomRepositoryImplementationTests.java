/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.repository.custom;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.mongodb.repository.User;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for custom Repository implementations.
 * 
 * @author Thomas Darimont
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CustomRepositoryImplementationTests {

	@Configuration
	@EnableMongoRepositories
	@ImportResource("classpath:infrastructure.xml")
	static class Config {}

	@Autowired CustomMongoRepository customMongoRepository;

	/**
	 * @see DATAMONGO-804
	 */
	@Test
	public void shouldExecuteMethodOnCustomRepositoryImplementation() {

		String username = "bubu";
		List<User> users = customMongoRepository.findByUsernameCustom(username);

		assertThat(users.size(), is(1));
		assertThat(users.get(0), is(notNullValue()));
		assertThat(users.get(0).getUsername(), is(username));
	}
}
