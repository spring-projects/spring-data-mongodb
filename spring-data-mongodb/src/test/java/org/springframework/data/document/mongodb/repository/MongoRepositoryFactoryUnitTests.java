/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.document.mongodb.repository;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.User;
import org.springframework.data.document.mongodb.repository.MongoRepositoryFactoryBean.MongoRepositoryFactory;

/**
 * Unit test for {@link MongoRepositoryFactory}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoRepositoryFactoryUnitTests {
	
	@Mock
	MongoTemplate template;

	@Test(expected = IllegalArgumentException.class)
	public void rejectsInvalidIdType() throws Exception {
		MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
		factory.getRepository(SampleRepository.class);
	}
	
	private interface SampleRepository extends MongoRepository<User, Long> {
		
	}
}
