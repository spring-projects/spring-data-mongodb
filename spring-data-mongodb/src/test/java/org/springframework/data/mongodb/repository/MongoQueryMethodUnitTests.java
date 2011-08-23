/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository;


import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.MongoRepositoryFactoryBean.EntityInformationCreator;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit test for {@link MongoQueryMethod}.
 *
 * @author Oliver Gierke
 */
public class MongoQueryMethodUnitTests {
	
	EntityInformationCreator creator;
	
	@Before
	public void setUp() {
		MongoMappingContext context = new MongoMappingContext();
		creator = new MongoRepositoryFactoryBean.EntityInformationCreator(context);
	}

	@Test
	public void detectsCollectionFromRepoTypeIfReturnTypeNotAssignable() throws Exception {
		
		Method method = SampleRepository.class.getMethod("method");
		
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(SampleRepository.class), creator);
		MongoEntityInformation<?,?> entityInformation = queryMethod.getEntityInformation();
		
		assertThat(entityInformation.getJavaType(), is(typeCompatibleWith(Address.class)));
		assertThat(entityInformation.getCollectionName(), is("contact"));
	}
	
	@Test
	public void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {
		
		Method method = SampleRepository2.class.getMethod("method");
		
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(SampleRepository.class), creator);
		MongoEntityInformation<?,?> entityInformation = queryMethod.getEntityInformation();
		
		assertThat(entityInformation.getJavaType(), is(typeCompatibleWith(Person.class)));
		assertThat(entityInformation.getCollectionName(), is("person"));
	}
	
	
	interface SampleRepository extends Repository<Contact, Long> {
		
		List<Address> method();
	}
	
	interface SampleRepository2 extends Repository<Contact, Long> {
		
		List<Person> method();
	}
}
