/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Unit tests for {@link ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveMongoTemplateUnitTests {

	ReactiveMongoTemplate template;

	@Mock SimpleReactiveMongoDatabaseFactory factory;
	@Mock MongoClient mongoClient;

	MongoExceptionTranslator exceptionTranslator = new MongoExceptionTranslator();
	MappingMongoConverter converter;
	MongoMappingContext mappingContext;

	@Before
	public void setUp() {

		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslator);

		this.mappingContext = new MongoMappingContext();
		this.converter = new MappingMongoConverter(new NoOpDbRefResolver(), mappingContext);
		this.template = new ReactiveMongoTemplate(factory, converter);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1444
	public void rejectsNullDatabaseName() throws Exception {
		new ReactiveMongoTemplate(mongoClient, null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1444
	public void rejectsNullMongo() throws Exception {
		new ReactiveMongoTemplate(null, "database");
	}

	@Test // DATAMONGO-1444
	public void defaultsConverterToMappingMongoConverter() throws Exception {
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(mongoClient, "database");
		assertTrue(ReflectionTestUtils.getField(template, "mongoConverter") instanceof MappingMongoConverter);
	}

}
