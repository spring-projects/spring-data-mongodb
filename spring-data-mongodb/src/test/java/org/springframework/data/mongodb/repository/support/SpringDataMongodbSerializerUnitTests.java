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
package org.springframework.data.mongodb.repository.support;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.QAddress;
import org.springframework.data.mongodb.repository.QPerson;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mysema.query.types.path.StringPath;

/**
 * Unit tests for {@link SpringDataMongodbSerializer}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class SpringDataMongodbSerializerUnitTests {

	@Mock
	MongoDbFactory dbFactory;
	MongoConverter converter;
	SpringDataMongodbSerializer serializer;

	@Before
	public void setUp() {
		MongoMappingContext context = new MongoMappingContext();
		converter = new MappingMongoConverter(dbFactory, context);
		serializer = new SpringDataMongodbSerializer(converter);
	}

	@Test
	public void uses_idAsKeyForIdProperty() {

		StringPath path = QPerson.person.id;
		assertThat(serializer.getKeyForPath(path, path.getMetadata()), is("_id"));
	}

	@Test
	public void buildsNestedKeyCorrectly() {
		StringPath path = QPerson.person.address.street;
		assertThat(serializer.getKeyForPath(path, path.getMetadata()), is("street"));
	}

	@Test
	public void convertsComplexObjectOnSerializing() {

		Address address = new Address();
		address.street = "Foo";
		address.zipCode = "01234";

		DBObject result = serializer.asDBObject("foo", address);
		assertThat(result, is(instanceOf(BasicDBObject.class)));
		BasicDBObject dbObject = (BasicDBObject) result;

		Object value = dbObject.get("foo");
		assertThat(value, is(notNullValue()));
		assertThat(value, is(instanceOf(BasicDBObject.class)));

		Object reference = converter.convertToMongoType(address);
		assertThat(value, is(reference));
	}

	/**
	 * @see DATAMONGO-376
	 */
	@Test
	public void returnsEmptyStringIfNoPathExpressionIsGiven() {

		QAddress address = QPerson.person.shippingAddresses.any();
		assertThat(serializer.getKeyForPath(address, address.getMetadata()), is(""));
	}

	class Address {
		String street;
		@Field("zip_code")
		String zipCode;
	}
}
