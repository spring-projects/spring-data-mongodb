/*
 * Copyright 2011-2014 the original author or authors.
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
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;

import org.bson.types.ObjectId;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.QAddress;
import org.springframework.data.mongodb.repository.QPerson;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.querydsl.core.types.dsl.BooleanOperation;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.SimplePath;
import com.querydsl.core.types.dsl.StringPath;

/**
 * Unit tests for {@link SpringDataMongodbSerializer}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class SpringDataMongodbSerializerUnitTests {

	@Mock DbRefResolver dbFactory;
	MongoConverter converter;
	SpringDataMongodbSerializer serializer;

	@Before
	public void setUp() {

		MongoMappingContext context = new MongoMappingContext();

		this.converter = new MappingMongoConverter(dbFactory, context);
		this.serializer = new SpringDataMongodbSerializer(converter);
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

	/**
	 * @see DATAMONGO-467
	 */
	@Test
	public void convertsIdPropertyCorrectly() {

		ObjectId id = new ObjectId();

		PathBuilder<Address> builder = new PathBuilder<Address>(Address.class, "address");
		StringPath idPath = builder.getString("id");

		DBObject result = (DBObject) serializer.visit((BooleanOperation) idPath.eq(id.toString()), (Void) null);
		assertThat(result.get("_id"), is(notNullValue()));
		assertThat(result.get("_id"), is(instanceOf(ObjectId.class)));
		assertThat(result.get("_id"), is((Object) id));
	}

	/**
	 * @see DATAMONGO-761
	 */
	@Test
	public void looksUpKeyForNonPropertyPath() {

		PathBuilder<Address> builder = new PathBuilder<Address>(Address.class, "address");
		SimplePath<Object> firstElementPath = builder.getArray("foo", String[].class).get(0);
		String path = serializer.getKeyForPath(firstElementPath, firstElementPath.getMetadata());

		assertThat(path, is("0"));
	}

	/**
	 * @see DATAMONGO-969
	 */
	@Test
	public void shouldConvertObjectIdEvenWhenNestedInOperatorDbObject() {

		ObjectId value = new ObjectId("53bb9fd14438765b29c2d56e");
		DBObject serialized = serializer.asDBObject("_id", new BasicDBObject("$ne", value.toString()));

		DBObject _id = getAsDBObject(serialized, "_id");
		ObjectId $ne = getTypedValue(_id, "$ne", ObjectId.class);
		assertThat($ne, is(value));
	}

	/**
	 * @see DATAMONGO-969
	 */
	@Test
	public void shouldConvertCollectionOfObjectIdEvenWhenNestedInOperatorDbObject() {

		ObjectId firstId = new ObjectId("53bb9fd14438765b29c2d56e");
		ObjectId secondId = new ObjectId("53bb9fda4438765b29c2d56f");

		BasicDBList objectIds = new BasicDBList();
		objectIds.add(firstId.toString());
		objectIds.add(secondId.toString());

		DBObject serialized = serializer.asDBObject("_id", new BasicDBObject("$in", objectIds));

		DBObject _id = getAsDBObject(serialized, "_id");
		Object[] $in = getTypedValue(_id, "$in", Object[].class);

		assertThat($in, Matchers.<Object> arrayContaining(firstId, secondId));
	}

	class Address {
		String id;
		String street;
		@Field("zip_code") String zipCode;
		@Field("bar") String[] foo;
	}
}
