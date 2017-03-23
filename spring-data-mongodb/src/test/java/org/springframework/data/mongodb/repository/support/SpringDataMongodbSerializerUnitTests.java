/*
 * Copyright 2011-2017 the original author or authors.
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
import static org.springframework.data.mongodb.core.DocumentTestUtils.*;

import java.util.List;
import java.util.Collections;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Person.Sex;
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
		BasicDBObject document = (BasicDBObject) result;

		Object value = document.get("foo");
		assertThat(value, is(notNullValue()));
		assertThat(value, is(instanceOf(Document.class)));

		Object reference = converter.convertToMongoType(address);
		assertThat(value, is(reference));
	}

	@Test // DATAMONGO-376
	public void returnsEmptyStringIfNoPathExpressionIsGiven() {

		QAddress address = QPerson.person.shippingAddresses.any();
		assertThat(serializer.getKeyForPath(address, address.getMetadata()), is(""));
	}

	@Test // DATAMONGO-467
	public void convertsIdPropertyCorrectly() {

		ObjectId id = new ObjectId();

		PathBuilder<Address> builder = new PathBuilder<Address>(Address.class, "address");
		StringPath idPath = builder.getString("id");

		DBObject result = (DBObject) serializer.visit((BooleanOperation) idPath.eq(id.toString()), (Void) null);
		assertThat(result.get("_id"), is(notNullValue()));
		assertThat(result.get("_id"), is(instanceOf(ObjectId.class)));
		assertThat(result.get("_id"), is((Object) id));
	}

	@Test // DATAMONGO-761
	public void looksUpKeyForNonPropertyPath() {

		PathBuilder<Address> builder = new PathBuilder<Address>(Address.class, "address");
		SimplePath<Object> firstElementPath = builder.getArray("foo", String[].class).get(0);
		String path = serializer.getKeyForPath(firstElementPath, firstElementPath.getMetadata());

		assertThat(path, is("0"));
	}

	@Test // DATAMONGO-969
	public void shouldConvertObjectIdEvenWhenNestedInOperatorDbObject() {

		ObjectId value = new ObjectId("53bb9fd14438765b29c2d56e");
		DBObject serialized = serializer.asDBObject("_id", new Document("$ne", value.toString()));

		DBObject _id = getTypedValue(new Document(serialized.toMap()), "_id", DBObject.class);
		ObjectId $ne = getTypedValue(new Document(_id.toMap()), "$ne", ObjectId.class);
		assertThat($ne, is(value));
	}

	@Test // DATAMONGO-969
	public void shouldConvertCollectionOfObjectIdEvenWhenNestedInOperatorDocument() {

		ObjectId firstId = new ObjectId("53bb9fd14438765b29c2d56e");
		ObjectId secondId = new ObjectId("53bb9fda4438765b29c2d56f");

		BasicDBList objectIds = new BasicDBList();
		objectIds.add(firstId.toString());
		objectIds.add(secondId.toString());

		DBObject serialized = serializer.asDBObject("_id", new Document("$in", objectIds));

		DBObject _id = getTypedValue(new Document(serialized.toMap()), "_id", DBObject.class);
		List<Object> $in = getTypedValue(new Document(_id.toMap()), "$in", List.class);

		assertThat($in, IsIterableContainingInOrder.<Object> contains(firstId, secondId));
	}

	@Test // DATAMONGO-1485
	public void takesCustomConversionForEnumsIntoAccount() {

		MongoMappingContext context = new MongoMappingContext();

		MappingMongoConverter converter = new MappingMongoConverter(dbFactory, context);
		converter.setCustomConversions(new CustomConversions(Collections.singletonList(new SexTypeWriteConverter())));
		converter.afterPropertiesSet();

		this.converter = converter;
		this.serializer = new SpringDataMongodbSerializer(this.converter);

		Object mappedPredicate = this.serializer.handle(QPerson.person.sex.eq(Sex.FEMALE));

		assertThat(mappedPredicate, is(instanceOf(DBObject.class)));
		assertThat(((DBObject) mappedPredicate).get("sex"), is((Object) "f"));
	}

	class Address {
		String id;
		String street;
		@Field("zip_code") String zipCode;
		@Field("bar") String[] foo;
	}

	@WritingConverter
	public class SexTypeWriteConverter implements Converter<Sex, String> {

		@Override
		public String convert(Sex source) {

			if (source == null) {
				return null;
			}

			switch (source) {
				case MALE:
					return "m";
				case FEMALE:
					return "f";
				default:
					throw new IllegalArgumentException("o_O");
			}
		}
	}
}
