/*
 * Copyright 2011-2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.QAddress;
import org.springframework.data.mongodb.repository.QPerson;

import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.BooleanOperation;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.SimplePath;
import com.querydsl.core.types.dsl.StringPath;

/**
 * Unit tests for {@link SpringDataMongodbSerializer}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
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

		Document document = serializer.asDocument("foo", address);

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

		Document result = (Document) serializer.visit((BooleanOperation) idPath.eq(id.toString()), null);
		assertThat(result.get("_id"), is(notNullValue()));
		assertThat(result.get("_id"), is(instanceOf(ObjectId.class)));
		assertThat(result.get("_id"), is(id));
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
		Document serialized = serializer.asDocument("_id", new Document("$ne", value.toString()));

		Document _id = getTypedValue(serialized, "_id", Document.class);
		ObjectId $ne = getTypedValue(_id, "$ne", ObjectId.class);
		assertThat($ne, is(value));
	}

	@Test // DATAMONGO-969
	public void shouldConvertCollectionOfObjectIdEvenWhenNestedInOperatorDocument() {

		ObjectId firstId = new ObjectId("53bb9fd14438765b29c2d56e");
		ObjectId secondId = new ObjectId("53bb9fda4438765b29c2d56f");

		List<Object> objectIds = new ArrayList<>();
		objectIds.add(firstId.toString());
		objectIds.add(secondId.toString());

		Document serialized = serializer.asDocument("_id", new Document("$in", objectIds));

		Document _id = getTypedValue(serialized, "_id", Document.class);
		List<Object> $in = getTypedValue(_id, "$in", List.class);

		assertThat($in, IsIterableContainingInOrder.<Object> contains(firstId, secondId));
	}

	@Test // DATAMONGO-1485
	public void takesCustomConversionForEnumsIntoAccount() {

		MongoMappingContext context = new MongoMappingContext();

		MappingMongoConverter converter = new MappingMongoConverter(dbFactory, context);
		converter.setCustomConversions(new MongoCustomConversions(Collections.singletonList(new SexTypeWriteConverter())));
		converter.afterPropertiesSet();

		this.converter = converter;
		this.serializer = new SpringDataMongodbSerializer(this.converter);

		Object mappedPredicate = this.serializer.handle(QPerson.person.sex.eq(Sex.FEMALE));

		assertThat(mappedPredicate, is(instanceOf(Document.class)));
		assertThat(((Document) mappedPredicate).get("sex"), is("f"));
	}

	@Test // DATAMONGO-1943
	@Ignore("FIXME mp911de")
	public void shouldRemarshallListsAndDocuments() {

		BooleanExpression criteria = QPerson.person.firstname.isNotEmpty()
				.and(QPerson.person.firstname.containsIgnoreCase("foo")).not();

		assertThat(this.serializer.handle(criteria),
				is(equalTo(Document.parse("{ \"$or\" : [ { \"firstname\" : { \"$not\" : { "
				+ "\"$ne\" : \"\"}}} , { \"firstname\" : { \"$not\" : { \"$regex\" : \".*\\\\Qfoo\\\\E.*\" , \"$options\" : \"i\"}}}]}"))));
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
