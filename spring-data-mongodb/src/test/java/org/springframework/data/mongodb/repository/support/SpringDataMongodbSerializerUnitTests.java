/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static com.querydsl.core.types.ExpressionUtils.path;
import static com.querydsl.core.types.ExpressionUtils.predicate;
import static com.querydsl.core.types.dsl.Expressions.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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

import com.querydsl.core.types.Ops;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.PredicateOperation;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.BooleanOperation;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.SimplePath;
import com.querydsl.core.types.dsl.StringPath;
import org.springframework.data.mongodb.repository.User;

/**
 * Unit tests for {@link SpringDataMongodbSerializer}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mikhail Kaduchka
 * @author Enrique Leon Molina
 */
@ExtendWith(MockitoExtension.class)
public class SpringDataMongodbSerializerUnitTests {

	@Mock DbRefResolver dbFactory;
	MongoConverter converter;
	SpringDataMongodbSerializer serializer;

	@BeforeEach
	public void setUp() {

		MongoMappingContext context = new MongoMappingContext();

		this.converter = new MappingMongoConverter(dbFactory, context);
		this.serializer = new SpringDataMongodbSerializer(converter);
	}

	@Test
	public void uses_idAsKeyForIdProperty() {

		StringPath path = QPerson.person.id;
		assertThat(serializer.getKeyForPath(path, path.getMetadata())).isEqualTo("_id");
	}

	@Test
	public void buildsNestedKeyCorrectly() {

		StringPath path = QPerson.person.address.street;
		assertThat(serializer.getKeyForPath(path, path.getMetadata())).isEqualTo("street");
	}

	@Test
	public void convertsComplexObjectOnSerializing() {

		Address address = new Address();
		address.street = "Foo";
		address.zipCode = "01234";

		Document document = serializer.asDocument("foo", address);

		Object value = document.get("foo");
		assertThat(value).isNotNull().isInstanceOf(Document.class);

		Object reference = converter.convertToMongoType(address);
		assertThat(value).isEqualTo(reference);
	}

	@Test // DATAMONGO-376
	public void returnsEmptyStringIfNoPathExpressionIsGiven() {

		QAddress address = QPerson.person.shippingAddresses.any();
		assertThat(serializer.getKeyForPath(address, address.getMetadata())).isEmpty();
	}

	@Test // DATAMONGO-467, DATAMONGO-1798
	public void retainsIdPropertyType() {

		ObjectId id = new ObjectId();

		PathBuilder<Address> builder = new PathBuilder<Address>(Address.class, "address");
		StringPath idPath = builder.getString("id");

		Document result = (Document) serializer.visit((BooleanOperation) idPath.eq(id.toString()), null);
		assertThat(result.get("_id")).isNotNull().isInstanceOf(String.class).isEqualTo(id.toString());
	}

	@Test // DATAMONGO-761
	public void looksUpKeyForNonPropertyPath() {

		PathBuilder<Address> builder = new PathBuilder<Address>(Address.class, "address");
		SimplePath<Object> firstElementPath = builder.getArray("foo", String[].class).get(0);
		String path = serializer.getKeyForPath(firstElementPath, firstElementPath.getMetadata());

		assertThat(path).isEqualTo("0");
	}

	@Test // DATAMONGO-1485
	public void takesCustomConversionForEnumsIntoAccount() {

		MongoMappingContext context = new MongoMappingContext();

		MappingMongoConverter converter = new MappingMongoConverter(dbFactory, context);
		converter.setCustomConversions(new MongoCustomConversions(Collections.singletonList(new SexTypeWriteConverter())));
		converter.afterPropertiesSet();

		this.converter = converter;
		this.serializer = new SpringDataMongodbSerializer(this.converter);

		Object mappedPredicate = serializer.handle(QPerson.person.sex.eq(Sex.FEMALE));

		assertThat(mappedPredicate).isInstanceOf(Document.class);
		assertThat(((Document) mappedPredicate).get("sex")).isEqualTo("f");
	}

	@Test // DATAMONGO-1848, DATAMONGO-1943
	public void shouldRemarshallListsAndDocuments() {

		BooleanExpression criteria = QPerson.person.lastname.isNotEmpty()
				.and(QPerson.person.firstname.containsIgnoreCase("foo")).not();

		assertThat(serializer.handle(criteria)).isEqualTo(Document.parse("{ \"$or\" : [ { \"lastname\" : { \"$not\" : { "
				+ "\"$ne\" : \"\"}}} , { \"firstname\" : { \"$not\" : { \"$regex\" : \".*\\\\Qfoo\\\\E.*\" , \"$options\" : \"i\"}}}]}"));
	}

	@Test // DATAMONGO-2228
	public void retainsOpsInAndExpression() {

		PredicateOperation testExpression = predicate(Ops.AND,
				predicate(Ops.OR, predicate(Ops.EQ, path(Object.class, "firstname"), constant("John")),
						predicate(Ops.EQ, path(Object.class, "firstname"), constant("Sarah"))),
				predicate(Ops.OR, predicate(Ops.EQ, path(Object.class, "lastname"), constant("Smith")),
						predicate(Ops.EQ, path(Object.class, "lastname"), constant("Connor"))));

		assertThat(serializer.handle(testExpression)).isEqualTo(Document.parse(
				"{\"$and\": [{\"$or\": [{\"firstname\": \"John\"}, {\"firstname\": \"Sarah\"}]}, {\"$or\": [{\"lastname\": \"Smith\"}, {\"lastname\": \"Connor\"}]}]}"));
	}

	@Test // DATAMONGO-2475
	public void chainedOrsInSameDocument() {

		Predicate predicate = QPerson.person.firstname.eq("firstname_value")
				.or(QPerson.person.lastname.eq("lastname_value")).or(QPerson.person.age.goe(30)).or(QPerson.person.age.loe(20))
				.or(QPerson.person.uniqueId.isNull());

		assertThat(serializer.handle(predicate)).isEqualTo(Document.parse(
				"{\"$or\": [{\"firstname\": \"firstname_value\"}, {\"lastname\": \"lastname_value\"}, {\"age\": {\"$gte\": 30}}, {\"age\": {\"$lte\": 20}}, {\"uniqueId\": {\"$exists\": false}}]}"));
	}

	@Test // DATAMONGO-2475
	public void chainedNestedOrsInSameDocument() {

		Predicate predicate = QPerson.person.firstname.eq("firstname_value")
				.or(QPerson.person.lastname.eq("lastname_value")).or(QPerson.person.address.street.eq("spring"));

		assertThat(serializer.handle(predicate)).isEqualTo(Document.parse(
				"{\"$or\": [{\"firstname\": \"firstname_value\"}, {\"lastname\": \"lastname_value\"}, {\"add.street\": \"spring\"}]}"));
	}

	@Test // DATAMONGO-2475
	public void chainedAndsInSameDocument() {

		Predicate predicate = QPerson.person.firstname.eq("firstname_value")
				.and(QPerson.person.lastname.eq("lastname_value")).and(QPerson.person.age.goe(30))
				.and(QPerson.person.age.loe(20)).and(QPerson.person.uniqueId.isNull());

		assertThat(serializer.handle(predicate)).isEqualTo(Document.parse(
				"{\"$and\": [{\"firstname\": \"firstname_value\", \"lastname\": \"lastname_value\", \"age\": {\"$gte\": 30}, \"uniqueId\": {\"$exists\": false}}, {\"age\": {\"$lte\": 20}}]}"));
	}

	@Test // DATAMONGO-2475
	void chainMultipleAndFlattensCorrectly() {

		Document p1doc = Document.parse("{ \"$or\" : [ { \"firstname\" : \"fn\"}, { \"lastname\" : \"ln\" } ] }");
		Document p2doc = Document
				.parse("{ \"$or\" : [ { \"age\" : { \"$gte\" : 20 } }, { \"age\" : { \"$lte\" : 30} } ] }");
		Document p3doc = Document.parse("{ \"$or\" : [ { \"add.city\" : \"c\"}, { \"add.zipCode\" : \"0\" } ] }");
		Document expected = new Document("$and", Arrays.asList(p1doc, p2doc, p3doc));

		Predicate predicate1 = QPerson.person.firstname.eq("fn").or(QPerson.person.lastname.eq("ln"));
		Predicate predicate2 = QPerson.person.age.goe(20).or(QPerson.person.age.loe(30));
		Predicate predicate3 = QPerson.person.address.city.eq("c").or(QPerson.person.address.zipCode.eq("0"));
		PredicateOperation testExpression = predicate(Ops.AND, predicate1, predicate2, predicate3);

		assertThat(serializer.handle(testExpression)).isEqualTo(expected);
	}

	@Test // GH-4037
	void parsesDocumentReference() {

		User user = new User();
		user.setId("007");
		Predicate predicate = QPerson.person.spiritAnimal.eq(user);

		assertThat(serializer.handle(predicate)).isEqualTo(Document.parse("{ 'spiritAnimal' : '007' }"));
	}

	@Test // GH-4037
	void parsesDocumentReferenceOnId() {

		User user = new User();
		user.setId("007");
		Predicate predicate = QPerson.person.spiritAnimal.id.eq("007");

		assertThat(serializer.handle(predicate)).isEqualTo(Document.parse("{ 'spiritAnimal' : '007' }"));
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
