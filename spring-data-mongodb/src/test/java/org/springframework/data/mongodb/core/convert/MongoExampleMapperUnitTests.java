/*
 * Copyright 2015-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.domain.Example.*;
import static org.springframework.data.domain.ExampleMatcher.*;
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.ExampleMatcher.*;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.QueryMapperUnitTests.ClassWithGeoTypes;
import org.springframework.data.mongodb.core.convert.QueryMapperUnitTests.WithDBRef;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.test.util.IsBsonObject;
import org.springframework.data.util.TypeInformation;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoExampleMapperUnitTests {

	MongoExampleMapper mapper;
	MongoMappingContext context;
	MappingMongoConverter converter;

	@Mock MongoDbFactory factory;

	@Before
	public void setUp() {

		this.context = new MongoMappingContext();

		this.converter = new MappingMongoConverter(new DefaultDbRefResolver(factory), context);
		this.converter.afterPropertiesSet();

		this.mapper = new MongoExampleMapper(converter);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIdIsSet() {

		FlatDocument probe = new FlatDocument();
		probe.id = "steelheart";

		IsBsonObject<BSONObject> expected = isBsonObject().containing("_id", "steelheart");

		assertThat(mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenMultipleValuesSet() {

		FlatDocument probe = new FlatDocument();
		probe.id = "steelheart";
		probe.stringValue = "firefight";
		probe.intValue = 100;

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("_id", "steelheart").//
				containing("stringValue", "firefight").//
				containing("intValue", 100);

		assertThat(mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIdIsNotSet() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue", "firefight").//
				containing("intValue", 100);

		assertThat(mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenListHasValues() {

		FlatDocument probe = new FlatDocument();
		probe.listOfString = Arrays.asList("Prof", "Tia", "David");

		BasicDBList list = new BasicDBList();
		list.addAll(Arrays.asList("Prof", "Tia", "David"));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("listOfString", list);

		assertThat(mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenFieldNameIsCustomized() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "Mitosis";

		IsBsonObject<BSONObject> expected = isBsonObject().containing("custom_field_name", "Mitosis");

		assertThat(mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void typedExampleShouldContainTypeRestriction() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		DBObject dbo = mapper.getMappedExample(Example.of(probe), context.getPersistentEntity(WrapperDocument.class));

		assertThat(dbo,
				isBsonObject().containing("_class", new BasicDBObject("$in", new String[] { probe.getClass().getName() })));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedAsFlatMapWhenGivenNestedElementsWithLenientMatchMode() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		IsBsonObject<BSONObject> expected = isBsonObject().containing("flatDoc\\.stringValue", "conflux");

		assertThat(mapper.getMappedExample(of(probe), context.getPersistentEntity(WrapperDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedAsExactObjectWhenGivenNestedElementsWithStrictMatchMode() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		Example<?> example = Example.of(probe, matching().withIncludeNullValues());

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(WrapperDocument.class)), //
				isBsonObject().containing("flatDoc.stringValue", "conflux"));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeIsStarting() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.STARTING));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue.$regex", "^firefight").//
				containing("intValue", 100);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeContainingDotsWhenStringMatchModeIsStarting() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "fire.ight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.STARTING));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue.$regex", "^" + Pattern.quote("fire.ight")).//
				containing("intValue", 100);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeIsEnding() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.ENDING));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue.$regex", "firefight$").//
				containing("intValue", 100);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeRegex() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "^(cat|dog).*shelter\\d?";

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.REGEX));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue.$regex", "firefight").//
				containing("custom_field_name.$regex", "^(cat|dog).*shelter\\d?");

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIgnoreCaseEnabledAndMatchModeSet() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.ENDING).withIgnoreCase());

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue", new BasicDBObject("$regex", "firefight$").append("$options", "i")).//
				containing("intValue", 100);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIgnoreCaseEnabled() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withIgnoreCase());

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue", new BasicDBObject("$regex", Pattern.quote("firefight")).append("$options", "i")).//
				containing("intValue", 100);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedWhenContainingDBRef() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "steelheart";
		probe.referenceDocument = new ReferenceDocument();
		probe.referenceDocument.id = "200";

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(WithDBRef.class));
		com.mongodb.DBRef reference = getTypedValue(dbo, "referenceDocument", com.mongodb.DBRef.class);

		assertThat(reference.getId(), Is.<Object> is("200"));
		assertThat(reference.getCollectionName(), is("refDoc"));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedWhenDBRefIsNull() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "steelheart";

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, isBsonObject().containing("stringValue", "steelheart"));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyWhenContainingLegacyPoint() {

		ClassWithGeoTypes probe = new ClassWithGeoTypes();
		probe.legacyPoint = new Point(10D, 20D);

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(WithDBRef.class));

		assertThat(dbo.get("legacyPoint.x"), Is.<Object> is(10D));
		assertThat(dbo.get("legacyPoint.y"), Is.<Object> is(20D));
	}

	@Test // DATAMONGO-1245
	public void mappingShouldExcludeFieldWithCustomNameCorrectly() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "foo";
		probe.intValue = 10;
		probe.stringValue = "string";

		Example<?> example = Example.of(probe, matching().withIgnorePaths("customNamedField"));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue", "string").//
				containing("intValue", 10);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void mappingShouldExcludeFieldCorrectly() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "foo";
		probe.intValue = 10;
		probe.stringValue = "string";

		Example<?> example = Example.of(probe, matching().withIgnorePaths("stringValue"));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("custom_field_name", "foo").//
				containing("intValue", 10);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void mappingShouldExcludeNestedFieldCorrectly() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.customNamedField = "foo";
		probe.flatDoc.intValue = 10;
		probe.flatDoc.stringValue = "string";

		Example<?> example = Example.of(probe, matching().withIgnorePaths("flatDoc.stringValue"));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("flatDoc\\.custom_field_name", "foo").//
				containing("flatDoc\\.intValue", 10);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(WrapperDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void mappingShouldExcludeNestedFieldWithCustomNameCorrectly() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.customNamedField = "foo";
		probe.flatDoc.intValue = 10;
		probe.flatDoc.stringValue = "string";

		Example<?> example = Example.of(probe, matching().withIgnorePaths("flatDoc.customNamedField"));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("flatDoc\\.stringValue", "string").//
				containing("flatDoc\\.intValue", 10);

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(WrapperDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void mappingShouldFavorFieldSpecificationStringMatcherOverDefaultStringMatcher() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "steelheart";

		Example<?> example = Example.of(probe, matching().withMatcher("stringValue", GenericPropertyMatchers.contains()));

		IsBsonObject<BSONObject> expected = isBsonObject().//
				containing("stringValue.$regex", ".*firefight.*").//
				containing("custom_field_name", "steelheart");

		assertThat(mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class)), is(expected));
	}

	@Test // DATAMONGO-1245
	public void mappingShouldIncludePropertiesFromHierarchicalDocument() {

		HierachicalDocument probe = new HierachicalDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "steelheart";
		probe.anotherStringValue = "calamity";

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, isBsonObject().containing("anotherStringValue", "calamity"));
	}

	@Test // DATAMONGO-1459
	public void mapsAnyMatchingExampleCorrectly() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "steelheart";

		Example<FlatDocument> example = Example.of(probe, ExampleMatcher.matchingAny());

		assertThat(mapper.getMappedExample(example), isBsonObject().containing("$or").containing("_class"));
	}

	@Test // DATAMONGO-1768
	public void allowIgnoringTypeRestrictionBySettingUpTypeKeyAsAnIgnoredPath() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		DBObject dbo = mapper.getMappedExample(Example.of(probe, ExampleMatcher.matching().withIgnorePaths("_class")));

		assertThat(dbo, isBsonObject().notContaining("_class"));
	}

	@Test // DATAMONGO-1768
	public void allowIgnoringTypeRestrictionBySettingUpTypeKeyAsAnIgnoredPathWhenUsingCustomTypeMapper() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		MappingMongoConverter mappingMongoConverter = new MappingMongoConverter(new DefaultDbRefResolver(factory), context);
		mappingMongoConverter.setTypeMapper(new DefaultMongoTypeMapper() {

			@Override
			public boolean isTypeKey(String key) {
				return "_foo".equals(key);
			}

			@Override
			public void writeTypeRestrictions(DBObject dbo, Set<Class<?>> restrictedTypes) {
				dbo.put("_foo", "bar");
			}

			@Override
			public void writeType(TypeInformation<?> info, DBObject sink) {
				sink.put("_foo", "bar");

			}
		});
		mappingMongoConverter.afterPropertiesSet();

		DBObject dbo = new MongoExampleMapper(mappingMongoConverter)
				.getMappedExample(Example.of(probe, ExampleMatcher.matching().withIgnorePaths("_foo")));

		assertThat(dbo, isBsonObject().notContaining("_class").notContaining("_foo"));
	}

	static class FlatDocument {

		@Id String id;
		String stringValue;
		@Field("custom_field_name") String customNamedField;
		Integer intValue;
		List<String> listOfString;
		@DBRef ReferenceDocument referenceDocument;
	}

	static class HierachicalDocument extends FlatDocument {

		String anotherStringValue;
	}

	static class WrapperDocument {

		@Id String id;
		FlatDocument flatDoc;
	}

	@Document(collection = "refDoc")
	static class ReferenceDocument {

		@Id String id;
		String value;
	}
}
