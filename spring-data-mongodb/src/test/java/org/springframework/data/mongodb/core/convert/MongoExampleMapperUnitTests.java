/*
 * Copyright 2015-2016 the original author or authors.
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
import static org.springframework.data.mongodb.core.DBObjectTestUtils.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleSpec;
import org.springframework.data.domain.ExampleSpec.GenericPropertyMatcher;
import org.springframework.data.domain.ExampleSpec.StringMatcher;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.QueryMapperUnitTests.ClassWithGeoTypes;
import org.springframework.data.mongodb.core.convert.QueryMapperUnitTests.WithDBRef;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
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

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIdIsSet() {

		FlatDocument probe = new FlatDocument();
		probe.id = "steelheart";

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("_id", "steelheart").get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenMultipleValuesSet() {

		FlatDocument probe = new FlatDocument();
		probe.id = "steelheart";
		probe.stringValue = "firefight";
		probe.intValue = 100;

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("_id", "steelheart").add("stringValue", "firefight")
				.add("intValue", 100).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIdIsNotSet() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("stringValue", "firefight").add("intValue", 100).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenListHasValues() {

		FlatDocument probe = new FlatDocument();
		probe.listOfString = Arrays.asList("Prof", "Tia", "David");

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("listOfString", Arrays.asList("Prof", "Tia", "David")).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenFieldNameIsCustomized() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "Mitosis";

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("custom_field_name", "Mitosis").get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedAsFlatMapWhenGivenNestedElementsWithLenientMatchMode() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(WrapperDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("flatDoc.stringValue", "conflux").get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedAsExactObjectWhenGivenNestedElementsWithStrictMatchMode() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		Example<?> example = ExampleSpec.of(WrapperDocument.class).withIncludeNullValues().createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(WrapperDocument.class));

		assertThat(dbo, isBsonObject().containing("flatDoc.stringValue", "conflux"));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeIsStarting() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = ExampleSpec.of(FlatDocument.class).withStringMatcher(StringMatcher.STARTING)
				.createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("stringValue", new BasicDBObject("$regex", "^firefight"))
				.add("intValue", 100).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeContainingDotsWhenStringMatchModeIsStarting() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "fire.ight";
		probe.intValue = 100;

		Example<?> example = ExampleSpec.of(FlatDocument.class).withStringMatcherStarting().createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder()
				.add("stringValue", new BasicDBObject("$regex", "^" + Pattern.quote("fire.ight"))).add("intValue", 100).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeIsEnding() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = ExampleSpec.of(FlatDocument.class).withStringMatcher(StringMatcher.ENDING).createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("stringValue", new BasicDBObject("$regex", "firefight$"))
				.add("intValue", 100).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeRegex() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "^(cat|dog).*shelter\\d?";

		Example<?> example = ExampleSpec.of(FlatDocument.class).withStringMatcher(StringMatcher.REGEX).createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("stringValue", new BasicDBObject("$regex", "firefight"))
				.add("custom_field_name", new BasicDBObject("$regex", "^(cat|dog).*shelter\\d?")).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIgnoreCaseEnabledAndMatchModeSet() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = ExampleSpec.of(FlatDocument.class).withStringMatcher(StringMatcher.ENDING).withIgnoreCase()
				.createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo,
				is(new BasicDBObjectBuilder()
						.add("stringValue", new BasicDBObjectBuilder().add("$regex", "firefight$").add("$options", "i").get())
						.add("intValue", 100).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIgnoreCaseEnabled() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = ExampleSpec.of(FlatDocument.class).withIgnoreCase().createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo,
				is(new BasicDBObjectBuilder()
						.add("stringValue",
								new BasicDBObjectBuilder().add("$regex", Pattern.quote("firefight")).add("$options", "i").get())
						.add("intValue", 100).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
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

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedWhenDBRefIsNull() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "steelheart";

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("stringValue", "steelheart").get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void exampleShouldBeMappedCorrectlyWhenContainingLegacyPoint() {

		ClassWithGeoTypes probe = new ClassWithGeoTypes();
		probe.legacyPoint = new Point(10D, 20D);

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(WithDBRef.class));

		assertThat(dbo.get("legacyPoint.x"), Is.<Object> is(10D));
		assertThat(dbo.get("legacyPoint.y"), Is.<Object> is(20D));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void mappingShouldExcludeFieldWithCustomNameCorrectly() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "foo";
		probe.intValue = 10;
		probe.stringValue = "string";

		Example<?> example = ExampleSpec.of(FlatDocument.class).withIgnorePaths("customNamedField").createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("stringValue", "string").add("intValue", 10).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void mappingShouldExcludeFieldCorrectly() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "foo";
		probe.intValue = 10;
		probe.stringValue = "string";

		Example<?> example = ExampleSpec.of(FlatDocument.class).withIgnorePaths("stringValue").createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("custom_field_name", "foo").add("intValue", 10).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void mappingShouldExcludeNestedFieldCorrectly() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.customNamedField = "foo";
		probe.flatDoc.intValue = 10;
		probe.flatDoc.stringValue = "string";

		Example<?> example = ExampleSpec.of(WrapperDocument.class).withIgnorePaths("flatDoc.stringValue").createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(WrapperDocument.class));

		assertThat(dbo,
				is(new BasicDBObjectBuilder().add("flatDoc.custom_field_name", "foo").add("flatDoc.intValue", 10).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void mappingShouldExcludeNestedFieldWithCustomNameCorrectly() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.customNamedField = "foo";
		probe.flatDoc.intValue = 10;
		probe.flatDoc.stringValue = "string";

		Example<?> example = ExampleSpec.of(WrapperDocument.class).withIgnorePaths("flatDoc.customNamedField")
				.createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(WrapperDocument.class));

		assertThat(dbo,
				is(new BasicDBObjectBuilder().add("flatDoc.stringValue", "string").add("flatDoc.intValue", 10).get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void mappingShouldFavorFieldSpecificationStringMatcherOverDefaultStringMatcher() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "steelheart";

		Example<?> example = ExampleSpec.of(FlatDocument.class)
				.withMatcher("stringValue", new GenericPropertyMatcher().contains()).createExample(probe);

		DBObject dbo = mapper.getMappedExample(example, context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, is(new BasicDBObjectBuilder().add("stringValue", new BasicDBObject("$regex", ".*firefight.*"))
				.add("custom_field_name", "steelheart").get()));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void mappingShouldIncludePropertiesFromHierarchicalDocument() {

		HierachicalDocument probe = new HierachicalDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "steelheart";
		probe.anotherStringValue = "calamity";

		DBObject dbo = mapper.getMappedExample(of(probe), context.getPersistentEntity(FlatDocument.class));

		assertThat(dbo, isBsonObject().containing("anotherStringValue", "calamity"));
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
