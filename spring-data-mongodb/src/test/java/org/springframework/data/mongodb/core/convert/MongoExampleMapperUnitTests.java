/*
 * Copyright 2015-2021 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.springframework.data.domain.Example.*;
import static org.springframework.data.domain.ExampleMatcher.*;
import static org.springframework.data.mongodb.core.DocumentTestUtils.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.ExampleMatcher.*;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.QueryMapperUnitTests.ClassWithGeoTypes;
import org.springframework.data.mongodb.core.convert.QueryMapperUnitTests.WithDBRef;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Embedded;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.UntypedExampleMatcher;
import org.springframework.data.util.TypeInformation;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
public class MongoExampleMapperUnitTests {

	MongoExampleMapper mapper;
	MongoMappingContext context;
	MappingMongoConverter converter;

	@Mock MongoDatabaseFactory factory;

	@BeforeEach
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

		assertThat(mapper.getMappedExample(of(probe), context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("_id", "steelheart");
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenMultipleValuesSet() {

		FlatDocument probe = new FlatDocument();
		probe.id = "steelheart";
		probe.stringValue = "firefight";
		probe.intValue = 100;

		assertThat(mapper.getMappedExample(of(probe), context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("_id", "steelheart") //
				.containsEntry("stringValue", "firefight") //
				.containsEntry("intValue", 100);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIdIsNotSet() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		assertThat(mapper.getMappedExample(of(probe), context.getRequiredPersistentEntity(FlatDocument.class))) //
				.containsEntry("stringValue", "firefight") //
				.containsEntry("intValue", 100);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenListHasValues() {

		FlatDocument probe = new FlatDocument();
		probe.listOfString = Arrays.asList("Prof", "Tia", "David");

		List list = (Arrays.asList("Prof", "Tia", "David"));

		assertThat(mapper.getMappedExample(of(probe), context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("listOfString", list);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenFieldNameIsCustomized() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "Mitosis";

		assertThat(mapper.getMappedExample(of(probe), context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("custom_field_name", "Mitosis");
	}

	@Test // DATAMONGO-1245
	public void typedExampleShouldContainTypeRestriction() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		org.bson.Document document = mapper.getMappedExample(Example.of(probe),
				context.getRequiredPersistentEntity(WrapperDocument.class));

		assertThat(document).containsEntry("_class",
				new org.bson.Document("$in", Collections.singletonList(probe.getClass().getName())));
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedAsFlatMapWhenGivenNestedElementsWithLenientMatchMode() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		assertThat(mapper.getMappedExample(of(probe), context.getRequiredPersistentEntity(WrapperDocument.class)))
				.containsEntry("flatDoc\\.stringValue", "conflux");
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedAsExactObjectWhenGivenNestedElementsWithStrictMatchMode() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		Example<?> example = Example.of(probe, matching().withIncludeNullValues());

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(WrapperDocument.class)))
				.containsEntry("flatDoc.stringValue", "conflux");
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeIsStarting() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.STARTING));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("stringValue.$regex", "^firefight")//
				.containsEntry("intValue", 100);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeContainingDotsWhenStringMatchModeIsStarting() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "fire.ight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.STARTING));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("stringValue.$regex", "^" + Pattern.quote("fire.ight"))//
				.containsEntry("intValue", 100);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeIsEnding() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.ENDING));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("stringValue.$regex", "firefight$") //
				.containsEntry("intValue", 100);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenStringMatchModeRegex() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "^(cat|dog).*shelter\\d?";

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.REGEX));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("stringValue.$regex", "firefight") //
				.containsEntry("custom_field_name.$regex", "^(cat|dog).*shelter\\d?");
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIgnoreCaseEnabledAndMatchModeSet() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withStringMatcher(StringMatcher.ENDING).withIgnoreCase());

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("stringValue", new org.bson.Document("$regex", "firefight$").append("$options", "i")) //
				.containsEntry("intValue", 100);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyForFlatTypeWhenIgnoreCaseEnabled() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.intValue = 100;

		Example<?> example = Example.of(probe, matching().withIgnoreCase());

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("stringValue",
						new org.bson.Document("$regex", Pattern.quote("firefight")).append("$options", "i")) //
				.containsEntry("intValue", 100);
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedWhenContainingDBRef() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "steelheart";
		probe.referenceDocument = new ReferenceDocument();
		probe.referenceDocument.id = "200";

		org.bson.Document document = mapper.getMappedExample(of(probe),
				context.getRequiredPersistentEntity(WithDBRef.class));
		com.mongodb.DBRef reference = getTypedValue(document, "referenceDocument", com.mongodb.DBRef.class);

		assertThat(reference.getId()).isEqualTo("200");
		assertThat(reference.getCollectionName()).isEqualTo("refDoc");
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedWhenDBRefIsNull() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "steelheart";

		org.bson.Document document = mapper.getMappedExample(of(probe),
				context.getRequiredPersistentEntity(FlatDocument.class));

		assertThat(document).containsEntry("stringValue", "steelheart");
	}

	@Test // DATAMONGO-1245
	public void exampleShouldBeMappedCorrectlyWhenContainingLegacyPoint() {

		ClassWithGeoTypes probe = new ClassWithGeoTypes();
		probe.legacyPoint = new Point(10D, 20D);

		org.bson.Document document = mapper.getMappedExample(of(probe),
				context.getRequiredPersistentEntity(WithDBRef.class));

		assertThat(document.get("legacyPoint.x")).isEqualTo(10D);
		assertThat(document.get("legacyPoint.y")).isEqualTo(20D);
	}

	@Test // DATAMONGO-1245
	public void mappingShouldExcludeFieldWithCustomNameCorrectly() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "foo";
		probe.intValue = 10;
		probe.stringValue = "string";

		Example<?> example = Example.of(probe, matching().withIgnorePaths("customNamedField"));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("stringValue", "string") //
				.containsEntry("intValue", 10);
	}

	@Test // DATAMONGO-1245
	public void mappingShouldExcludeFieldCorrectly() {

		FlatDocument probe = new FlatDocument();
		probe.customNamedField = "foo";
		probe.intValue = 10;
		probe.stringValue = "string";

		Example<?> example = Example.of(probe, matching().withIgnorePaths("stringValue"));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("custom_field_name", "foo") //
				.containsEntry("intValue", 10);
	}

	@Test // DATAMONGO-1245
	public void mappingShouldExcludeNestedFieldCorrectly() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.customNamedField = "foo";
		probe.flatDoc.intValue = 10;
		probe.flatDoc.stringValue = "string";

		Example<?> example = Example.of(probe, matching().withIgnorePaths("flatDoc.stringValue"));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(WrapperDocument.class)))
				.containsEntry("flatDoc\\.custom_field_name", "foo")//
				.containsEntry("flatDoc\\.intValue", 10);
	}

	@Test // DATAMONGO-1245
	public void mappingShouldExcludeNestedFieldWithCustomNameCorrectly() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.customNamedField = "foo";
		probe.flatDoc.intValue = 10;
		probe.flatDoc.stringValue = "string";

		Example<?> example = Example.of(probe, matching().withIgnorePaths("flatDoc.customNamedField"));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(WrapperDocument.class)))
				.containsEntry("flatDoc\\.stringValue", "string") //
				.containsEntry("flatDoc\\.intValue", 10);
	}

	@Test // DATAMONGO-1245
	public void mappingShouldFavorFieldSpecificationStringMatcherOverDefaultStringMatcher() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "steelheart";

		Example<?> example = Example.of(probe, matching().withMatcher("stringValue", GenericPropertyMatchers.contains()));

		assertThat(mapper.getMappedExample(example, context.getRequiredPersistentEntity(FlatDocument.class)))
				.containsEntry("stringValue.$regex", ".*firefight.*") //
				.containsEntry("custom_field_name", "steelheart");
	}

	@Test // DATAMONGO-1245
	public void mappingShouldIncludePropertiesFromHierarchicalDocument() {

		HierachicalDocument probe = new HierachicalDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "steelheart";
		probe.anotherStringValue = "calamity";

		org.bson.Document document = mapper.getMappedExample(of(probe),
				context.getRequiredPersistentEntity(FlatDocument.class));

		assertThat(document).containsEntry("anotherStringValue", "calamity");
	}

	@Test // DATAMONGO-1459
	public void mapsAnyMatchingExampleCorrectly() {

		FlatDocument probe = new FlatDocument();
		probe.stringValue = "firefight";
		probe.customNamedField = "steelheart";

		Example<FlatDocument> example = Example.of(probe, ExampleMatcher.matchingAny());

		assertThat(mapper.getMappedExample(example)).containsKeys("$or", "_class");
	}

	@Test // DATAMONGO-1768
	public void allowIgnoringTypeRestrictionBySettingUpTypeKeyAsAnIgnoredPath() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		org.bson.Document document = mapper
				.getMappedExample(Example.of(probe, ExampleMatcher.matching().withIgnorePaths("_class")));

		assertThat(document).doesNotContainKey("_class");
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
			public void writeTypeRestrictions(org.bson.Document result, Set<Class<?>> restrictedTypes) {
				result.put("_foo", "bar");
			}

			@Override
			public void writeType(TypeInformation<?> info, Bson sink) {
				((org.bson.Document) sink).put("_foo", "bar");

			}
		});
		mappingMongoConverter.afterPropertiesSet();

		org.bson.Document document = new MongoExampleMapper(mappingMongoConverter)
				.getMappedExample(Example.of(probe, ExampleMatcher.matching().withIgnorePaths("_foo")));

		assertThat(document).doesNotContainKeys("_class", "_foo");
	}

	@Test // DATAMONGO-1768
	public void untypedExampleShouldNotInferTypeRestriction() {

		WrapperDocument probe = new WrapperDocument();
		probe.flatDoc = new FlatDocument();
		probe.flatDoc.stringValue = "conflux";

		org.bson.Document document = mapper.getMappedExample(Example.of(probe, UntypedExampleMatcher.matching()));
		assertThat(document).doesNotContainKey("_class");
	}

	@Test // DATAMONGO-1902
	void mapsEmbeddedType() {

		WithEmbedded probe = new WithEmbedded();
		probe.embeddableType = new EmbeddableType();
		probe.embeddableType.atFieldAnnotatedValue = "@Field";
		probe.embeddableType.stringValue = "string-value";

		org.bson.Document document = mapper.getMappedExample(Example.of(probe, UntypedExampleMatcher.matching()));
		assertThat(document).containsEntry("stringValue", "string-value").containsEntry("with-at-field-annotation",
				"@Field");
	}

	@Test // DATAMONGO-1902
	void mapsPrefixedEmbeddedType() {

		WithEmbedded probe = new WithEmbedded();
		probe.prefixedEmbeddableValue = new EmbeddableType();
		probe.prefixedEmbeddableValue.atFieldAnnotatedValue = "@Field";
		probe.prefixedEmbeddableValue.stringValue = "string-value";

		org.bson.Document document = mapper.getMappedExample(Example.of(probe, UntypedExampleMatcher.matching()));
		assertThat(document).containsEntry("prefix-stringValue", "string-value")
				.containsEntry("prefix-with-at-field-annotation", "@Field");
	}

	@Test // DATAMONGO-1902
	void mapsNestedEmbeddedType() {

		WrapperAroundWithEmbedded probe = new WrapperAroundWithEmbedded();
		probe.withEmbedded = new WithEmbedded();
		probe.withEmbedded.embeddableType = new EmbeddableType();
		probe.withEmbedded.embeddableType.atFieldAnnotatedValue = "@Field";
		probe.withEmbedded.embeddableType.stringValue = "string-value";

		org.bson.Document document = mapper.getMappedExample(Example.of(probe, UntypedExampleMatcher.matching()));
		assertThat(document).containsEntry("withEmbedded.stringValue", "string-value")
				.containsEntry("withEmbedded.with-at-field-annotation", "@Field");
	}

	@Test // DATAMONGO-1902
	void mapsNestedPrefixedEmbeddedType() {

		WrapperAroundWithEmbedded probe = new WrapperAroundWithEmbedded();
		probe.withEmbedded = new WithEmbedded();
		probe.withEmbedded.prefixedEmbeddableValue = new EmbeddableType();
		probe.withEmbedded.prefixedEmbeddableValue.atFieldAnnotatedValue = "@Field";
		probe.withEmbedded.prefixedEmbeddableValue.stringValue = "string-value";

		org.bson.Document document = mapper.getMappedExample(Example.of(probe, UntypedExampleMatcher.matching()));
		assertThat(document).containsEntry("withEmbedded.prefix-stringValue", "string-value")
				.containsEntry("withEmbedded.prefix-with-at-field-annotation", "@Field");
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

	@Document("refDoc")
	static class ReferenceDocument {

		@Id String id;
		String value;
	}

	@Document
	static class WrapperAroundWithEmbedded {

		String id;
		WithEmbedded withEmbedded;
	}

	@Document
	static class WithEmbedded {

		String id;

		@Embedded.Nullable EmbeddableType embeddableType;
		@Embedded.Nullable("prefix-") EmbeddableType prefixedEmbeddableValue;
	}

	static class EmbeddableType {

		@Indexed String stringValue;

		@Indexed //
		@Field("with-at-field-annotation") //
		String atFieldAnnotatedValue;
	}
}
