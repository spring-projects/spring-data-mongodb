/*
 * Copyright 2014-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.IndexDefinitionHolder;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.CompoundIndexResolutionTests;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.GeoSpatialIndexResolutionTests;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.IndexResolutionTests;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.MixedIndexResolutionTests;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.TextIndexedResolutionTests;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.Language;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Suite.class)
@SuiteClasses({ IndexResolutionTests.class, GeoSpatialIndexResolutionTests.class, CompoundIndexResolutionTests.class,
		TextIndexedResolutionTests.class, MixedIndexResolutionTests.class })
public class MongoPersistentEntityIndexResolverUnitTests {

	/**
	 * Test resolution of {@link Indexed}.
	 * 
	 * @author Christoph Strobl
	 */
	public static class IndexResolutionTests {

		@Test // DATAMONGO-899
		public void indexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnLevelZero.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("indexedProperty", "Zero", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void indexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelOne.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("zero.indexedProperty", "One", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void depplyNestedIndexPathIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelTwo.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("one.zero.indexedProperty", "Two", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void resolvesIndexPathNameForNamedPropertiesCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnLevelOneWithExplicitlyNamedField.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("customZero.customFieldName", "indexOnLevelOneWithExplicitlyNamedField",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void resolvesIndexDefinitionCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnLevelZero.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(), equalTo(new org.bson.Document().append("name", "indexedProperty")));
		}

		@Test // DATAMONGO-899
		public void resolvesIndexDefinitionOptionsCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithOptionsOnIndexedProperty.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(),
					equalTo(
							new org.bson.Document().append("name", "indexedProperty").append("unique", true)
									.append("sparse", true).append("background", true).append("expireAfterSeconds", 10L)));
		}

		@Test // DATAMONGO-1297
		public void resolvesIndexOnDbrefWhenDefined() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WithDbRef.class);

			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getCollection(), equalTo("withDbRef"));
			assertThat(indexDefinitions.get(0).getIndexKeys(), equalTo(new org.bson.Document().append("indexedDbRef", 1)));
		}

		@Test // DATAMONGO-1297
		public void resolvesIndexOnDbrefWhenDefinedOnNestedElement() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WrapperOfWithDbRef.class);

			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getCollection(), equalTo("wrapperOfWithDbRef"));
			assertThat(indexDefinitions.get(0).getIndexKeys(),
					equalTo(new org.bson.Document().append("nested.indexedDbRef", 1)));
		}

		@Test // DATAMONGO-1163
		public void resolveIndexDefinitionInMetaAnnotatedFields() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnMetaAnnotatedField.class);

			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getCollection(), equalTo("indexOnMetaAnnotatedField"));
			assertThat(indexDefinitions.get(0).getIndexOptions(), equalTo(new org.bson.Document().append("name", "_name")));
		}

		@Test // DATAMONGO-1373
		public void resolveIndexDefinitionInComposedAnnotatedFields() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexedDocumentWithComposedAnnotations.class);

			assertThat(indexDefinitions, hasSize(2));

			IndexDefinitionHolder indexDefinitionHolder = indexDefinitions.get(1);

			assertThat(indexDefinitionHolder.getIndexKeys(), isBsonObject().containing("fieldWithMyIndexName", 1));
			assertThat(indexDefinitionHolder.getIndexOptions(),
					isBsonObject().containing("sparse", true).containing("unique", true).containing("name", "my_index_name"));
		}

		@Test // DATAMONGO-1373
		public void resolveIndexDefinitionInCustomComposedAnnotatedFields() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexedDocumentWithComposedAnnotations.class);

			assertThat(indexDefinitions, hasSize(2));

			IndexDefinitionHolder indexDefinitionHolder = indexDefinitions.get(0);

			assertThat(indexDefinitionHolder.getIndexKeys(), isBsonObject().containing("fieldWithDifferentIndexName", 1));
			assertThat(indexDefinitionHolder.getIndexOptions(),
					isBsonObject().containing("sparse", true).containing("name", "different_name").notContaining("unique"));
		}

		@Document(collection = "Zero")
		static class IndexOnLevelZero {
			@Indexed String indexedProperty;
		}

		@Document(collection = "One")
		static class IndexOnLevelOne {
			IndexOnLevelZero zero;
		}

		@Document(collection = "Two")
		static class IndexOnLevelTwo {
			IndexOnLevelOne one;
		}

		@Document(collection = "WithOptionsOnIndexedProperty")
		static class WithOptionsOnIndexedProperty {

			@Indexed(background = true, direction = IndexDirection.DESCENDING,
					dropDups = true, expireAfterSeconds = 10, sparse = true, unique = true) //
			String indexedProperty;
		}

		@Document
		static class IndexOnLevelOneWithExplicitlyNamedField {

			@Field("customZero") IndexOnLevelZeroWithExplicityNamedField zero;
		}

		static class IndexOnLevelZeroWithExplicityNamedField {

			@Indexed @Field("customFieldName") String namedProperty;
		}

		@Document
		static class WrapperOfWithDbRef {
			WithDbRef nested;
		}

		@Document
		static class WithDbRef {

			@Indexed //
			@DBRef //
			NoIndex indexedDbRef;
		}

		@Document(collection = "no-index")
		static class NoIndex {
			@Id String id;
		}

		@Document
		static class IndexedDocumentWithComposedAnnotations {

			@Id String id;
			@CustomIndexedAnnotation String fieldWithDifferentIndexName;
			@ComposedIndexedAnnotation String fieldWithMyIndexName;
		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ ElementType.FIELD })
		@ComposedIndexedAnnotation(indexName = "different_name", beUnique = false)
		static @interface CustomIndexedAnnotation {
		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
		@Indexed
		static @interface ComposedIndexedAnnotation {

			@AliasFor(annotation = Indexed.class, attribute = "unique")
			boolean beUnique() default true;

			@AliasFor(annotation = Indexed.class, attribute = "sparse")
			boolean beSparse() default true;

			@AliasFor(annotation = Indexed.class, attribute = "name")
			String indexName() default "my_index_name";
		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target(ElementType.FIELD)
		@org.springframework.data.mongodb.core.mapping.Field
		static @interface ComposedFieldAnnotation {

			@AliasFor(annotation = org.springframework.data.mongodb.core.mapping.Field.class, attribute = "value")
			String name() default "_id";
		}
	}

	@Target({ ElementType.FIELD })
	@Retention(RetentionPolicy.RUNTIME)
	@Indexed
	@interface IndexedFieldAnnotation {

	}

	@Document
	static class IndexOnMetaAnnotatedField {
		@Field("_name") @IndexedFieldAnnotation String lastname;
	}

	/**
	 * Test resolution of {@link GeoSpatialIndexed}.
	 * 
	 * @author Christoph Strobl
	 */
	public static class GeoSpatialIndexResolutionTests {

		@Test // DATAMONGO-899
		public void geoSpatialIndexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					GeoSpatialIndexOnLevelZero.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("geoIndexedProperty", "Zero", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void geoSpatialIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					GeoSpatialIndexOnLevelOne.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("zero.geoIndexedProperty", "One", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void depplyNestedGeoSpatialIndexPathIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					GeoSpatialIndexOnLevelTwo.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("one.zero.geoIndexedProperty", "Two", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void resolvesIndexDefinitionOptionsCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithOptionsOnGeoSpatialIndexProperty.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();

			assertThat(indexDefinition.getIndexOptions(), equalTo(
					new org.bson.Document().append("name", "location").append("min", 1).append("max", 100).append("bits", 2)));
		}

		@Test // DATAMONGO-1373
		public void resolvesComposedAnnotationIndexDefinitionOptionsCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					GeoSpatialIndexedDocumentWithComposedAnnotation.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();

			assertThat(indexDefinition.getIndexKeys(),
					isBsonObject().containing("location", "geoHaystack").containing("What light?", 1));
			assertThat(indexDefinition.getIndexOptions(),
					isBsonObject().containing("name", "my_geo_index_name").containing("bucketSize", 2.0));
		}

		@Document(collection = "Zero")
		static class GeoSpatialIndexOnLevelZero {
			@GeoSpatialIndexed Point geoIndexedProperty;
		}

		@Document(collection = "One")
		static class GeoSpatialIndexOnLevelOne {
			GeoSpatialIndexOnLevelZero zero;
		}

		@Document(collection = "Two")
		static class GeoSpatialIndexOnLevelTwo {
			GeoSpatialIndexOnLevelOne one;
		}

		@Document(collection = "WithOptionsOnGeoSpatialIndexProperty")
		static class WithOptionsOnGeoSpatialIndexProperty {

			@GeoSpatialIndexed(bits = 2, max = 100, min = 1,
					type = GeoSpatialIndexType.GEO_2D) //
			Point location;
		}

		@Document(collection = "WithComposedAnnotation")
		static class GeoSpatialIndexedDocumentWithComposedAnnotation {

			@ComposedGeoSpatialIndexed //
			Point location;
		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ ElementType.FIELD })
		@GeoSpatialIndexed
		@interface ComposedGeoSpatialIndexed {

			@AliasFor(annotation = GeoSpatialIndexed.class, attribute = "name")
			String indexName() default "my_geo_index_name";

			@AliasFor(annotation = GeoSpatialIndexed.class, attribute = "additionalField")
			String theAdditionalFieldINeedToDefine() default "What light?";

			@AliasFor(annotation = GeoSpatialIndexed.class, attribute = "bucketSize")
			double size() default 2;

			@AliasFor(annotation = GeoSpatialIndexed.class, attribute = "type")
			GeoSpatialIndexType indexType() default GeoSpatialIndexType.GEO_HAYSTACK;
		}

	}

	/**
	 * Test resolution of {@link CompoundIndexes}.
	 * 
	 * @author Christoph Strobl
	 */
	public static class CompoundIndexResolutionTests {

		@Test // DATAMONGO-899
		public void compoundIndexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexOnLevelZero.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "CompoundIndexOnLevelZero", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void compoundIndexOptionsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexOnLevelZero.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(), equalTo(new org.bson.Document().append("name", "compound_index")
					.append("unique", true).append("sparse", true).append("background", true)));
			assertThat(indexDefinition.getIndexKeys(), equalTo(new org.bson.Document().append("foo", 1).append("bar", -1)));
		}

		@Test // DATAMONGO-909
		public void compoundIndexOnSuperClassResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexDefinedOnSuperClass.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(), equalTo(new org.bson.Document().append("name", "compound_index")
					.append("unique", true).append("sparse", true).append("background", true)));
			assertThat(indexDefinition.getIndexKeys(), equalTo(new org.bson.Document().append("foo", 1).append("bar", -1)));
		}

		@Test // DATAMONGO-827
		public void compoundIndexDoesNotSpecifyNameWhenUsingGenerateName() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					ComountIndexWithAutogeneratedName.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(), equalTo(new org.bson.Document().append("unique", true)
					.append("sparse", true).append("background", true)));
			assertThat(indexDefinition.getIndexKeys(), equalTo(new org.bson.Document().append("foo", 1).append("bar", -1)));
		}

		@Test // DATAMONGO-929
		public void compoundIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexOnLevelOne.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection(new String[] { "zero.foo", "zero.bar" }, "CompoundIndexOnLevelOne",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-929
		public void emptyCompoundIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexOnLevelOneWithEmptyIndexDefinition.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection(new String[] { "zero" }, "CompoundIndexOnLevelZeroWithEmptyIndexDef",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-929
		public void singleCompoundIndexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					SingleCompoundIndex.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "CompoundIndexOnLevelZero", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-1373
		public void singleCompoundIndexUsingComposedAnnotationsOnTypeResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexDocumentWithComposedAnnotation.class);

			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getIndexKeys(), isBsonObject().containing("foo", 1).containing("bar", -1));
			assertThat(indexDefinitions.get(0).getIndexOptions(), isBsonObject().containing("name", "my_compound_index_name")
					.containing("unique", true).containing("background", true));
		}

		@Document(collection = "CompoundIndexOnLevelOne")
		static class CompoundIndexOnLevelOne {

			CompoundIndexOnLevelZero zero;
		}

		@Document(collection = "CompoundIndexOnLevelZeroWithEmptyIndexDef")
		static class CompoundIndexOnLevelOneWithEmptyIndexDefinition {

			CompoundIndexOnLevelZeroWithEmptyIndexDef zero;
		}

		@Document(collection = "CompoundIndexOnLevelZero")
		@CompoundIndexes({ @CompoundIndex(name = "compound_index", def = "{'foo': 1, 'bar': -1}", background = true,
				dropDups = true, sparse = true, unique = true) })
		static class CompoundIndexOnLevelZero {}

		@CompoundIndexes({
				@CompoundIndex(name = "compound_index", background = true, dropDups = true, sparse = true, unique = true) })
		static class CompoundIndexOnLevelZeroWithEmptyIndexDef {}

		@Document(collection = "CompoundIndexOnLevelZero")
		@CompoundIndex(name = "compound_index", def = "{'foo': 1, 'bar': -1}", background = true, dropDups = true,
				sparse = true, unique = true)
		static class SingleCompoundIndex {}

		static class IndexDefinedOnSuperClass extends CompoundIndexOnLevelZero {

		}

		@Document(collection = "ComountIndexWithAutogeneratedName")
		@CompoundIndexes({ @CompoundIndex(useGeneratedName = true, def = "{'foo': 1, 'bar': -1}", background = true,
				dropDups = true, sparse = true, unique = true) })
		static class ComountIndexWithAutogeneratedName {

		}

		@Document(collection = "WithComposedAnnotation")
		@ComposedCompoundIndex
		static class CompoundIndexDocumentWithComposedAnnotation {

		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ ElementType.TYPE })
		@CompoundIndex
		@interface ComposedCompoundIndex {

			@AliasFor(annotation = CompoundIndex.class, attribute = "def")
			String fields() default "{'foo': 1, 'bar': -1}";

			@AliasFor(annotation = CompoundIndex.class, attribute = "background")
			boolean inBackground() default true;

			@AliasFor(annotation = CompoundIndex.class, attribute = "name")
			String indexName() default "my_compound_index_name";

			@AliasFor(annotation = CompoundIndex.class, attribute = "useGeneratedName")
			boolean useGeneratedName() default false;

			@AliasFor(annotation = CompoundIndex.class, attribute = "unique")
			boolean isUnique() default true;

		}

	}

	public static class TextIndexedResolutionTests {

		@Test // DATAMONGO-937
		public void shouldResolveSingleFieldTextIndexCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnSinglePropertyInRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection("bar", "textIndexOnSinglePropertyInRoot", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-937
		public void shouldResolveMultiFieldTextIndexCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnMutiplePropertiesInRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "textIndexOnMutiplePropertiesInRoot",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-937
		public void shouldResolveTextIndexOnElementCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnNestedRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection(new String[] { "nested.foo" }, "textIndexOnNestedRoot", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-937
		public void shouldResolveTextIndexOnElementWithWeightCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnNestedWithWeightRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection(new String[] { "nested.foo" }, "textIndexOnNestedWithWeightRoot",
					indexDefinitions.get(0));

			org.bson.Document weights = DocumentTestUtils.getAsDocument(indexDefinitions.get(0).getIndexOptions(), "weights");
			assertThat(weights.get("nested.foo"), is((Object) 5F));
		}

		@Test // DATAMONGO-937
		public void shouldResolveTextIndexOnElementWithMostSpecificWeightCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnNestedWithMostSpecificValueRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection(new String[] { "nested.foo", "nested.bar" },
					"textIndexOnNestedWithMostSpecificValueRoot", indexDefinitions.get(0));

			org.bson.Document weights = DocumentTestUtils.getAsDocument(indexDefinitions.get(0).getIndexOptions(), "weights");
			assertThat(weights.get("nested.foo"), is((Object) 5F));
			assertThat(weights.get("nested.bar"), is((Object) 10F));
		}

		@Test // DATAMONGO-937
		public void shouldSetDefaultLanguageCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithDefaultLanguage.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("default_language"), is((Object) "spanish"));
		}

		@Test // DATAMONGO-937, DATAMONGO-1049
		public void shouldResolveTextIndexLanguageOverrideCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithLanguageOverride.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("language_override"), is((Object) "lang"));
		}

		@Test // DATAMONGO-1049
		public void shouldIgnoreTextIndexLanguageOverrideOnNestedElements() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithLanguageOverrideOnNestedElement.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("language_override"), is(nullValue()));
		}

		@Test // DATAMONGO-1049
		public void shouldNotCreateIndexDefinitionWhenOnlyLanguageButNoTextIndexPresent() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNoTextIndexPropertyButReservedFieldLanguage.class);
			assertThat(indexDefinitions, is(empty()));
		}

		@Test // DATAMONGO-1049
		public void shouldNotCreateIndexDefinitionWhenOnlyAnnotatedLanguageButNoTextIndexPresent() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNoTextIndexPropertyButReservedFieldLanguageAnnotated.class);
			assertThat(indexDefinitions, is(empty()));
		}

		@Test // DATAMONGO-1049
		public void shouldPreferExplicitlyAnnotatedLanguageProperty() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithOverlappingLanguageProps.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("language_override"), is((Object) "lang"));
		}

		@Test // DATAMONGO-1373
		public void shouldResolveComposedAnnotationCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexedDocumentWithComposedAnnotation.class);

			org.bson.Document weights = DocumentTestUtils.getAsDocument(indexDefinitions.get(0).getIndexOptions(), "weights");
			assertThat(weights, isBsonObject().containing("foo", 99f));
		}

		@Document
		static class TextIndexOnSinglePropertyInRoot {

			String foo;

			@TextIndexed String bar;
		}

		@Document
		static class TextIndexOnMutiplePropertiesInRoot {

			@TextIndexed String foo;

			@TextIndexed(weight = 5) String bar;
		}

		@Document
		static class TextIndexOnNestedRoot {

			String bar;

			@TextIndexed TextIndexOnNested nested;
		}

		static class TextIndexOnNested {

			String foo;
		}

		@Document
		static class TextIndexOnNestedWithWeightRoot {

			@TextIndexed(weight = 5) TextIndexOnNested nested;
		}

		@Document
		static class TextIndexOnNestedWithMostSpecificValueRoot {
			@TextIndexed(weight = 5) TextIndexOnNestedWithMostSpecificValue nested;
		}

		static class TextIndexOnNestedWithMostSpecificValue {

			String foo;
			@TextIndexed(weight = 10) String bar;
		}

		@Document(language = "spanish")
		static class DocumentWithDefaultLanguage {
			@TextIndexed String foo;
		}

		@Document
		static class DocumentWithLanguageOverrideOnNestedElement {

			DocumentWithLanguageOverride nested;
		}

		@Document
		static class DocumentWithLanguageOverride {

			@TextIndexed String foo;

			@Language String lang;
		}

		@Document
		static class DocumentWithNoTextIndexPropertyButReservedFieldLanguage {

			String language;
		}

		@Document
		static class DocumentWithNoTextIndexPropertyButReservedFieldLanguageAnnotated {

			@Field("language") String lang;
		}

		@Document
		static class DocumentWithOverlappingLanguageProps {

			@TextIndexed String foo;
			String language;
			@Language String lang;
		}

		@Document
		static class TextIndexedDocumentWithComposedAnnotation {

			@ComposedTextIndexedAnnotation String foo;
			String lang;
		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
		@TextIndexed
		static @interface ComposedTextIndexedAnnotation {

			@AliasFor(annotation = TextIndexed.class, attribute = "weight")
			float heavyweight() default 99f;
		}
	}

	public static class MixedIndexResolutionTests {

		@Test // DATAMONGO-899
		public void multipleIndexesResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(MixedIndexRoot.class);

			assertThat(indexDefinitions, hasSize(2));
			assertThat(indexDefinitions.get(0).getIndexDefinition(), instanceOf(Index.class));
			assertThat(indexDefinitions.get(1).getIndexDefinition(), instanceOf(GeospatialIndex.class));
		}

		@Test // DATAMONGO-899
		public void cyclicPropertyReferenceOverDBRefShouldNotBeTraversed() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(Inner.class);
			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getIndexDefinition().getIndexKeys(),
					equalTo(new org.bson.Document().append("outer", 1)));
		}

		@Test // DATAMONGO-899
		public void associationsShouldNotBeTraversed() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(Outer.class);
			assertThat(indexDefinitions, empty());
		}

		@Test // DATAMONGO-926
		public void shouldNotRunIntoStackOverflow() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CycleStartingInBetween.class);
			assertThat(indexDefinitions, hasSize(1));
		}

		@Test // DATAMONGO-926
		public void indexShouldBeFoundEvenForCyclePropertyReferenceOnLevelZero() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CycleLevelZero.class);
			assertIndexPathAndCollection("indexedProperty", "cycleLevelZero", indexDefinitions.get(0));
			assertIndexPathAndCollection("cyclicReference.indexedProperty", "cycleLevelZero", indexDefinitions.get(1));
			assertThat(indexDefinitions, hasSize(2));
		}

		@Test // DATAMONGO-926
		public void indexShouldBeFoundEvenForCyclePropertyReferenceOnLevelOne() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CycleOnLevelOne.class);
			assertIndexPathAndCollection("reference.indexedProperty", "cycleOnLevelOne", indexDefinitions.get(0));
			assertThat(indexDefinitions, hasSize(1));
		}

		@Test // DATAMONGO-926
		public void indexBeResolvedCorrectlyWhenPropertiesOfDifferentTypesAreNamedEqually() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					NoCycleButIdenticallyNamedProperties.class);
			assertIndexPathAndCollection("foo", "noCycleButIdenticallyNamedProperties", indexDefinitions.get(0));
			assertIndexPathAndCollection("reference.foo", "noCycleButIdenticallyNamedProperties", indexDefinitions.get(1));
			assertIndexPathAndCollection("reference.deep.foo", "noCycleButIdenticallyNamedProperties",
					indexDefinitions.get(2));
			assertThat(indexDefinitions, hasSize(3));
		}

		@Test // DATAMONGO-949
		public void shouldNotDetectCycleInSimilarlyNamedProperties() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					SimilarityHolingBean.class);
			assertIndexPathAndCollection("norm", "similarityHolingBean", indexDefinitions.get(0));
			assertThat(indexDefinitions, hasSize(1));
		}

		@Test // DATAMONGO-962
		public void shouldDetectSelfCycleViaCollectionTypeCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					SelfCyclingViaCollectionType.class);
			assertThat(indexDefinitions, empty());
		}

		@Test // DATAMONGO-962
		public void shouldNotDetectCycleWhenTypeIsUsedMoreThanOnce() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					MultipleObjectsOfSameType.class);
			assertThat(indexDefinitions, empty());
		}

		@Test // DATAMONGO-962
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void shouldCatchCyclicReferenceExceptionOnRoot() {

			MongoPersistentEntity entity = new BasicMongoPersistentEntity<Object>(ClassTypeInformation.from(Object.class));

			MongoPersistentProperty propertyMock = mock(MongoPersistentProperty.class);
			when(propertyMock.isEntity()).thenReturn(true);
			when(propertyMock.getOwner()).thenReturn(entity);
			when(propertyMock.getActualType()).thenThrow(
					new MongoPersistentEntityIndexResolver.CyclicPropertyReferenceException("foo", Object.class, "bar"));

			MongoPersistentEntity<SelfCyclingViaCollectionType> selfCyclingEntity = new BasicMongoPersistentEntity<SelfCyclingViaCollectionType>(
					ClassTypeInformation.from(SelfCyclingViaCollectionType.class));

			new MongoPersistentEntityIndexResolver(prepareMappingContext(SelfCyclingViaCollectionType.class))
					.resolveIndexForEntity(selfCyclingEntity);
		}

		@Test // DATAMONGO-1025
		public void shouldUsePathIndexAsIndexNameForDocumentsHavingNamedNestedCompoundIndexFixedOnCollection() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNestedDocumentHavingNamedCompoundIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("propertyOfTypeHavingNamedCompoundIndex.c_index"));
		}

		@Test // DATAMONGO-1025
		public void shouldUseIndexNameForNestedTypesWithNamedCompoundIndexDefinition() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNestedTypeHavingNamedCompoundIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("propertyOfTypeHavingNamedCompoundIndex.c_index"));
		}

		@Test // DATAMONGO-1025
		public void shouldUsePathIndexAsIndexNameForDocumentsHavingNamedNestedIndexFixedOnCollection() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNestedDocumentHavingNamedIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("propertyOfTypeHavingNamedIndex.property_index"));
		}

		@Test // DATAMONGO-1025
		public void shouldUseIndexNameForNestedTypesWithNamedIndexDefinition() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNestedTypeHavingNamedIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("propertyOfTypeHavingNamedIndex.property_index"));
		}

		@Test // DATAMONGO-1025
		public void shouldUseIndexNameOnRootLevel() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNamedIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"), equalTo("property_index"));
		}

		@Test // DATAMONGO-1087
		public void shouldAllowMultiplePropertiesOfSameTypeWithMatchingStartLettersOnRoot() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					MultiplePropertiesOfSameTypeWithMatchingStartLetters.class);

			assertThat(indexDefinitions, hasSize(2));
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"), equalTo("name.component"));
			assertThat((String) indexDefinitions.get(1).getIndexOptions().get("name"), equalTo("nameLast.component"));
		}

		@Test // DATAMONGO-1087
		public void shouldAllowMultiplePropertiesOfSameTypeWithMatchingStartLettersOnNestedProperty() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					MultiplePropertiesOfSameTypeWithMatchingStartLettersOnNestedProperty.class);

			assertThat(indexDefinitions, hasSize(2));
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"), equalTo("component.nameLast"));
			assertThat((String) indexDefinitions.get(1).getIndexOptions().get("name"), equalTo("component.name"));
		}

		@Test // DATAMONGO-1121
		public void shouldOnlyConsiderEntitiesAsPotentialCycleCandidates() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					OuterDocumentReferingToIndexedPropertyViaDifferentNonCyclingPaths.class);

			assertThat(indexDefinitions, hasSize(2));
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"), equalTo("path1.foo"));
			assertThat((String) indexDefinitions.get(1).getIndexOptions().get("name"),
					equalTo("path2.propertyWithIndexedStructure.foo"));

		}

		@Test // DATAMONGO-1263
		public void shouldConsiderGenericTypeArgumentsOfCollectionElements() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					EntityWithGenericTypeWrapperAsElement.class);

			assertThat(indexDefinitions, hasSize(1));
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("listWithGeneircTypeElement.entity.property_index"));
		}

		@Document
		static class MixedIndexRoot {

			@Indexed String first;
			NestedGeoIndex nestedGeo;
		}

		static class NestedGeoIndex {

			@GeoSpatialIndexed Point location;
		}

		@Document
		static class Outer {

			@DBRef Inner inner;
		}

		@Document
		static class Inner {

			@Indexed Outer outer;
		}

		@Document
		static class CycleLevelZero {

			@Indexed String indexedProperty;
			CycleLevelZero cyclicReference;
		}

		@Document
		static class CycleOnLevelOne {

			CycleOnLevelOneReferenced reference;
		}

		static class CycleOnLevelOneReferenced {

			@Indexed String indexedProperty;
			CycleOnLevelOne cyclicReference;
		}

		@Document
		public static class CycleStartingInBetween {

			CycleOnLevelOne referenceToCycleStart;
		}

		@Document
		static class NoCycleButIdenticallyNamedProperties {

			@Indexed String foo;
			NoCycleButIdenticallyNamedPropertiesNested reference;
		}

		static class NoCycleButIdenticallyNamedPropertiesNested {

			@Indexed String foo;
			NoCycleButIndenticallNamedPropertiesDeeplyNested deep;
		}

		static class NoCycleButIndenticallNamedPropertiesDeeplyNested {

			@Indexed String foo;
		}

		@Document
		static class SimilarityHolingBean {

			@Indexed @Field("norm") String normalProperty;
			@Field("similarityL") private List<SimilaritySibling> listOfSimilarilyNamedEntities = null;
		}

		static class SimilaritySibling {
			@Field("similarity") private String similarThoughNotEqualNamedProperty;
		}

		@Document
		static class MultipleObjectsOfSameType {

			SelfCyclingViaCollectionType cycleOne;

			SelfCyclingViaCollectionType cycleTwo;
		}

		@Document
		static class SelfCyclingViaCollectionType {

			List<SelfCyclingViaCollectionType> cyclic;

		}

		@Document
		@CompoundIndex(name = "c_index", def = "{ foo:1, bar:1 }")
		static class DocumentWithNamedCompoundIndex {

			String property;
		}

		@Document
		static class DocumentWithNamedIndex {

			@Indexed(name = "property_index") String property;
		}

		static class TypeWithNamedIndex {

			@Indexed(name = "property_index") String property;
		}

		@Document
		static class DocumentWithNestedDocumentHavingNamedCompoundIndex {

			DocumentWithNamedCompoundIndex propertyOfTypeHavingNamedCompoundIndex;
		}

		@CompoundIndex(name = "c_index", def = "{ foo:1, bar:1 }")
		static class TypeWithNamedCompoundIndex {
			String property;
		}

		@Document
		static class DocumentWithNestedTypeHavingNamedCompoundIndex {

			TypeWithNamedCompoundIndex propertyOfTypeHavingNamedCompoundIndex;
		}

		@Document
		static class DocumentWithNestedDocumentHavingNamedIndex {

			DocumentWithNamedIndex propertyOfTypeHavingNamedIndex;
		}

		@Document
		static class DocumentWithNestedTypeHavingNamedIndex {

			TypeWithNamedIndex propertyOfTypeHavingNamedIndex;
		}

		@Document
		public class MultiplePropertiesOfSameTypeWithMatchingStartLetters {

			public class NameComponent {

				@Indexed String component;
			}

			NameComponent name;
			NameComponent nameLast;
		}

		@Document
		public class MultiplePropertiesOfSameTypeWithMatchingStartLettersOnNestedProperty {

			public class NameComponent {

				@Indexed String nameLast;
				@Indexed String name;
			}

			NameComponent component;
		}

		@Document
		public static class OuterDocumentReferingToIndexedPropertyViaDifferentNonCyclingPaths {

			NoCycleButIndenticallNamedPropertiesDeeplyNested path1;
			AlternatePathToNoCycleButIndenticallNamedPropertiesDeeplyNestedDocument path2;
		}

		public static class AlternatePathToNoCycleButIndenticallNamedPropertiesDeeplyNestedDocument {
			NoCycleButIndenticallNamedPropertiesDeeplyNested propertyWithIndexedStructure;
		}

		static class GenericEntityWrapper<T> {
			T entity;
		}

		@Document
		static class EntityWithGenericTypeWrapperAsElement {
			List<GenericEntityWrapper<DocumentWithNamedIndex>> listWithGeneircTypeElement;
		}

	}

	private static List<IndexDefinitionHolder> prepareMappingContextAndResolveIndexForType(Class<?> type) {

		MongoMappingContext mappingContext = prepareMappingContext(type);
		MongoPersistentEntityIndexResolver resolver = new MongoPersistentEntityIndexResolver(mappingContext);
		return resolver.resolveIndexForEntity(mappingContext.getRequiredPersistentEntity(type));
	}

	private static MongoMappingContext prepareMappingContext(Class<?> type) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(Collections.singleton(type));
		mappingContext.initialize();

		return mappingContext;
	}

	private static void assertIndexPathAndCollection(String expectedPath, String expectedCollection,
			IndexDefinitionHolder holder) {
		assertIndexPathAndCollection(new String[] { expectedPath }, expectedCollection, holder);
	}

	private static void assertIndexPathAndCollection(String[] expectedPaths, String expectedCollection,
			IndexDefinitionHolder holder) {

		for (String expectedPath : expectedPaths) {
			assertThat(holder.getIndexDefinition().getIndexKeys().containsKey(expectedPath), equalTo(true));
		}

		assertThat(holder.getCollection(), equalTo(expectedCollection));
	}
}
