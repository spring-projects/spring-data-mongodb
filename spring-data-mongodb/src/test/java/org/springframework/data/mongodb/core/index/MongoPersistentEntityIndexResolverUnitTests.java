/*
 * Copyright 2014-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.test.util.Assertions.assertThatExceptionOfType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.springframework.core.annotation.AliasFor;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mapping.MappingException;
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
import org.springframework.data.mongodb.core.mapping.Unwrapped;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Tests for {@link MongoPersistentEntityIndexResolver}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Dave Perryman
 * @author Stefan Tirea
 */
@RunWith(Suite.class)
@SuiteClasses({ IndexResolutionTests.class, GeoSpatialIndexResolutionTests.class, CompoundIndexResolutionTests.class,
		TextIndexedResolutionTests.class, MixedIndexResolutionTests.class })
@SuppressWarnings("unused")
public class MongoPersistentEntityIndexResolverUnitTests {

	/**
	 * Test resolution of {@link Indexed}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 */
	public static class IndexResolutionTests {

		@Test // DATAMONGO-899
		public void indexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnLevelZero.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection("indexedProperty", "Zero", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void indexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelOne.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection("zero.indexedProperty", "One", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899, DATAMONGO-2188
		public void shouldResolveIndexViaClass() {

			MongoMappingContext mappingContext = new MongoMappingContext();
			IndexResolver indexResolver = IndexResolver.create(mappingContext);
			Iterable<? extends IndexDefinition> definitions = indexResolver.resolveIndexFor(IndexOnLevelOne.class);

			assertThat(definitions).isNotEmpty();
		}

		@Test // DATAMONGO-899
		public void deeplyNestedIndexPathIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelTwo.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection("one.zero.indexedProperty", "Two", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void resolvesIndexPathNameForNamedPropertiesCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnLevelOneWithExplicitlyNamedField.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection("customZero.customFieldName", "indexOnLevelOneWithExplicitlyNamedField",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void resolvesIndexDefinitionCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnLevelZero.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions()).isEqualTo(new org.bson.Document("name", "indexedProperty"));
		}

		@Test // DATAMONGO-899
		public void resolvesIndexDefinitionOptionsCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithOptionsOnIndexedProperty.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions()).isEqualTo(new org.bson.Document().append("name", "indexedProperty")
					.append("unique", true).append("sparse", true).append("background", true).append("expireAfterSeconds", 10L));
		}

		@Test // DATAMONGO-1297
		public void resolvesIndexOnDbrefWhenDefined() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WithDbRef.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0).getCollection()).isEqualTo("withDbRef");
			assertThat(indexDefinitions.get(0).getIndexKeys()).isEqualTo(new org.bson.Document("indexedDbRef", 1));
		}

		@Test // DATAMONGO-1297
		public void resolvesIndexOnDbrefWhenDefinedOnNestedElement() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WrapperOfWithDbRef.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0).getCollection()).isEqualTo("wrapperOfWithDbRef");
			assertThat(indexDefinitions.get(0).getIndexKeys()).isEqualTo(new org.bson.Document("nested.indexedDbRef", 1));
		}

		@Test // DATAMONGO-1163
		public void resolveIndexDefinitionInMetaAnnotatedFields() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnMetaAnnotatedField.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0).getCollection()).isEqualTo("indexOnMetaAnnotatedField");
			assertThat(indexDefinitions.get(0).getIndexOptions()).isEqualTo(new org.bson.Document("name", "_name"));
		}

		@Test // DATAMONGO-1373
		public void resolveIndexDefinitionInComposedAnnotatedFields() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexedDocumentWithComposedAnnotations.class);

			assertThat(indexDefinitions).hasSize(2);

			IndexDefinitionHolder indexDefinitionHolder = indexDefinitions.get(1);

			assertThat(indexDefinitionHolder.getIndexKeys()).containsEntry("fieldWithMyIndexName", 1);
			assertThat(indexDefinitionHolder.getIndexOptions()) //
					.containsEntry("sparse", true) //
					.containsEntry("unique", true) //
					.containsEntry("name", "my_index_name");
		}

		@Test // DATAMONGO-1373
		public void resolveIndexDefinitionInCustomComposedAnnotatedFields() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexedDocumentWithComposedAnnotations.class);

			assertThat(indexDefinitions).hasSize(2);

			IndexDefinitionHolder indexDefinitionHolder = indexDefinitions.get(0);

			assertThat(indexDefinitionHolder.getIndexKeys()).containsEntry("fieldWithDifferentIndexName", 1);
			assertThat(indexDefinitionHolder.getIndexOptions()) //
					.containsEntry("sparse", true) //
					.containsEntry("name", "different_name") //
					.doesNotContainKey("unique");
		}

		@Test // DATAMONGO-2112
		public void shouldResolveTimeoutFromString() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithExpireAfterAsPlainString.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("expireAfterSeconds", 600L);
		}

		@Test // DATAMONGO-2112
		public void shouldResolveTimeoutFromIso8601String() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithIso8601Style.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("expireAfterSeconds", 86400L);
		}

		@Test // DATAMONGO-2112
		public void shouldResolveTimeoutFromExpression() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithExpireAfterAsExpression.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("expireAfterSeconds", 11L);
		}

		@Test // DATAMONGO-2112
		public void shouldResolveTimeoutFromExpressionReturningDuration() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithExpireAfterAsExpressionResultingInDuration.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("expireAfterSeconds", 100L);
		}

		@Test // DATAMONGO-2112
		public void shouldErrorOnInvalidTimeoutExpression() {

			MongoMappingContext mappingContext = prepareMappingContext(WithInvalidExpireAfter.class);
			MongoPersistentEntityIndexResolver indexResolver = new MongoPersistentEntityIndexResolver(mappingContext);

			assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> indexResolver
					.resolveIndexForEntity(mappingContext.getRequiredPersistentEntity(WithInvalidExpireAfter.class)));
		}

		@Test // DATAMONGO-2112
		public void shouldErrorOnDuplicateTimeoutExpression() {

			MongoMappingContext mappingContext = prepareMappingContext(WithDuplicateExpiry.class);
			MongoPersistentEntityIndexResolver indexResolver = new MongoPersistentEntityIndexResolver(mappingContext);

			assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> indexResolver
					.resolveIndexForEntity(mappingContext.getRequiredPersistentEntity(WithDuplicateExpiry.class)));
		}

		@Test // DATAMONGO-2112
		public void resolveExpressionIndexName() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithIndexNameAsExpression.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "my1st");
		}

		@Test // DATAMONGO-1569
		public void resolvesPartialFilter() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithPartialFilter.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("partialFilterExpression",
					org.bson.Document.parse("{'value': {'$exists': true}}"));
		}

		@Document("Zero")
		class IndexOnLevelZero {
			@Indexed String indexedProperty;
		}

		@Document("One")
		class IndexOnLevelOne {
			IndexOnLevelZero zero;
		}

		@Document("Two")
		class IndexOnLevelTwo {
			IndexOnLevelOne one;
		}

		@Document("WithOptionsOnIndexedProperty")
		class WithOptionsOnIndexedProperty {

			@Indexed(background = true, direction = IndexDirection.DESCENDING, expireAfterSeconds = 10, sparse = true,
					unique = true) //
			String indexedProperty;
		}

		@Document
		class IndexOnLevelOneWithExplicitlyNamedField {

			@Field("customZero") IndexOnLevelZeroWithExplicityNamedField zero;
		}

		class IndexOnLevelZeroWithExplicityNamedField {

			@Indexed @Field("customFieldName") String namedProperty;
		}

		@Document
		class WrapperOfWithDbRef {
			WithDbRef nested;
		}

		@Document
		class WithDbRef {

			@Indexed //
			@DBRef //
			NoIndex indexedDbRef;
		}

		@Document("no-index")
		class NoIndex {
			@Id String id;
		}

		@Document
		class IndexedDocumentWithComposedAnnotations {

			@Id String id;
			@CustomIndexedAnnotation String fieldWithDifferentIndexName;
			@ComposedIndexedAnnotation String fieldWithMyIndexName;
		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ ElementType.FIELD })
		@ComposedIndexedAnnotation(indexName = "different_name", beUnique = false)
		@interface CustomIndexedAnnotation {
		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
		@Indexed
		@interface ComposedIndexedAnnotation {

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
		@interface ComposedFieldAnnotation {

			@AliasFor(annotation = org.springframework.data.mongodb.core.mapping.Field.class, attribute = "value")
			String name() default "_id";
		}

		@Document
		class WithExpireAfterAsPlainString {
			@Indexed(expireAfter = "10m") String withTimeout;
		}

		@Document
		class WithIso8601Style {
			@Indexed(expireAfter = "P1D") String withTimeout;
		}

		@Document
		class WithExpireAfterAsExpression {
			@Indexed(expireAfter = "#{10 + 1 + 's'}") String withTimeout;
		}

		@Document
		class WithExpireAfterAsExpressionResultingInDuration {
			@Indexed(expireAfter = "#{T(java.time.Duration).ofSeconds(100)}") String withTimeout;
		}

		@Document
		class WithInvalidExpireAfter {
			@Indexed(expireAfter = "123ops") String withTimeout;
		}

		@Document
		class WithDuplicateExpiry {
			@Indexed(expireAfter = "1s", expireAfterSeconds = 2) String withTimeout;
		}

		@Document
		class WithIndexNameAsExpression {
			@Indexed(name = "#{'my' + 1 + 'st'}") String spelIndexName;
		}

		@Document
		class WithPartialFilter {
			@Indexed(partialFilter = "{'value': {'$exists': true}}") String withPartialFilter;
		}
	}

	@Target({ ElementType.FIELD })
	@Retention(RetentionPolicy.RUNTIME)
	@Indexed
	@interface IndexedFieldAnnotation {
	}

	@Document
	class IndexOnMetaAnnotatedField {
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

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection("geoIndexedProperty", "Zero", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void geoSpatialIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					GeoSpatialIndexOnLevelOne.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection("zero.geoIndexedProperty", "One", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void depplyNestedGeoSpatialIndexPathIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					GeoSpatialIndexOnLevelTwo.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection("one.zero.geoIndexedProperty", "Two", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void resolvesIndexDefinitionOptionsCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithOptionsOnGeoSpatialIndexProperty.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();

			assertThat(indexDefinition.getIndexOptions()).isEqualTo(
					new org.bson.Document().append("name", "location").append("min", 1).append("max", 100).append("bits", 2));
		}

		@Test // DATAMONGO-1373
		public void resolvesComposedAnnotationIndexDefinitionOptionsCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					GeoSpatialIndexedDocumentWithComposedAnnotation.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();

			assertThat(indexDefinition.getIndexKeys()).containsEntry("location", "geoHaystack").containsEntry("What light?",
					1);
			assertThat(indexDefinition.getIndexOptions()).containsEntry("name", "my_geo_index_name")
					.containsEntry("bucketSize", 2.0);
		}

		@Test // DATAMONGO-2112
		public void resolveExpressionIndexNameForGeoIndex() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					GeoIndexWithNameAsExpression.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "my1st");
		}

		@Document("Zero")
		class GeoSpatialIndexOnLevelZero {
			@GeoSpatialIndexed Point geoIndexedProperty;
		}

		@Document("One")
		class GeoSpatialIndexOnLevelOne {
			GeoSpatialIndexOnLevelZero zero;
		}

		@Document("Two")
		class GeoSpatialIndexOnLevelTwo {
			GeoSpatialIndexOnLevelOne one;
		}

		@Document("WithOptionsOnGeoSpatialIndexProperty")
		class WithOptionsOnGeoSpatialIndexProperty {

			@GeoSpatialIndexed(bits = 2, max = 100, min = 1, type = GeoSpatialIndexType.GEO_2D) //
			Point location;
		}

		@Document("WithComposedAnnotation")
		class GeoSpatialIndexedDocumentWithComposedAnnotation {

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

		@Document
		class GeoIndexWithNameAsExpression {
			@GeoSpatialIndexed(name = "#{'my' + 1 + 'st'}") Point spelIndexName;
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

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "CompoundIndexOnLevelZero", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-899
		public void compoundIndexOptionsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexOnLevelZero.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions()).isEqualTo(new org.bson.Document("name", "compound_index")
					.append("unique", true).append("sparse", true).append("background", true));
			assertThat(indexDefinition.getIndexKeys()).isEqualTo(new org.bson.Document().append("foo", 1).append("bar", -1));
		}

		@Test // DATAMONGO-909
		public void compoundIndexOnSuperClassResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexDefinedOnSuperClass.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions()).isEqualTo(new org.bson.Document().append("name", "compound_index")
					.append("unique", true).append("sparse", true).append("background", true));
			assertThat(indexDefinition.getIndexKeys()).isEqualTo(new org.bson.Document().append("foo", 1).append("bar", -1));
		}

		@Test // DATAMONGO-827
		public void compoundIndexDoesNotSpecifyNameWhenUsingGenerateName() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					ComountIndexWithAutogeneratedName.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions())
					.isEqualTo(new org.bson.Document().append("unique", true).append("sparse", true).append("background", true));
			assertThat(indexDefinition.getIndexKeys()).isEqualTo(new org.bson.Document().append("foo", 1).append("bar", -1));
		}

		@Test // DATAMONGO-929
		public void compoundIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexOnLevelOne.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "zero.foo", "zero.bar" }, "CompoundIndexOnLevelOne",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-929
		public void emptyCompoundIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexOnLevelOneWithEmptyIndexDefinition.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "zero" }, "CompoundIndexOnLevelZeroWithEmptyIndexDef",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-929
		public void singleCompoundIndexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					SingleCompoundIndex.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "CompoundIndexOnLevelZero", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-1373
		public void singleCompoundIndexUsingComposedAnnotationsOnTypeResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexDocumentWithComposedAnnotation.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0).getIndexKeys()).containsEntry("foo", 1).containsEntry("bar", -1);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "my_compound_index_name")
					.containsEntry("unique", true).containsEntry("background", true);
		}

		@Test // DATAMONGO-2112
		public void resolveExpressionIndexNameForCompoundIndex() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexWithNameExpression.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "cmp2name");
		}

		@Test // DATAMONGO-2112
		public void resolveExpressionDefForCompoundIndex() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexWithDefExpression.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "compoundIndexWithDefExpression",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-2067
		public void shouldIdentifyRepeatedAnnotationCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					RepeatedCompoundIndex.class);

			assertThat(indexDefinitions).hasSize(2);
			assertIndexPathAndCollection(new String[] { "firstname", "lastname" }, "repeatedCompoundIndex",
					indexDefinitions.get(0));
			assertIndexPathAndCollection(new String[] { "address.city", "address.street" }, "repeatedCompoundIndex",
					indexDefinitions.get(1));
		}

		@Test // DATAMONGO-1569
		public void singleIndexWithPartialFilter() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					SingleCompoundIndexWithPartialFilter.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0).getIndexKeys()).containsEntry("foo", 1).containsEntry("bar", -1);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "compound_index_with_partial")
					.containsEntry("unique", true).containsEntry("background", true);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("partialFilterExpression",
					org.bson.Document.parse("{'value': {'$exists': true}}"));
		}

		@Test // GH-3002
		public void compoundIndexWithCollation() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CompoundIndexWithCollation.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions())
					.isEqualTo(new org.bson.Document().append("name", "compound_index_with_collation").append("collation",
							new org.bson.Document().append("locale", "en_US").append("strength", 2)));
			assertThat(indexDefinition.getIndexKeys()).isEqualTo(new org.bson.Document().append("foo", 1));
		}

		@Test // GH-3002
		public void compoundIndexWithCollationFromDocumentAnnotation() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithCompoundCollationFromDocument.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions())
					.isEqualTo(new org.bson.Document().append("name", "compound_index_with_collation").append("collation",
							new org.bson.Document().append("locale", "en_US").append("strength", 2)));
			assertThat(indexDefinition.getIndexKeys()).isEqualTo(new org.bson.Document().append("foo", 1));
		}

		@Test // GH-3002
		public void compoundIndexWithEvaluatedCollationFromAnnotation() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithEvaluatedCollationFromCompoundIndex.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions())
					.isEqualTo(new org.bson.Document().append("name", "compound_index_with_collation").append("collation",
							new org.bson.Document().append("locale", "de_AT")));
			assertThat(indexDefinition.getIndexKeys()).isEqualTo(new org.bson.Document().append("foo", 1));
		}

		@Document("CompoundIndexOnLevelOne")
		class CompoundIndexOnLevelOne {

			CompoundIndexOnLevelZero zero;
		}

		@Document("CompoundIndexOnLevelZeroWithEmptyIndexDef")
		class CompoundIndexOnLevelOneWithEmptyIndexDefinition {

			CompoundIndexOnLevelZeroWithEmptyIndexDef zero;
		}

		@Document("CompoundIndexOnLevelZero")
		@CompoundIndexes({ @CompoundIndex(name = "compound_index", def = "{'foo': 1, 'bar': -1}", background = true,
				sparse = true, unique = true) })
		class CompoundIndexOnLevelZero {}

		@CompoundIndexes({ @CompoundIndex(name = "compound_index", background = true, sparse = true, unique = true) })
		class CompoundIndexOnLevelZeroWithEmptyIndexDef {}

		@Document("CompoundIndexOnLevelZero")
		@CompoundIndex(name = "compound_index", def = "{'foo': 1, 'bar': -1}", background = true, sparse = true,
				unique = true)
		class SingleCompoundIndex {}

		class IndexDefinedOnSuperClass extends CompoundIndexOnLevelZero {}

		@Document("ComountIndexWithAutogeneratedName")
		@CompoundIndexes({ @CompoundIndex(useGeneratedName = true, def = "{'foo': 1, 'bar': -1}", background = true,
				sparse = true, unique = true) })
		class ComountIndexWithAutogeneratedName {}

		@Document("WithComposedAnnotation")
		@ComposedCompoundIndex
		class CompoundIndexDocumentWithComposedAnnotation {}

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

		@Document
		@CompoundIndex(name = "#{'cmp' + 2 + 'name'}", def = "{'foo': 1, 'bar': -1}")
		class CompoundIndexWithNameExpression {}

		@Document
		@CompoundIndex(def = "#{T(org.bson.Document).parse(\"{ 'foo': 1, 'bar': -1 }\")}")
		class CompoundIndexWithDefExpression {}

		@Document
		@CompoundIndex(name = "cmp-idx-one", def = "{'firstname': 1, 'lastname': -1}")
		@CompoundIndex(name = "cmp-idx-two", def = "{'address.city': -1, 'address.street': 1}")
		class RepeatedCompoundIndex {}

		@Document("SingleCompoundIndexWithPartialFilter")
		@CompoundIndex(name = "compound_index_with_partial", def = "{'foo': 1, 'bar': -1}", background = true,
				unique = true, partialFilter = "{'value': {'$exists': true}}")
		class SingleCompoundIndexWithPartialFilter {}

		@Document
		@CompoundIndex(name = "compound_index_with_collation", def = "{'foo': 1}",
				collation = "{'locale': 'en_US', 'strength': 2}")
		class CompoundIndexWithCollation {}

		@Document(collation = "{'locale': 'en_US', 'strength': 2}")
		@CompoundIndex(name = "compound_index_with_collation", def = "{'foo': 1}")
		class WithCompoundCollationFromDocument {}

		@Document(collation = "{'locale': 'en_US', 'strength': 2}")
		@CompoundIndex(name = "compound_index_with_collation", def = "{'foo': 1}", collation = "#{{ 'locale' : 'de' + '_' + 'AT' }}")
		class WithEvaluatedCollationFromCompoundIndex {}
	}

	public static class TextIndexedResolutionTests {

		@Test // DATAMONGO-937
		public void shouldResolveSingleFieldTextIndexCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnSinglePropertyInRoot.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection("bar", "textIndexOnSinglePropertyInRoot", indexDefinitions.get(0));
			assertThat(indexDefinitions.get(0).getIndexOptions()).doesNotContainKey("collation");
		}

		@Test // DATAMONGO-2316
		public void shouldEnforceSimpleCollationOnTextIndex() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexWithCollation.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("collation",
					new org.bson.Document("locale", "simple"));
		}

		@Test // DATAMONGO-937
		public void shouldResolveMultiFieldTextIndexCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnMultiplePropertiesInRoot.class);

			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "textIndexOnMultiplePropertiesInRoot",
					indexDefinitions.get(0));
		}

		@Test // DATAMONGO-937
		public void shouldResolveTextIndexOnElementCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnNestedRoot.class);
			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "nested.foo" }, "textIndexOnNestedRoot", indexDefinitions.get(0));
		}

		@Test // DATAMONGO-937
		public void shouldResolveTextIndexOnElementWithWeightCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnNestedWithWeightRoot.class);
			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "nested.foo" }, "textIndexOnNestedWithWeightRoot",
					indexDefinitions.get(0));

			org.bson.Document weights = DocumentTestUtils.getAsDocument(indexDefinitions.get(0).getIndexOptions(), "weights");
			assertThat(weights.get("nested.foo")).isEqualTo(5F);
		}

		@Test // DATAMONGO-937
		public void shouldResolveTextIndexOnElementWithMostSpecificWeightCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexOnNestedWithMostSpecificValueRoot.class);
			assertThat(indexDefinitions).hasSize(1);
			assertIndexPathAndCollection(new String[] { "nested.foo", "nested.bar" },
					"textIndexOnNestedWithMostSpecificValueRoot", indexDefinitions.get(0));

			org.bson.Document weights = DocumentTestUtils.getAsDocument(indexDefinitions.get(0).getIndexOptions(), "weights");
			assertThat(weights.get("nested.foo")).isEqualTo(5F);
			assertThat(weights.get("nested.bar")).isEqualTo(10F);
		}

		@Test // DATAMONGO-937
		public void shouldSetDefaultLanguageCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithDefaultLanguage.class);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("default_language", "spanish");
		}

		@Test // DATAMONGO-937, DATAMONGO-1049
		public void shouldResolveTextIndexLanguageOverrideCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithLanguageOverride.class);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("language_override", "lang");
		}

		@Test // DATAMONGO-1049
		public void shouldIgnoreTextIndexLanguageOverrideOnNestedElements() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithLanguageOverrideOnNestedElement.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("language_override")).isNull();
		}

		@Test // DATAMONGO-1049
		public void shouldNotCreateIndexDefinitionWhenOnlyLanguageButNoTextIndexPresent() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNoTextIndexPropertyButReservedFieldLanguage.class);

			assertThat(indexDefinitions).isEmpty();
		}

		@Test // DATAMONGO-1049
		public void shouldNotCreateIndexDefinitionWhenOnlyAnnotatedLanguageButNoTextIndexPresent() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNoTextIndexPropertyButReservedFieldLanguageAnnotated.class);

			assertThat(indexDefinitions).isEmpty();
		}

		@Test // DATAMONGO-1049
		public void shouldPreferExplicitlyAnnotatedLanguageProperty() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithOverlappingLanguageProps.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("language_override", "lang");
		}

		@Test // DATAMONGO-1373
		public void shouldResolveComposedAnnotationCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					TextIndexedDocumentWithComposedAnnotation.class);

			org.bson.Document weights = DocumentTestUtils.getAsDocument(indexDefinitions.get(0).getIndexOptions(), "weights");
			assertThat(weights).containsEntry("foo", 99f);
		}

		@Document
		class TextIndexOnSinglePropertyInRoot {

			String foo;

			@TextIndexed String bar;
		}

		@Document(collation = "de_AT")
		class TextIndexWithCollation {

			@TextIndexed String foo;
		}

		@Document
		class TextIndexOnMultiplePropertiesInRoot {

			@TextIndexed String foo;

			@TextIndexed(weight = 5) String bar;
		}

		@Document
		class TextIndexOnNestedRoot {

			String bar;

			@TextIndexed TextIndexOnNested nested;
		}

		class TextIndexOnNested {

			String foo;
		}

		@Document
		class TextIndexOnNestedWithWeightRoot {

			@TextIndexed(weight = 5) TextIndexOnNested nested;
		}

		@Document
		class TextIndexOnNestedWithMostSpecificValueRoot {
			@TextIndexed(weight = 5) TextIndexOnNestedWithMostSpecificValue nested;
		}

		class TextIndexOnNestedWithMostSpecificValue {

			String foo;
			@TextIndexed(weight = 10) String bar;
		}

		@Document(language = "spanish")
		class DocumentWithDefaultLanguage {
			@TextIndexed String foo;
		}

		@Document
		class DocumentWithLanguageOverrideOnNestedElement {

			DocumentWithLanguageOverride nested;
		}

		@Document
		class DocumentWithLanguageOverride {

			@TextIndexed String foo;

			@Language String lang;
		}

		@Document
		class DocumentWithNoTextIndexPropertyButReservedFieldLanguage {

			String language;
		}

		@Document
		class DocumentWithNoTextIndexPropertyButReservedFieldLanguageAnnotated {

			@Field("language") String lang;
		}

		@Document
		class DocumentWithOverlappingLanguageProps {

			@TextIndexed String foo;
			String language;
			@Language String lang;
		}

		@Document
		class TextIndexedDocumentWithComposedAnnotation {

			@ComposedTextIndexedAnnotation String foo;
			String lang;
		}

		@Retention(RetentionPolicy.RUNTIME)
		@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
		@TextIndexed
		@interface ComposedTextIndexedAnnotation {

			@AliasFor(annotation = TextIndexed.class, attribute = "weight")
			float heavyweight() default 99f;
		}
	}

	public static class MixedIndexResolutionTests {

		@Test // DATAMONGO-899
		public void multipleIndexesResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(MixedIndexRoot.class);

			assertThat(indexDefinitions).hasSize(2);
			assertThat(indexDefinitions.get(0).getIndexDefinition()).isInstanceOf(Index.class);
			assertThat(indexDefinitions.get(1).getIndexDefinition()).isInstanceOf(GeospatialIndex.class);
		}

		@Test // DATAMONGO-899
		public void cyclicPropertyReferenceOverDBRefShouldNotBeTraversed() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(Inner.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0).getIndexDefinition().getIndexKeys())
					.isEqualTo(new org.bson.Document().append("outer", 1));
		}

		@Test // DATAMONGO-899
		public void associationsShouldNotBeTraversed() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(Outer.class);

			assertThat(indexDefinitions).isEmpty();
		}

		@Test // DATAMONGO-926
		public void shouldNotRunIntoStackOverflow() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					CycleStartingInBetween.class);

			assertThat(indexDefinitions).hasSize(1);
		}

		@Test // DATAMONGO-926
		public void indexShouldBeFoundEvenForCyclePropertyReferenceOnLevelZero() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CycleLevelZero.class);
			assertIndexPathAndCollection("indexedProperty", "cycleLevelZero", indexDefinitions.get(0));
			assertIndexPathAndCollection("cyclicReference.indexedProperty", "cycleLevelZero", indexDefinitions.get(1));
			assertThat(indexDefinitions).hasSize(2);
		}

		@Test // DATAMONGO-926
		public void indexShouldBeFoundEvenForCyclePropertyReferenceOnLevelOne() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CycleOnLevelOne.class);
			assertIndexPathAndCollection("reference.indexedProperty", "cycleOnLevelOne", indexDefinitions.get(0));
			assertThat(indexDefinitions).hasSize(1);
		}

		@Test // DATAMONGO-926
		public void indexBeResolvedCorrectlyWhenPropertiesOfDifferentTypesAreNamedEqually() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					NoCycleButIdenticallyNamedProperties.class);

			assertThat(indexDefinitions).hasSize(3);
			assertIndexPathAndCollection("foo", "noCycleButIdenticallyNamedProperties", indexDefinitions.get(0));
			assertIndexPathAndCollection("reference.foo", "noCycleButIdenticallyNamedProperties", indexDefinitions.get(1));
			assertIndexPathAndCollection("reference.deep.foo", "noCycleButIdenticallyNamedProperties",
					indexDefinitions.get(2));
		}

		@Test // DATAMONGO-949
		public void shouldNotDetectCycleInSimilarlyNamedProperties() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					SimilarityHolingBean.class);
			assertIndexPathAndCollection("norm", "similarityHolingBean", indexDefinitions.get(0));
			assertThat(indexDefinitions).hasSize(1);
		}

		@Test // DATAMONGO-962
		public void shouldDetectSelfCycleViaCollectionTypeCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					SelfCyclingViaCollectionType.class);

			assertThat(indexDefinitions).isEmpty();
		}

		@Test // DATAMONGO-962
		public void shouldNotDetectCycleWhenTypeIsUsedMoreThanOnce() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					MultipleObjectsOfSameType.class);

			assertThat(indexDefinitions).isEmpty();
		}

		@Test // DATAMONGO-962
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void shouldCatchCyclicReferenceExceptionOnRoot() {

			MongoPersistentEntity entity = new BasicMongoPersistentEntity<>(ClassTypeInformation.from(Object.class));

			MongoPersistentProperty propertyMock = mock(MongoPersistentProperty.class);
			when(propertyMock.isEntity()).thenReturn(true);
			when(propertyMock.getOwner()).thenReturn(entity);
			when(propertyMock.getActualType()).thenThrow(
					new MongoPersistentEntityIndexResolver.CyclicPropertyReferenceException("foo", Object.class, "bar"));

			MongoPersistentEntity<SelfCyclingViaCollectionType> selfCyclingEntity = new BasicMongoPersistentEntity<>(
					ClassTypeInformation.from(SelfCyclingViaCollectionType.class));

			new MongoPersistentEntityIndexResolver(prepareMappingContext(SelfCyclingViaCollectionType.class))
					.resolveIndexForEntity(selfCyclingEntity);
		}

		@Test // DATAMONGO-1782
		public void shouldAllowMultiplePathsToDeeplyType() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					NoCycleManyPathsToDeepValueObject.class);

			assertThat(indexDefinitions).hasSize(2);
			assertIndexPathAndCollection("l3.valueObject.value", "rules", indexDefinitions.get(0));
			assertIndexPathAndCollection("l2.l3.valueObject.value", "rules", indexDefinitions.get(1));
		}

		@Test // DATAMONGO-1025
		public void shouldUsePathIndexAsIndexNameForDocumentsHavingNamedNestedCompoundIndexFixedOnCollection() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNestedDocumentHavingNamedCompoundIndex.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name",
					"propertyOfTypeHavingNamedCompoundIndex.c_index");
		}

		@Test // DATAMONGO-1025
		public void shouldUseIndexNameForNestedTypesWithNamedCompoundIndexDefinition() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNestedTypeHavingNamedCompoundIndex.class);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name",
					"propertyOfTypeHavingNamedCompoundIndex.c_index");
		}

		@Test // DATAMONGO-1025
		public void shouldUsePathIndexAsIndexNameForDocumentsHavingNamedNestedIndexFixedOnCollection() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNestedDocumentHavingNamedIndex.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name",
					"propertyOfTypeHavingNamedIndex.property_index");
		}

		@Test // DATAMONGO-1025
		public void shouldUseIndexNameForNestedTypesWithNamedIndexDefinition() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNestedTypeHavingNamedIndex.class);

			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name",
					"propertyOfTypeHavingNamedIndex.property_index");
		}

		@Test // DATAMONGO-1025
		public void shouldUseIndexNameOnRootLevel() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					DocumentWithNamedIndex.class);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "property_index");
		}

		@Test // DATAMONGO-1087
		public void shouldAllowMultiplePropertiesOfSameTypeWithMatchingStartLettersOnRoot() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					MultiplePropertiesOfSameTypeWithMatchingStartLetters.class);

			assertThat(indexDefinitions).hasSize(2);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "name.component");
			assertThat(indexDefinitions.get(1).getIndexOptions()).containsEntry("name", "nameLast.component");
		}

		@Test // DATAMONGO-1087
		public void shouldAllowMultiplePropertiesOfSameTypeWithMatchingStartLettersOnNestedProperty() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					MultiplePropertiesOfSameTypeWithMatchingStartLettersOnNestedProperty.class);

			assertThat(indexDefinitions).hasSize(2);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "component.nameLast");
			assertThat(indexDefinitions.get(1).getIndexOptions()).containsEntry("name", "component.name");
		}

		@Test // DATAMONGO-1121
		public void shouldOnlyConsiderEntitiesAsPotentialCycleCandidates() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					OuterDocumentReferingToIndexedPropertyViaDifferentNonCyclingPaths.class);

			assertThat(indexDefinitions).hasSize(2);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name", "path1.foo");
			assertThat(indexDefinitions.get(1).getIndexOptions()).containsEntry("name",
					"path2.propertyWithIndexedStructure.foo");
		}

		@Test // DATAMONGO-1263
		public void shouldConsiderGenericTypeArgumentsOfCollectionElements() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					EntityWithGenericTypeWrapperAsElement.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0).getIndexOptions()).containsEntry("name",
					"listWithGeneircTypeElement.entity.property_index");
		}

		@Test // DATAMONGO-1183
		public void hashedIndexOnId() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithHashedIndexOnId.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).hasSize(1).containsEntry("_id", "hashed");
			});
		}

		@Test // DATAMONGO-1183
		public void hashedIndex() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WithHashedIndex.class);

			assertThat(indexDefinitions).hasSize(1);
			assertThat(indexDefinitions.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).hasSize(1).containsEntry("value", "hashed");
			});
		}

		@Test // DATAMONGO-1183
		public void hashedIndexAndIndex() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithHashedIndexAndIndex.class);

			assertThat(indexDefinitions).hasSize(2);
			assertThat(indexDefinitions.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("value", 1);
			});
			assertThat(indexDefinitions.get(1)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("value", "hashed");
			});
		}

		@Test // DATAMONGO-1183
		public void hashedIndexAndIndexViaComposedAnnotation() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithComposedHashedIndexAndIndex.class);

			assertThat(indexDefinitions).hasSize(2);
			assertThat(indexDefinitions.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("value", 1);
				assertThat(it.getIndexOptions()).containsEntry("name", "idx-name");
			});
			assertThat(indexDefinitions.get(1)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("value", "hashed");
			});
		}

		@Test // DATAMONGO-1902
		public void resolvedIndexOnUnwrappedType() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WithUnwrapped.class,
					UnwrappableType.class);

			assertThat(indexDefinitions).hasSize(2);
			assertThat(indexDefinitions.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("stringValue", 1);
			});
			assertThat(indexDefinitions.get(1)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("with-at-field-annotation", 1);
			});
		}

		@Test // DATAMONGO-1902
		public void resolvedIndexOnNestedUnwrappedType() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WrapperAroundWithUnwrapped.class, WithUnwrapped.class, UnwrappableType.class);

			assertThat(indexDefinitions).hasSize(2);
			assertThat(indexDefinitions.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("withEmbedded.stringValue", 1);
			});
			assertThat(indexDefinitions.get(1)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("withEmbedded.with-at-field-annotation", 1);
			});
		}

		@Test // DATAMONGO-1902
		public void errorsOnIndexOnEmbedded() {

			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> prepareMappingContextAndResolveIndexForType(InvalidIndexOnUnwrapped.class));

		}

		@Test // GH-3225
		public void resolvesWildcardOnRoot() {

			List<IndexDefinitionHolder> indices = prepareMappingContextAndResolveIndexForType(
					WithWildCardIndexOnEntity.class);
			assertThat(indices).hasSize(1);
			assertThat(indices.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("$**", 1);
				assertThat(it.getIndexOptions()).isEmpty();
			});
		}

		@Test // GH-3225
		public void resolvesWildcardWithProjectionOnRoot() {

			List<IndexDefinitionHolder> indices = prepareMappingContextAndResolveIndexForType(
					WithWildCardIndexHavingProjectionOnEntity.class);
			assertThat(indices).hasSize(1);
			assertThat(indices.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("$**", 1);
				assertThat(it.getIndexOptions()).containsEntry("wildcardProjection",
						org.bson.Document.parse("{'_id' : 1, 'value' : 0}"));
			});
		}

		@Test // GH-3225
		public void resolvesWildcardOnProperty() {

			List<IndexDefinitionHolder> indices = prepareMappingContextAndResolveIndexForType(
					WithWildCardIndexOnProperty.class);
			assertThat(indices).hasSize(3);
			assertThat(indices.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("value.$**", 1);
			});
			assertThat(indices.get(1)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("the_field.$**", 1);
			});
			assertThat(indices.get(2)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("withOptions.$**", 1);
				assertThat(it.getIndexOptions()).containsEntry("name", "withOptions.idx")
						.containsEntry("collation", new org.bson.Document("locale", "en_US"))
						.containsEntry("partialFilterExpression", new org.bson.Document("$eq", 1));
			});
		}

		@Test // GH-3225
		public void resolvesWildcardTypeOfNestedProperty() {

			List<IndexDefinitionHolder> indices = prepareMappingContextAndResolveIndexForType(
					WithWildCardOnEntityOfNested.class);
			assertThat(indices).hasSize(1);
			assertThat(indices.get(0)).satisfies(it -> {
				assertThat(it.getIndexKeys()).containsEntry("value.$**", 1);
				assertThat(it.getIndexOptions()).hasSize(1).containsKey("name");
			});
		}

		@Test // GH-3225
		public void rejectsWildcardProjectionOnNestedPaths() {

			assertThatExceptionOfType(MappingException.class).isThrownBy(() -> {
				prepareMappingContextAndResolveIndexForType(WildcardIndexedProjectionOnNestedPath.class);
			});
		}

		@Test // GH-3914
		public void shouldSkipMapStructuresUnlessAnnotatedWithWildcardIndex() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithMapStructures.class);

			assertThat(indexDefinitions).hasSize(1);
		}

		@Test // GH-3002
		public void indexedWithCollation() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithCollationFromIndexedAnnotation.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions()).isEqualTo(new org.bson.Document().append("name", "value")
					.append("unique", true)
					.append("collation", new org.bson.Document().append("locale", "en_US").append("strength", 2)));
		}

		@Test // GH-3002
		public void indexedWithCollationFromDocumentAnnotation() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithCollationFromDocumentAnnotation.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions()).isEqualTo(new org.bson.Document().append("name", "value")
					.append("unique", true)
					.append("collation", new org.bson.Document().append("locale", "en_US").append("strength", 2)));
		}

		@Test // GH-3002
		public void indexedWithEvaluatedCollation() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					WithEvaluatedCollationFromIndexedAnnotation.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions()).isEqualTo(new org.bson.Document().append("name", "value")
					.append("collation", new org.bson.Document().append("locale", "de_AT")));
		}

		@Document
		class MixedIndexRoot {

			@Indexed String first;
			NestedGeoIndex nestedGeo;
		}

		class NestedGeoIndex {

			@GeoSpatialIndexed Point location;
		}

		@Document
		class Outer {

			@DBRef Inner inner;
		}

		@Document
		class Inner {

			@Indexed Outer outer;
		}

		@Document
		class CycleLevelZero {

			@Indexed String indexedProperty;
			CycleLevelZero cyclicReference;
		}

		@Document
		class CycleOnLevelOne {

			CycleOnLevelOneReferenced reference;
		}

		class CycleOnLevelOneReferenced {

			@Indexed String indexedProperty;
			CycleOnLevelOne cyclicReference;
		}

		@Document
		static class CycleStartingInBetween {

			CycleOnLevelOne referenceToCycleStart;
		}

		@Document
		class NoCycleButIdenticallyNamedProperties {

			@Indexed String foo;
			NoCycleButIdenticallyNamedPropertiesNested reference;
		}

		class NoCycleButIdenticallyNamedPropertiesNested {

			@Indexed String foo;
			NoCycleButIndenticallNamedPropertiesDeeplyNested deep;
		}

		class NoCycleButIndenticallNamedPropertiesDeeplyNested {

			@Indexed String foo;
		}

		@Document("rules")
		class NoCycleManyPathsToDeepValueObject {

			private NoCycleLevel3 l3;
			private NoCycleLevel2 l2;
		}

		class NoCycleLevel2 {
			private NoCycleLevel3 l3;
		}

		class NoCycleLevel3 {
			private ValueObject valueObject;
		}

		class ValueObject {
			@Indexed private String value;
		}

		@Document
		class SimilarityHolingBean {

			@Indexed @Field("norm") String normalProperty;
			@Field("similarityL") private List<SimilaritySibling> listOfSimilarilyNamedEntities = null;
		}

		class SimilaritySibling {
			@Field("similarity") private String similarThoughNotEqualNamedProperty;
		}

		@Document
		class MultipleObjectsOfSameType {

			SelfCyclingViaCollectionType cycleOne;

			SelfCyclingViaCollectionType cycleTwo;
		}

		@Document
		class SelfCyclingViaCollectionType {

			List<SelfCyclingViaCollectionType> cyclic;

		}

		@Document
		@CompoundIndex(name = "c_index", def = "{ foo:1, bar:1 }")
		class DocumentWithNamedCompoundIndex {

			String property;
		}

		@Document
		class DocumentWithNamedIndex {

			@Indexed(name = "property_index") String property;
		}

		class TypeWithNamedIndex {

			@Indexed(name = "property_index") String property;
		}

		@Document
		class DocumentWithNestedDocumentHavingNamedCompoundIndex {

			DocumentWithNamedCompoundIndex propertyOfTypeHavingNamedCompoundIndex;
		}

		@CompoundIndex(name = "c_index", def = "{ foo:1, bar:1 }")
		class TypeWithNamedCompoundIndex {
			String property;
		}

		@Document
		class DocumentWithNestedTypeHavingNamedCompoundIndex {

			TypeWithNamedCompoundIndex propertyOfTypeHavingNamedCompoundIndex;
		}

		@Document
		class DocumentWithNestedDocumentHavingNamedIndex {

			DocumentWithNamedIndex propertyOfTypeHavingNamedIndex;
		}

		@Document
		class DocumentWithNestedTypeHavingNamedIndex {

			TypeWithNamedIndex propertyOfTypeHavingNamedIndex;
		}

		@Document
		class MultiplePropertiesOfSameTypeWithMatchingStartLetters {

			class NameComponent {

				@Indexed String component;
			}

			NameComponent name;
			NameComponent nameLast;
		}

		@Document
		class MultiplePropertiesOfSameTypeWithMatchingStartLettersOnNestedProperty {

			class NameComponent {

				@Indexed String nameLast;
				@Indexed String name;
			}

			NameComponent component;
		}

		@Document
		static class OuterDocumentReferingToIndexedPropertyViaDifferentNonCyclingPaths {

			NoCycleButIndenticallNamedPropertiesDeeplyNested path1;
			AlternatePathToNoCycleButIndenticallNamedPropertiesDeeplyNestedDocument path2;
		}

		@Document
		static class WrapperAroundWithUnwrapped {

			String id;
			WithUnwrapped withEmbedded;
		}

		@Document
		static class WithUnwrapped {

			String id;

			@Unwrapped.Nullable UnwrappableType unwrappableType;
		}

		@Document
		class InvalidIndexOnUnwrapped {

			@Indexed //
			@Unwrapped.Nullable //
			UnwrappableType unwrappableType;

		}

		static class UnwrappableType {

			@Indexed String stringValue;

			List<String> listValue;

			@Indexed //
			@Field("with-at-field-annotation") //
			String atFieldAnnotatedValue;
		}

		static class AlternatePathToNoCycleButIndenticallNamedPropertiesDeeplyNestedDocument {
			NoCycleButIndenticallNamedPropertiesDeeplyNested propertyWithIndexedStructure;
		}

		class GenericEntityWrapper<T> {
			T entity;
		}

		@Document
		class WithMapStructures {
			Map<String, ValueObject> rootMap;
			NestedInMapWithStructures nested;
			ValueObject plainValue;
		}

		class NestedInMapWithStructures {
			Map<String, ValueObject> nestedMap;
		}

		@Document
		class EntityWithGenericTypeWrapperAsElement {
			List<GenericEntityWrapper<DocumentWithNamedIndex>> listWithGeneircTypeElement;
		}

		@Document
		class WithHashedIndexOnId {

			@HashIndexed @Id String id;
		}

		@Document
		class WithHashedIndex {

			@HashIndexed String value;
		}

		@Document
		@WildcardIndexed
		class WithWildCardIndexOnEntity {

			String value;
		}

		@Document
		@WildcardIndexed(wildcardProjection = "{'_id' : 1, 'value' : 0}")
		class WithWildCardIndexHavingProjectionOnEntity {

			String value;
		}

		@Document
		class WithWildCardIndexOnProperty {

			@WildcardIndexed //
			Map<String, String> value;

			@WildcardIndexed //
			@Field("the_field") //
			Map<String, String> renamedField;

			@WildcardIndexed(name = "idx", partialFilter = "{ '$eq' : 1 }", collation = "en_US") //
			Map<String, String> withOptions;

		}

		@Document
		class WildcardIndexedProjectionOnNestedPath {

			@WildcardIndexed(wildcardProjection = "{}") String foo;
		}

		@Document
		class WithWildCardOnEntityOfNested {

			WithWildCardIndexHavingProjectionOnEntity value;

		}

		@Document
		class WithHashedIndexAndIndex {

			@Indexed //
			@HashIndexed //
			String value;
		}

		@Document
		class WithComposedHashedIndexAndIndex {

			@ComposedHashIndexed(name = "idx-name") String value;
		}

		@Document
		class WithCollationFromIndexedAnnotation {

			@Indexed(collation = "{'locale': 'en_US', 'strength': 2}", unique = true) //
			private String value;
		}

		@Document(collation = "{'locale': 'en_US', 'strength': 2}")
		class WithCollationFromDocumentAnnotation {

			@Indexed(unique = true) //
			private String value;
		}

		@Document(collation = "en_US")
		class WithEvaluatedCollationFromIndexedAnnotation {

			@Indexed(collation = "#{{'locale' :  'de' + '_' + 'AT'}}") //
			private String value;
		}

		@HashIndexed
		@Indexed
		@Retention(RetentionPolicy.RUNTIME)
		@interface ComposedHashIndexed {

			@AliasFor(annotation = Indexed.class, attribute = "name")
			String name() default "";
		}
	}

	private static List<IndexDefinitionHolder> prepareMappingContextAndResolveIndexForType(Class<?>... types) {

		MongoMappingContext mappingContext = prepareMappingContext(types);
		MongoPersistentEntityIndexResolver resolver = new MongoPersistentEntityIndexResolver(mappingContext);
		return resolver.resolveIndexForEntity(mappingContext.getRequiredPersistentEntity(types[0]));
	}

	private static MongoMappingContext prepareMappingContext(Class<?>... types) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new LinkedHashSet<>(Arrays.asList(types)));
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
			assertThat(holder.getIndexDefinition().getIndexKeys()).containsKey(expectedPath);
		}

		assertThat(holder.getCollection()).isEqualTo(expectedCollection);
	}
}
