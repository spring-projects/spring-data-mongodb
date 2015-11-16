/*
 * Copyright 2014-2015 the original author or authors.
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
import org.springframework.data.annotation.Id;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.DBObjectTestUtils;
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

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
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

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void indexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelZero.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("indexedProperty", "Zero", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void indexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelOne.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("zero.indexedProperty", "One", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void depplyNestedIndexPathIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelTwo.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("one.zero.indexedProperty", "Two", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void resolvesIndexPathNameForNamedPropertiesCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelOneWithExplicitlyNamedField.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("customZero.customFieldName", "indexOnLevelOneWithExplicitlyNamedField",
					indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void resolvesIndexDefinitionCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexOnLevelZero.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(), equalTo(new BasicDBObjectBuilder().add("name", "indexedProperty")
					.get()));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void resolvesIndexDefinitionOptionsCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WithOptionsOnIndexedProperty.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(),
					equalTo(new BasicDBObjectBuilder().add("name", "indexedProperty").add("unique", true).add("dropDups", true)
							.add("sparse", true).add("background", true).add("expireAfterSeconds", 10L).get()));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void resolvesIndexCollectionNameCorrectlyWhenDefinedInAnnotation() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WithOptionsOnIndexedProperty.class);
			assertThat(indexDefinitions.get(0).getCollection(), equalTo("CollectionOverride"));
		}

		/**
		 * @see DATAMONGO-1297
		 */
		@Test
		public void resolvesIndexOnDbrefWhenDefined() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WithDbRef.class);

			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getCollection(), equalTo("withDbRef"));
			assertThat(indexDefinitions.get(0).getIndexKeys(), equalTo(new BasicDBObjectBuilder().add("indexedDbRef", 1)
					.get()));
		}

		/**
		 * @see DATAMONGO-1297
		 */
		@Test
		public void resolvesIndexOnDbrefWhenDefinedOnNestedElement() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WrapperOfWithDbRef.class);

			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getCollection(), equalTo("wrapperOfWithDbRef"));
			assertThat(indexDefinitions.get(0).getIndexKeys(),
					equalTo(new BasicDBObjectBuilder().add("nested.indexedDbRef", 1).get()));
		}

		/**
		 * @see DATAMONGO-1163
		 */
		@Test
		public void resolveIndexDefinitionInMetaAnnotatedFields() {
			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(
					IndexOnMetaAnnotatedField.class);
			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getCollection(), equalTo("indexOnMetaAnnotatedField"));
			assertThat(indexDefinitions.get(0).getIndexOptions(),
					equalTo(new BasicDBObjectBuilder().add("name", "_name").get()));
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

			@Indexed(background = true, collection = "CollectionOverride", direction = IndexDirection.DESCENDING,
					dropDups = true, expireAfterSeconds = 10, sparse = true, unique = true)//
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

			@Indexed//
			@DBRef//
			NoIndex indexedDbRef;
		}

		@Document(collection = "no-index")
		static class NoIndex {
			@Id String id;
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

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void geoSpatialIndexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(GeoSpatialIndexOnLevelZero.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("geoIndexedProperty", "Zero", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void geoSpatialIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(GeoSpatialIndexOnLevelOne.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("zero.geoIndexedProperty", "One", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void depplyNestedGeoSpatialIndexPathIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(GeoSpatialIndexOnLevelTwo.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection("one.zero.geoIndexedProperty", "Two", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void resolvesIndexDefinitionOptionsCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(WithOptionsOnGeoSpatialIndexProperty.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();

			assertThat(
					indexDefinition.getIndexOptions(),
					equalTo(new BasicDBObjectBuilder().add("name", "location").add("min", 1).add("max", 100).add("bits", 2).get()));
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

			@GeoSpatialIndexed(collection = "CollectionOverride", bits = 2, max = 100, min = 1,
					type = GeoSpatialIndexType.GEO_2D)//
			Point location;
		}

	}

	/**
	 * Test resolution of {@link CompoundIndexes}.
	 * 
	 * @author Christoph Strobl
	 */
	public static class CompoundIndexResolutionTests {

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void compoundIndexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CompoundIndexOnLevelZero.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "CompoundIndexOnLevelZero", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void compoundIndexOptionsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CompoundIndexOnLevelZero.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(), equalTo(new BasicDBObjectBuilder().add("name", "compound_index")
					.add("unique", true).add("dropDups", true).add("sparse", true).add("background", true).get()));
			assertThat(indexDefinition.getIndexKeys(), equalTo(new BasicDBObjectBuilder().add("foo", 1).add("bar", -1).get()));
		}

		/**
		 * @see DATAMONGO-909
		 */
		@Test
		public void compoundIndexOnSuperClassResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexDefinedOnSuperClass.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(), equalTo(new BasicDBObjectBuilder().add("name", "compound_index")
					.add("unique", true).add("dropDups", true).add("sparse", true).add("background", true).get()));
			assertThat(indexDefinition.getIndexKeys(), equalTo(new BasicDBObjectBuilder().add("foo", 1).add("bar", -1).get()));
		}

		/**
		 * @see DATAMONGO-827
		 */
		@Test
		public void compoundIndexDoesNotSpecifyNameWhenUsingGenerateName() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(ComountIndexWithAutogeneratedName.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(
					indexDefinition.getIndexOptions(),
					equalTo(new BasicDBObjectBuilder().add("unique", true).add("dropDups", true).add("sparse", true)
							.add("background", true).get()));
			assertThat(indexDefinition.getIndexKeys(), equalTo(new BasicDBObjectBuilder().add("foo", 1).add("bar", -1).get()));
		}

		/**
		 * @see DATAMONGO-929
		 */
		@Test
		public void compoundIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CompoundIndexOnLevelOne.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection(new String[] { "zero.foo", "zero.bar" }, "CompoundIndexOnLevelOne",
					indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-929
		 */
		@Test
		public void emptyCompoundIndexPathOnLevelOneIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CompoundIndexOnLevelOneWithEmptyIndexDefinition.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection(new String[] { "zero" }, "CompoundIndexOnLevelZeroWithEmptyIndexDef",
					indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-929
		 */
		@Test
		public void singleCompoundIndexPathOnLevelZeroIsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(SingleCompoundIndex.class);

			assertThat(indexDefinitions, hasSize(1));
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "CompoundIndexOnLevelZero", indexDefinitions.get(0));
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

		@CompoundIndexes({ @CompoundIndex(name = "compound_index", background = true, dropDups = true, sparse = true,
				unique = true) })
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

	}

	public static class TextIndexedResolutionTests {

		/**
		 * @see DATAMONGO-937
		 */
		@Test
		public void shouldResolveSingleFieldTextIndexCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(TextIndexOnSinglePropertyInRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection("bar", "textIndexOnSinglePropertyInRoot", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-937
		 */
		@Test
		public void shouldResolveMultiFieldTextIndexCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(TextIndexOnMutiplePropertiesInRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection(new String[] { "foo", "bar" }, "textIndexOnMutiplePropertiesInRoot",
					indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-937
		 */
		@Test
		public void shouldResolveTextIndexOnElementCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(TextIndexOnNestedRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection(new String[] { "nested.foo" }, "textIndexOnNestedRoot", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-937
		 */
		@Test
		public void shouldResolveTextIndexOnElementWithWeightCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(TextIndexOnNestedWithWeightRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection(new String[] { "nested.foo" }, "textIndexOnNestedWithWeightRoot",
					indexDefinitions.get(0));

			DBObject weights = DBObjectTestUtils.getAsDBObject(indexDefinitions.get(0).getIndexOptions(), "weights");
			assertThat(weights.get("nested.foo"), is((Object) 5F));
		}

		/**
		 * @see DATAMONGO-937
		 */
		@Test
		public void shouldResolveTextIndexOnElementWithMostSpecificWeightCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(TextIndexOnNestedWithMostSpecificValueRoot.class);
			assertThat(indexDefinitions.size(), equalTo(1));
			assertIndexPathAndCollection(new String[] { "nested.foo", "nested.bar" },
					"textIndexOnNestedWithMostSpecificValueRoot", indexDefinitions.get(0));

			DBObject weights = DBObjectTestUtils.getAsDBObject(indexDefinitions.get(0).getIndexOptions(), "weights");
			assertThat(weights.get("nested.foo"), is((Object) 5F));
			assertThat(weights.get("nested.bar"), is((Object) 10F));
		}

		/**
		 * @see DATAMONGO-937
		 */
		@Test
		public void shouldSetDefaultLanguageCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithDefaultLanguage.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("default_language"), is((Object) "spanish"));
		}

		/**
		 * @see DATAMONGO-937, DATAMONGO-1049
		 */
		@Test
		public void shouldResolveTextIndexLanguageOverrideCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithLanguageOverride.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("language_override"), is((Object) "lang"));
		}

		/**
		 * @see DATAMONGO-1049
		 */
		@Test
		public void shouldIgnoreTextIndexLanguageOverrideOnNestedElements() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithLanguageOverrideOnNestedElement.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("language_override"), is(nullValue()));
		}

		/**
		 * @see DATAMONGO-1049
		 */
		@Test
		public void shouldNotCreateIndexDefinitionWhenOnlyLanguageButNoTextIndexPresent() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithNoTextIndexPropertyButReservedFieldLanguage.class);
			assertThat(indexDefinitions, is(empty()));
		}

		/**
		 * @see DATAMONGO-1049
		 */
		@Test
		public void shouldNotCreateIndexDefinitionWhenOnlyAnnotatedLanguageButNoTextIndexPresent() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithNoTextIndexPropertyButReservedFieldLanguageAnnotated.class);
			assertThat(indexDefinitions, is(empty()));
		}

		/**
		 * @see DATAMONGO-1049
		 */
		@Test
		public void shouldPreferExplicitlyAnnotatedLanguageProperty() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithOverlappingLanguageProps.class);
			assertThat(indexDefinitions.get(0).getIndexOptions().get("language_override"), is((Object) "lang"));
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

	}

	public static class MixedIndexResolutionTests {

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void multipleIndexesResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(MixedIndexRoot.class);

			assertThat(indexDefinitions, hasSize(2));
			assertThat(indexDefinitions.get(0).getIndexDefinition(), instanceOf(Index.class));
			assertThat(indexDefinitions.get(1).getIndexDefinition(), instanceOf(GeospatialIndex.class));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void cyclicPropertyReferenceOverDBRefShouldNotBeTraversed() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(Inner.class);
			assertThat(indexDefinitions, hasSize(1));
			assertThat(indexDefinitions.get(0).getIndexDefinition().getIndexKeys(),
					equalTo(new BasicDBObjectBuilder().add("outer", 1).get()));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void associationsShouldNotBeTraversed() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(Outer.class);
			assertThat(indexDefinitions, empty());
		}

		/**
		 * @see DATAMONGO-926
		 */
		@Test
		public void shouldNotRunIntoStackOverflow() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CycleStartingInBetween.class);
			assertThat(indexDefinitions, hasSize(1));
		}

		/**
		 * @see DATAMONGO-926
		 */
		@Test
		public void indexShouldBeFoundEvenForCyclePropertyReferenceOnLevelZero() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CycleLevelZero.class);
			assertIndexPathAndCollection("indexedProperty", "cycleLevelZero", indexDefinitions.get(0));
			assertIndexPathAndCollection("cyclicReference.indexedProperty", "cycleLevelZero", indexDefinitions.get(1));
			assertThat(indexDefinitions, hasSize(2));
		}

		/**
		 * @see DATAMONGO-926
		 */
		@Test
		public void indexShouldBeFoundEvenForCyclePropertyReferenceOnLevelOne() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CycleOnLevelOne.class);
			assertIndexPathAndCollection("reference.indexedProperty", "cycleOnLevelOne", indexDefinitions.get(0));
			assertThat(indexDefinitions, hasSize(1));
		}

		/**
		 * @see DATAMONGO-926
		 */
		@Test
		public void indexBeResolvedCorrectlyWhenPropertiesOfDifferentTypesAreNamedEqually() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(NoCycleButIdenticallyNamedProperties.class);
			assertIndexPathAndCollection("foo", "noCycleButIdenticallyNamedProperties", indexDefinitions.get(0));
			assertIndexPathAndCollection("reference.foo", "noCycleButIdenticallyNamedProperties", indexDefinitions.get(1));
			assertIndexPathAndCollection("reference.deep.foo", "noCycleButIdenticallyNamedProperties",
					indexDefinitions.get(2));
			assertThat(indexDefinitions, hasSize(3));
		}

		/**
		 * @see DATAMONGO-949
		 */
		@Test
		public void shouldNotDetectCycleInSimilarlyNamedProperties() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(SimilarityHolingBean.class);
			assertIndexPathAndCollection("norm", "similarityHolingBean", indexDefinitions.get(0));
			assertThat(indexDefinitions, hasSize(1));
		}

		/**
		 * @see DATAMONGO-962
		 */
		@Test
		public void shouldDetectSelfCycleViaCollectionTypeCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(SelfCyclingViaCollectionType.class);
			assertThat(indexDefinitions, empty());
		}

		/**
		 * @see DATAMONGO-962
		 */
		@Test
		public void shouldNotDetectCycleWhenTypeIsUsedMoreThanOnce() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(MultipleObjectsOfSameType.class);
			assertThat(indexDefinitions, empty());
		}

		/**
		 * @see DATAMONGO-962
		 */
		@Test
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

		/**
		 * @see DATAMONGO-1025
		 */
		@Test
		public void shouldUsePathIndexAsIndexNameForDocumentsHavingNamedNestedCompoundIndexFixedOnCollection() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithNestedDocumentHavingNamedCompoundIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("propertyOfTypeHavingNamedCompoundIndex.c_index"));
		}

		/**
		 * @see DATAMONGO-1025
		 */
		@Test
		public void shouldUseIndexNameForNestedTypesWithNamedCompoundIndexDefinition() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithNestedTypeHavingNamedCompoundIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("propertyOfTypeHavingNamedCompoundIndex.c_index"));
		}

		/**
		 * @see DATAMONGO-1025
		 */
		@Test
		public void shouldUsePathIndexAsIndexNameForDocumentsHavingNamedNestedIndexFixedOnCollection() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithNestedDocumentHavingNamedIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("propertyOfTypeHavingNamedIndex.property_index"));
		}

		/**
		 * @see DATAMONGO-1025
		 */
		@Test
		public void shouldUseIndexNameForNestedTypesWithNamedIndexDefinition() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithNestedTypeHavingNamedIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"),
					equalTo("propertyOfTypeHavingNamedIndex.property_index"));
		}

		/**
		 * @see DATAMONGO-1025
		 */
		@Test
		public void shouldUseIndexNameOnRootLevel() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(DocumentWithNamedIndex.class);
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"), equalTo("property_index"));
		}

		/**
		 * @see DATAMONGO-1087
		 */
		@Test
		public void shouldAllowMultiplePropertiesOfSameTypeWithMatchingStartLettersOnRoot() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(MultiplePropertiesOfSameTypeWithMatchingStartLetters.class);

			assertThat(indexDefinitions, hasSize(2));
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"), equalTo("name.component"));
			assertThat((String) indexDefinitions.get(1).getIndexOptions().get("name"), equalTo("nameLast.component"));
		}

		/**
		 * @see DATAMONGO-1087
		 */
		@Test
		public void shouldAllowMultiplePropertiesOfSameTypeWithMatchingStartLettersOnNestedProperty() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(MultiplePropertiesOfSameTypeWithMatchingStartLettersOnNestedProperty.class);

			assertThat(indexDefinitions, hasSize(2));
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"), equalTo("component.nameLast"));
			assertThat((String) indexDefinitions.get(1).getIndexOptions().get("name"), equalTo("component.name"));
		}

		/**
		 * @see DATAMONGO-1121
		 */
		@Test
		public void shouldOnlyConsiderEntitiesAsPotentialCycleCandidates() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(OuterDocumentReferingToIndexedPropertyViaDifferentNonCyclingPaths.class);

			assertThat(indexDefinitions, hasSize(2));
			assertThat((String) indexDefinitions.get(0).getIndexOptions().get("name"), equalTo("path1.foo"));
			assertThat((String) indexDefinitions.get(1).getIndexOptions().get("name"),
					equalTo("path2.propertyWithIndexedStructure.foo"));

		}

		/**
		 * @see DATAMONGO-1263
		 */
		@Test
		public void shouldConsiderGenericTypeArgumentsOfCollectionElements() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(EntityWithGenericTypeWrapperAsElement.class);

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
		return resolver.resolveIndexForEntity(mappingContext.getPersistentEntity(type));
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
			assertThat(holder.getIndexDefinition().getIndexKeys().containsField(expectedPath), equalTo(true));
		}
		assertThat(holder.getCollection(), equalTo(expectedCollection));
	}

}
