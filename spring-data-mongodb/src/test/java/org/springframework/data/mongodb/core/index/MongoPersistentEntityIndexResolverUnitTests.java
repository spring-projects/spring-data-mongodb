/*
 * Copyright 2014 the original author or authors.
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

import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.collection.IsEmptyCollection.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.IndexDefinitionHolder;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.CompoundIndexResolutionTests;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.GeoSpatialIndexResolutionTests;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.IndexResolutionTests;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolverUnitTests.MixedIndexResolutionTests;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.BasicDBObjectBuilder;

/**
 * @author Christoph Strobl
 */
@RunWith(Suite.class)
@SuiteClasses({ IndexResolutionTests.class, GeoSpatialIndexResolutionTests.class, CompoundIndexResolutionTests.class,
		MixedIndexResolutionTests.class })
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
			assertIndexPathAndCollection("compound_index", "CompoundIndexOnLevelZero", indexDefinitions.get(0));
		}

		/**
		 * @see DATAMONGO-899
		 */
		@Test
		public void compoundIndexOptionsResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(CompoundIndexOnLevelZero.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(),
					equalTo(new BasicDBObjectBuilder().add("name", "compound_index").add("unique", true).add("dropDups", true)
							.add("sparse", true).add("background", true).add("expireAfterSeconds", 10L).get()));
			assertThat(indexDefinition.getIndexKeys(), equalTo(new BasicDBObjectBuilder().add("foo", 1).add("bar", -1).get()));
		}

		/**
		 * @see DATAMONGO-909
		 */
		@Test
		public void compoundIndexOnSuperClassResolvedCorrectly() {

			List<IndexDefinitionHolder> indexDefinitions = prepareMappingContextAndResolveIndexForType(IndexDefinedOnSuperClass.class);

			IndexDefinition indexDefinition = indexDefinitions.get(0).getIndexDefinition();
			assertThat(indexDefinition.getIndexOptions(),
					equalTo(new BasicDBObjectBuilder().add("name", "compound_index").add("unique", true).add("dropDups", true)
							.add("sparse", true).add("background", true).add("expireAfterSeconds", 10L).get()));
			assertThat(indexDefinition.getIndexKeys(), equalTo(new BasicDBObjectBuilder().add("foo", 1).add("bar", -1).get()));
		}

		@Document(collection = "CompoundIndexOnLevelZero")
		@CompoundIndexes({ @CompoundIndex(name = "compound_index", def = "{'foo': 1, 'bar': -1}", background = true,
				dropDups = true, expireAfterSeconds = 10, sparse = true, unique = true) })
		static class CompoundIndexOnLevelZero {}

		static class IndexDefinedOnSuperClass extends CompoundIndexOnLevelZero {

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
			assertThat(indexDefinitions.get(0).getIndexDefinition().getCollection(), equalTo("inner"));
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

		assertThat(holder.getPath(), equalTo(expectedPath));
		assertThat(holder.getCollection(), equalTo(expectedCollection));
	}
}
