/*
 * Copyright 2018-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.test.util.Client;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Integration tests for {@link MappingMongoConverter}.
 *
 * @author Christoph Strobl
 */

class MappingMongoConverterTests {

	private static final String DATABASE = "mapping-converter-tests";

	private static @Client MongoClient client;

	private final MongoDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(client, DATABASE);

	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;
	private DbRefResolver dbRefResolver;

	@BeforeEach
	void setUp() {

		MongoDatabase database = client.getDatabase(DATABASE);

		database.getCollection("samples").deleteMany(new Document());
		database.getCollection("java-time-types").deleteMany(new Document());

		dbRefResolver = spy(new DefaultDbRefResolver(factory));

		mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(new MongoCustomConversions(Collections.emptyList()).getSimpleTypeHolder());
		mappingContext.setInitialEntitySet(Set.of(WithLazyDBRefAsConstructorArg.class, WithLazyDBRef.class, WithJavaTimeTypes.class));
		mappingContext.setAutoIndexCreation(false);
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		converter.afterPropertiesSet();
	}

	@Test // DATAMONGO-2004
	void resolvesLazyDBRefOnAccess() {

		client.getDatabase(DATABASE).getCollection("samples")
				.insertMany(Arrays.asList(new Document("_id", "sample-1").append("value", "one"),
						new Document("_id", "sample-2").append("value", "two")));

		Document source = new Document("_id", "id-1").append("lazyList",
				Arrays.asList(new com.mongodb.DBRef("samples", "sample-1"), new com.mongodb.DBRef("samples", "sample-2")));

		WithLazyDBRef target = converter.read(WithLazyDBRef.class, source);

		verify(dbRefResolver).resolveDbRef(any(), isNull(), any(), any());

		assertThat(target.lazyList).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.getLazyList()).contains(new Sample("sample-1", "one"), new Sample("sample-2", "two"));

		verify(dbRefResolver).bulkFetch(any());
	}

	@Test // GH-4312
	void conversionShouldAllowReadingAlreadyResolvedReferences() {

		Document sampleSource = new Document("_id", "sample-1").append("value", "one");
		Document source = new Document("_id", "id-1").append("sample", sampleSource);

		WithSingleValueDbRef read = converter.read(WithSingleValueDbRef.class, source);

		assertThat(read.sample).isEqualTo(converter.read(Sample.class, sampleSource));
		verifyNoInteractions(dbRefResolver);
	}

	@Test // GH-4312
	void conversionShouldAllowReadingAlreadyResolvedListOfReferences() {

		Document sample1Source = new Document("_id", "sample-1").append("value", "one");
		Document sample2Source = new Document("_id", "sample-2").append("value", "two");
		Document source = new Document("_id", "id-1").append("lazyList", List.of(sample1Source, sample2Source));

		WithLazyDBRef read = converter.read(WithLazyDBRef.class, source);

		assertThat(read.lazyList).containsExactly(converter.read(Sample.class, sample1Source),
				converter.read(Sample.class, sample2Source));
		verifyNoInteractions(dbRefResolver);
	}

	@Test // GH-4312
	void conversionShouldAllowReadingAlreadyResolvedMapOfReferences() {

		Document sample1Source = new Document("_id", "sample-1").append("value", "one");
		Document sample2Source = new Document("_id", "sample-2").append("value", "two");
		Document source = new Document("_id", "id-1").append("sampleMap",
				new Document("s1", sample1Source).append("s2", sample2Source));

		WithMapValueDbRef read = converter.read(WithMapValueDbRef.class, source);

		assertThat(read.sampleMap) //
				.containsEntry("s1", converter.read(Sample.class, sample1Source)) //
				.containsEntry("s2", converter.read(Sample.class, sample2Source));
		verifyNoInteractions(dbRefResolver);
	}

	@Test // GH-4312
	void conversionShouldAllowReadingAlreadyResolvedMapOfLazyReferences() {

		Document sample1Source = new Document("_id", "sample-1").append("value", "one");
		Document sample2Source = new Document("_id", "sample-2").append("value", "two");
		Document source = new Document("_id", "id-1").append("sampleMapLazy",
				new Document("s1", sample1Source).append("s2", sample2Source));

		WithMapValueDbRef read = converter.read(WithMapValueDbRef.class, source);

		assertThat(read.sampleMapLazy) //
				.containsEntry("s1", converter.read(Sample.class, sample1Source)) //
				.containsEntry("s2", converter.read(Sample.class, sample2Source));
		verifyNoInteractions(dbRefResolver);
	}

	@Test // GH-4312
	void resolvesLazyDBRefMapOnAccess() {

		client.getDatabase(DATABASE).getCollection("samples")
				.insertMany(Arrays.asList(new Document("_id", "sample-1").append("value", "one"),
						new Document("_id", "sample-2").append("value", "two")));

		Document source = new Document("_id", "id-1").append("sampleMapLazy",
				new Document("s1", new com.mongodb.DBRef("samples", "sample-1")).append("s2",
						new com.mongodb.DBRef("samples", "sample-2")));

		WithMapValueDbRef target = converter.read(WithMapValueDbRef.class, source);

		verify(dbRefResolver).resolveDbRef(any(), isNull(), any(), any());

		assertThat(target.sampleMapLazy).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.getSampleMapLazy()).containsEntry("s1", new Sample("sample-1", "one")).containsEntry("s2",
				new Sample("sample-2", "two"));

		verify(dbRefResolver).bulkFetch(any());
	}

	@Test // GH-4312
	void conversionShouldAllowReadingAlreadyResolvedLazyReferences() {

		Document sampleSource = new Document("_id", "sample-1").append("value", "one");
		Document source = new Document("_id", "id-1").append("sampleLazy", sampleSource);

		WithSingleValueDbRef read = converter.read(WithSingleValueDbRef.class, source);

		assertThat(read.sampleLazy).isEqualTo(converter.read(Sample.class, sampleSource));
		verifyNoInteractions(dbRefResolver);
	}

	@Test // DATAMONGO-2004
	void resolvesLazyDBRefConstructorArgOnAccess() {

		client.getDatabase(DATABASE).getCollection("samples")
				.insertMany(Arrays.asList(new Document("_id", "sample-1").append("value", "one"),
						new Document("_id", "sample-2").append("value", "two")));

		Document source = new Document("_id", "id-1").append("lazyList",
				Arrays.asList(new com.mongodb.DBRef("samples", "sample-1"), new com.mongodb.DBRef("samples", "sample-2")));

		WithLazyDBRefAsConstructorArg target = converter.read(WithLazyDBRefAsConstructorArg.class, source);

		verify(dbRefResolver).resolveDbRef(any(), isNull(), any(), any());

		assertThat(target.lazyList).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.getLazyList()).contains(new Sample("sample-1", "one"), new Sample("sample-2", "two"));

		verify(dbRefResolver).bulkFetch(any());
	}

	@Test // DATAMONGO-2400
	void readJavaTimeValuesWrittenViaCodec() {

		configureConverterWithNativeJavaTimeCodec();
		MongoCollection<Document> mongoCollection = client.getDatabase(DATABASE).getCollection("java-time-types");

		Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
		WithJavaTimeTypes source = WithJavaTimeTypes.withJavaTimeTypes(now);
		source.id = "id-1";

		mongoCollection.insertOne(source.toDocument());

		assertThat(converter.read(WithJavaTimeTypes.class, mongoCollection.find(new Document("_id", source.id)).first()))
				.isEqualTo(source);
	}

	void configureConverterWithNativeJavaTimeCodec() {

		converter = new MappingMongoConverter(dbRefResolver, mappingContext);
		converter.setCustomConversions(
				MongoCustomConversions.create(MongoConverterConfigurationAdapter::useNativeDriverJavaTimeCodecs));
		converter.afterPropertiesSet();
	}

	public static class WithLazyDBRef {

		@Id String id;
		@DBRef(lazy = true) List<Sample> lazyList;

		List<Sample> getLazyList() {
			return lazyList;
		}
	}

	public static class WithSingleValueDbRef {

		@Id //
		String id;

		@DBRef //
		Sample sample;

		@DBRef(lazy = true) //
		Sample sampleLazy;

		public String getId() {
			return this.id;
		}

		public Sample getSample() {
			return this.sample;
		}

		public Sample getSampleLazy() {
			return this.sampleLazy;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setSample(Sample sample) {
			this.sample = sample;
		}

		public void setSampleLazy(Sample sampleLazy) {
			this.sampleLazy = sampleLazy;
		}

		public String toString() {
			return "MappingMongoConverterTests.WithSingleValueDbRef(id=" + this.getId() + ", sample=" + this.getSample()
					+ ", sampleLazy=" + this.getSampleLazy() + ")";
		}
	}

	public static class WithMapValueDbRef {

		@Id String id;

		@DBRef //
		Map<String, Sample> sampleMap;

		@DBRef(lazy = true) //
		Map<String, Sample> sampleMapLazy;

		public String getId() {
			return this.id;
		}

		public Map<String, Sample> getSampleMap() {
			return this.sampleMap;
		}

		public Map<String, Sample> getSampleMapLazy() {
			return this.sampleMapLazy;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setSampleMap(Map<String, Sample> sampleMap) {
			this.sampleMap = sampleMap;
		}

		public void setSampleMapLazy(Map<String, Sample> sampleMapLazy) {
			this.sampleMapLazy = sampleMapLazy;
		}

		public String toString() {
			return "MappingMongoConverterTests.WithMapValueDbRef(id=" + this.getId() + ", sampleMap=" + this.getSampleMap()
					+ ", sampleMapLazy=" + this.getSampleMapLazy() + ")";
		}
	}

	public static class WithLazyDBRefAsConstructorArg {

		@Id String id;
		@DBRef(lazy = true) List<Sample> lazyList;

		public WithLazyDBRefAsConstructorArg(String id, List<Sample> lazyList) {

			this.id = id;
			this.lazyList = lazyList;
		}

		List<Sample> getLazyList() {
			return lazyList;
		}
	}

	static class Sample {

		@Id String id;
		String value;

		public Sample(String id, String value) {

			this.id = id;
			this.value = value;
		}

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Sample sample = (Sample) o;
			return Objects.equals(id, sample.id) && Objects.equals(value, sample.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}

		public String toString() {
			return "MappingMongoConverterTests.Sample(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}

	static class WithJavaTimeTypes {

		@Id String id;
		LocalDate localDate;
		LocalTime localTime;
		LocalDateTime localDateTime;

		public WithJavaTimeTypes() {}

		static WithJavaTimeTypes withJavaTimeTypes(Instant instant) {

			WithJavaTimeTypes instance = new WithJavaTimeTypes();

			instance.localDate = LocalDate.from(instant.atZone(ZoneId.of("CET")));
			instance.localTime = LocalTime.from(instant.atZone(ZoneId.of("CET")));
			instance.localDateTime = LocalDateTime.from(instant.atZone(ZoneId.of("CET")));

			return instance;
		}

		Document toDocument() {
			return new Document("_id", id).append("localDate", localDate).append("localTime", localTime)
					.append("localDateTime", localDateTime);
		}

		public String getId() {
			return this.id;
		}

		public LocalDate getLocalDate() {
			return this.localDate;
		}

		public LocalTime getLocalTime() {
			return this.localTime;
		}

		public LocalDateTime getLocalDateTime() {
			return this.localDateTime;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setLocalDate(LocalDate localDate) {
			this.localDate = localDate;
		}

		public void setLocalTime(LocalTime localTime) {
			this.localTime = localTime;
		}

		public void setLocalDateTime(LocalDateTime localDateTime) {
			this.localDateTime = localDateTime;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			WithJavaTimeTypes that = (WithJavaTimeTypes) o;
			return Objects.equals(id, that.id) && Objects.equals(localDate, that.localDate)
					&& Objects.equals(localTime, that.localTime) && Objects.equals(localDateTime, that.localDateTime);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, localDate, localTime, localDateTime);
		}

		public String toString() {
			return "MappingMongoConverterTests.WithJavaTimeTypes(id=" + this.getId() + ", localDate=" + this.getLocalDate()
					+ ", localTime=" + this.getLocalTime() + ", localDateTime=" + this.getLocalDateTime() + ")";
		}
	}

	@Nested // GH-5065
	class EmptyMapTests {

		@Test // GH-5065
		void controlTestToIllustrateThatTheEmptyMapProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblem() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			//EmptyMapDocument target = converter.read(EmptyMapDocument.class, document);
			assertThat(converter.read(EmptyMapDocument.class, document).map).isNotNull().isEmpty();
			assertThat(converter.read(EmptyMapDocument.class, document).getMap()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void controlTestToIllustrateThatTheNullMapProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblem() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":null}");
			//EmptyMapDocument target = converter.read(EmptyMapDocument.class, document);
			assertThat(converter.read(EmptyMapDocument.class, document).map).isNull();
			assertThat(converter.read(EmptyMapDocument.class, document).getMap()).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyMapWhenUsingDocumentReferenceAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			DocumentReferenceEmptyMapDocument target = converter.read(DocumentReferenceEmptyMapDocument.class, document);
			assertThat(target.map).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyMapWhenUsingDocumentReferenceAnnotationUsingGetMap() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			DocumentReferenceEmptyMapDocument target = converter.read(DocumentReferenceEmptyMapDocument.class, document);
			assertThat(target.getMap()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullMapWhenUsingDocumentReferenceAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":null}");
			DocumentReferenceEmptyMapDocument target = converter.read(DocumentReferenceEmptyMapDocument.class, document);
			assertThat(target.map).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullMapWhenUsingDocumentReferenceAnnotationUsingGetMap() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":null}");
			DocumentReferenceEmptyMapDocument target = converter.read(DocumentReferenceEmptyMapDocument.class, document);
			assertThat(target.getMap()).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyMapWhenUsingDocumentReferenceAnnotationWithLazyEqualToTrue() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			LazyDocumentReferenceEmptyMapDocument target = converter.read(LazyDocumentReferenceEmptyMapDocument.class, document);
			assertThat(target.map).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyMapWithAValidValuesPropertyWhenUsingDocumentReferenceAnnotationWithLazyEqualToTrue() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			LazyDocumentReferenceEmptyMapDocument target = converter.read(LazyDocumentReferenceEmptyMapDocument.class, document);
			assertThat(target.map.values()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyMapWhenUsingDocumentReferenceAnnotationWithLazyEqualToTrueUsingGetMap() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			LazyDocumentReferenceEmptyMapDocument target = converter.read(LazyDocumentReferenceEmptyMapDocument.class, document);
			assertThat(target.getMap()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyMapWithAValidValuesPropertyWhenUsingDocumentReferenceAnnotationWithLazyEqualToTrueUsingGetMap() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			LazyDocumentReferenceEmptyMapDocument target = converter.read(LazyDocumentReferenceEmptyMapDocument.class, document);
			assertThat(target.getMap().values()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyMapWhenUsingDBRefAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			DBRefEmptyMapDocument target = converter.read(DBRefEmptyMapDocument.class, document);
			assertThat(target.map).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyMapWhenUsingDBRefAnnotationUsingGetMap() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":{}}");
			DBRefEmptyMapDocument target = converter.read(DBRefEmptyMapDocument.class, document);
			assertThat(target.getMap()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullMapWhenUsingDBRefAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":null}");
			DBRefEmptyMapDocument target = converter.read(DBRefEmptyMapDocument.class, document);
			assertThat(target.map).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullMapWhenUsingDBRefAnnotationUsingGetMap() {
			org.bson.Document document = org.bson.Document.parse("{\"map\":null}");
			DBRefEmptyMapDocument target = converter.read(DBRefEmptyMapDocument.class, document);
			assertThat(target.getMap()).isNull();
		}

		static class EmptyMapDocument {

			Map<String, String> map;

			public EmptyMapDocument(Map<String, String> map) {
				this.map = map;
			}

			Map<String, String> getMap() {
				return map;
			}
		}

		static class DocumentReferenceEmptyMapDocument {

			@DocumentReference
			Map<String, String> map;

			public DocumentReferenceEmptyMapDocument(Map<String, String> map) {
				this.map = map;
			}

			Map<String, String> getMap() {
				return map;
			}
		}

		static class LazyDocumentReferenceEmptyMapDocument {

			@DocumentReference(lazy = true)
			Map<String, String> map;

			public LazyDocumentReferenceEmptyMapDocument(Map<String, String> map) {
				this.map = map;
			}

			Map<String, String> getMap() {
				return map;
			}
		}

		static class DBRefEmptyMapDocument {

			@DBRef
			Map<String, String> map;

			public DBRefEmptyMapDocument(Map<String, String> map) {
				this.map = map;
			}

			Map<String, String> getMap() {
				return map;
			}
		}
	}

	@Nested // GH-5065
	class EmptyListTests {

		@Test // GH-5065
		void controlTestToIllustrateThatTheEmptyListProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblem() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":[]}");
			EmptyListDocument target = converter.read(EmptyListDocument.class, document);
			assertThat(target.list).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void controlTestToIllustrateThatTheEmptyListProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblemUsingGetList() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":[]}");
			EmptyListDocument target = converter.read(EmptyListDocument.class, document);
			assertThat(target.getList()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void controlTestToIllustrateThatTheNullListProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblem() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":null}");
			EmptyListDocument target = converter.read(EmptyListDocument.class, document);
			assertThat(target.list).isNull();
		}

		@Test // GH-5065
		void controlTestToIllustrateThatTheNullListProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblemUsingGetList() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":null}");
			EmptyListDocument target = converter.read(EmptyListDocument.class, document);
			assertThat(target.getList()).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyListWhenUsingDocumentReferenceAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":[]}");
			DocumentReferenceEmptyListDocument target = converter.read(DocumentReferenceEmptyListDocument.class, document);
			assertThat(target.list).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyListWhenUsingDocumentReferenceAnnotationUsingGetList() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":[]}");
			DocumentReferenceEmptyListDocument target = converter.read(DocumentReferenceEmptyListDocument.class, document);
			assertThat(target.getList()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullListWhenUsingDocumentReferenceAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":null}");
			DocumentReferenceEmptyListDocument target = converter.read(DocumentReferenceEmptyListDocument.class, document);
			assertThat(target.list).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullListWhenUsingDocumentReferenceAnnotationUsingGetList() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":null}");
			DocumentReferenceEmptyListDocument target = converter.read(DocumentReferenceEmptyListDocument.class, document);
			assertThat(target.getList()).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyListWhenUsingDocumentReferenceAnnotationWithLazyEqualToTrue() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":[]}");
			LazyDocumentReferenceEmptyListDocument target = converter.read(LazyDocumentReferenceEmptyListDocument.class, document);
			assertThat(target.list).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyListWhenUsingDocumentReferenceAnnotationWithLazyEqualToTrueUsingGetList() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":[]}");
			LazyDocumentReferenceEmptyListDocument target = converter.read(LazyDocumentReferenceEmptyListDocument.class, document);
			assertThat(target.getList()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyListWhenUsingDBRefAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":[]}");
			DBRefEmptyListDocument target = converter.read(DBRefEmptyListDocument.class, document);
			assertThat(target.list).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptyListWhenUsingDBRefAnnotationUsingGetList() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":[]}");
			DBRefEmptyListDocument target = converter.read(DBRefEmptyListDocument.class, document);
			assertThat(target.getList()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullListWhenUsingDBRefAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":null}");
			DBRefEmptyListDocument target = converter.read(DBRefEmptyListDocument.class, document);
			assertThat(target.list).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullListWhenUsingDBRefAnnotationUsingGetList() {
			org.bson.Document document = org.bson.Document.parse("{\"list\":null}");
			DBRefEmptyListDocument target = converter.read(DBRefEmptyListDocument.class, document);
			assertThat(target.getList()).isNull();
		}

		static class EmptyListDocument {

			List<String> list;

			public EmptyListDocument(List<String> list) {
				this.list = list;
			}

			List<String> getList() {
				return list;
			}
		}

		static class DocumentReferenceEmptyListDocument {

			@DocumentReference
			List<String> list;

			public DocumentReferenceEmptyListDocument(List<String> list) {
				this.list = list;
			}

			List<String> getList() {
				return list;
			}
		}

		static class LazyDocumentReferenceEmptyListDocument {

			@DocumentReference(lazy = true)
			List<String> list;

			public LazyDocumentReferenceEmptyListDocument(List<String> list) {
				this.list = list;
			}

			List<String> getList() {
				return list;
			}
		}

		static class DBRefEmptyListDocument {

			@DBRef
			List<String> list;

			public DBRefEmptyListDocument(List<String> list) {
				this.list = list;
			}

			List<String> getList() {
				return list;
			}
		}
	}

	@Nested // GH-5065
	class EmptySetTests {

		@Test // GH-5065
		void controlTestToIllustrateThatTheEmptySetProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblem() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":[]}");
			EmptySetDocument target = converter.read(EmptySetDocument.class, document);
			assertThat(target.set).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void controlTestToIllustrateThatTheEmptySetProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblemUsingGetSet() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":[]}");
			EmptySetDocument target = converter.read(EmptySetDocument.class, document);
			assertThat(target.getSet()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void controlTestToIllustrateThatTheNullSetProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblem() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":null}");
			EmptySetDocument target = converter.read(EmptySetDocument.class, document);
			assertThat(target.set).isNull();
		}

		@Test // GH-5065
		void controlTestToIllustrateThatTheNullSetProblemIsLimitedToDocumentReferencesAndNotAMoreGenericProblemUsingGetSet() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":null}");
			EmptySetDocument target = converter.read(EmptySetDocument.class, document);
			assertThat(target.getSet()).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptySetWhenUsingDocumentReferenceAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":[]}");
			DocumentReferenceEmptySetDocument target = converter.read(DocumentReferenceEmptySetDocument.class, document);
			assertThat(target.set).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptySetWhenUsingDocumentReferenceAnnotationUsingGetSet() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":[]}");
			DocumentReferenceEmptySetDocument target = converter.read(DocumentReferenceEmptySetDocument.class, document);
			assertThat(target.getSet()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullSetWhenUsingDocumentReferenceAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":null}");
			DocumentReferenceEmptySetDocument target = converter.read(DocumentReferenceEmptySetDocument.class, document);
			assertThat(target.set).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullSetWhenUsingDocumentReferenceAnnotationUsingGetSet() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":null}");
			DocumentReferenceEmptySetDocument target = converter.read(DocumentReferenceEmptySetDocument.class, document);
			assertThat(target.getSet()).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptySetWhenUsingDocumentReferenceAnnotationWithLazyEqualToTrue() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":[]}");
			LazyDocumentReferenceEmptySetDocument target = converter.read(LazyDocumentReferenceEmptySetDocument.class, document);
			assertThat(target.set).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptySetWhenUsingDocumentReferenceAnnotationWithLazyEqualToTrueUsingGetSet() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":[]}");
			LazyDocumentReferenceEmptySetDocument target = converter.read(LazyDocumentReferenceEmptySetDocument.class, document);
			assertThat(target.getSet()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptySetWhenUsingDBRefAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":[]}");
			DBRefEmptySetDocument target = converter.read(DBRefEmptySetDocument.class, document);
			assertThat(target.set).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnEmptyObjectAsAnEmptySetWhenUsingDBRefAnnotationUsingGetSet() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":[]}");
			DBRefEmptySetDocument target = converter.read(DBRefEmptySetDocument.class, document);
			assertThat(target.getSet()).isNotNull().isEmpty();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullSetWhenUsingDBRefAnnotation() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":null}");
			DBRefEmptySetDocument target = converter.read(DBRefEmptySetDocument.class, document);
			assertThat(target.set).isNull();
		}

		@Test // GH-5065
		void converterShouldReadAnExplicitlyAssignedNullAsANullSetWhenUsingDBRefAnnotationUsingGetSet() {
			org.bson.Document document = org.bson.Document.parse("{\"set\":null}");
			DBRefEmptySetDocument target = converter.read(DBRefEmptySetDocument.class, document);
			assertThat(target.getSet()).isNull();
		}

		static class EmptySetDocument {

			Set<String> set;

			public EmptySetDocument(Set<String> set) {
				this.set = set;
			}

			Set<String> getSet() {
				return set;
			}
		}

		static class DocumentReferenceEmptySetDocument {

			@DocumentReference
			Set<String> set;

			public DocumentReferenceEmptySetDocument(Set<String> set) {
				this.set = set;
			}

			Set<String> getSet() {
				return set;
			}
		}

		static class LazyDocumentReferenceEmptySetDocument {

			@DocumentReference(lazy = true)
			Set<String> set;

			public LazyDocumentReferenceEmptySetDocument(Set<String> set) {
				this.set = set;
			}

			Set<String> getSet() {
				return set;
			}
		}

		static class DBRefEmptySetDocument {

			@DBRef
			Set<String> set;

			public DBRefEmptySetDocument(Set<String> set) {
				this.set = set;
			}

			Set<String> getSet() {
				return set;
			}
		}
	}

}
