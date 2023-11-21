/*
 * Copyright 2018-2023 the original author or authors.
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
import static org.mockito.Mockito.any;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Integration tests for {@link MappingMongoConverter}.
 *
 * @author Christoph Strobl
 * @author Piotr Kubowicz
 */
@ExtendWith(MongoClientExtension.class)
public class MappingMongoConverterTests {

	private static final String DATABASE = "mapping-converter-tests";

	private static @Client MongoClient client;

	private MongoDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(client, DATABASE);

	private MappingMongoConverter converter;
	private MongoMappingContext mappingContext;
	private DbRefResolver dbRefResolver;

	@BeforeEach
	void setUp() {

		MongoDatabase database = client.getDatabase(DATABASE);

		database.getCollection("samples").deleteMany(new Document());
		database.getCollection("java-time-types").deleteMany(new Document());
		database.getCollection("with-nonnull").deleteMany(new Document());

		dbRefResolver = spy(new DefaultDbRefResolver(factory));

		mappingContext = new MongoMappingContext();
		mappingContext.setSimpleTypeHolder(new MongoCustomConversions(Collections.emptyList()).getSimpleTypeHolder());
		mappingContext.setInitialEntitySet(new HashSet<>(
				Arrays.asList(WithLazyDBRefAsConstructorArg.class, WithLazyDBRef.class, WithJavaTimeTypes.class, WithNonnullField.class)));
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

	@Test // DATAMONGO-2511
	public void reportConversionFailedExceptionContext() {

		configureConverterWithNativeJavaTimeCodec();
		MongoCollection<Document> mongoCollection = client.getDatabase(DATABASE).getCollection("java-time-types");

		mongoCollection.insertOne(new Document("_id", "id-of-wrong-document").append("localDate", "not-a-date"));

		assertThatThrownBy(() -> converter.read(WithJavaTimeTypes.class,
				mongoCollection.find(new Document("_id", "id-of-wrong-document")).first()))
						.hasMessageContaining("id-of-wrong-document");
	}

	@Test // DATAMONGO-2511
	public void reportMappingInstantiationExceptionContext() {

		MongoCollection<Document> mongoCollection = client.getDatabase(DATABASE).getCollection("with-nonnull");

		mongoCollection.insertOne(new Document("_id", "id-of-wrong-document"));

		assertThatThrownBy(() -> converter.read(WithNonnullField.class,
				mongoCollection.find(new Document("_id", "id-of-wrong-document")).first()))
						.hasMessageContaining("id-of-wrong-document");
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

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Sample {

		@Id String id;
		String value;
	}

	@Data
	static class WithJavaTimeTypes {

		@Id String id;
		LocalDate localDate;
		LocalTime localTime;
		LocalDateTime localDateTime;

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
	}

	@Value
	static class WithNonnullField {
		@Id String id;
		@NonNull String field;
	}
}
