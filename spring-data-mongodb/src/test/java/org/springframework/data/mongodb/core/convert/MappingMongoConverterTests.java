/*
 * Copyright 2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.MongoClient;

/**
 * Integration tests for {@link MappingMongoConverter}.
 *
 * @author Christoph Strobl
 */
public class MappingMongoConverterTests {

	MongoClient client;

	MappingMongoConverter converter;
	MongoMappingContext mappingContext;
	DbRefResolver dbRefResolver;

	@Before
	public void setUp() {

		client = new MongoClient();
		client.dropDatabase("mapping-converter-tests");

		MongoDbFactory factory = new SimpleMongoDbFactory(client, "mapping-converter-tests");

		dbRefResolver = spy(new DefaultDbRefResolver(factory));
		mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(dbRefResolver, mappingContext);
	}

	@Test // DATAMONGO-2004
	public void resolvesLazyDBRefOnAccess() {

		client.getDatabase("mapping-converter-tests").getCollection("samples")
				.insertMany(Arrays.asList(new Document("_id", "sample-1").append("value", "one"),
						new Document("_id", "sample-2").append("value", "two")));

		Document source = new Document("_id", "id-1").append("lazyList",
				Arrays.asList(new com.mongodb.DBRef("samples", "sample-1"), new com.mongodb.DBRef("samples", "sample-2")));

		WithLazyDBRef target = converter.read(WithLazyDBRef.class, source);

		verify(dbRefResolver).resolveDbRef(any(), isNull(), any(), any());
		verifyNoMoreInteractions(dbRefResolver);

		assertThat(target.lazyList).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.getLazyList()).contains(new Sample("sample-1", "one"), new Sample("sample-2", "two"));

		verify(dbRefResolver).bulkFetch(any());
	}

	@Test // DATAMONGO-2004
	public void resolvesLazyDBRefConstructorArgOnAccess() {

		client.getDatabase("mapping-converter-tests").getCollection("samples")
				.insertMany(Arrays.asList(new Document("_id", "sample-1").append("value", "one"),
						new Document("_id", "sample-2").append("value", "two")));

		Document source = new Document("_id", "id-1").append("lazyList",
				Arrays.asList(new com.mongodb.DBRef("samples", "sample-1"), new com.mongodb.DBRef("samples", "sample-2")));

		WithLazyDBRefAsConstructorArg target = converter.read(WithLazyDBRefAsConstructorArg.class, source);

		verify(dbRefResolver).resolveDbRef(any(), isNull(), any(), any());
		verifyNoMoreInteractions(dbRefResolver);

		assertThat(target.lazyList).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.getLazyList()).contains(new Sample("sample-1", "one"), new Sample("sample-2", "two"));

		verify(dbRefResolver).bulkFetch(any());
	}

	public static class WithLazyDBRef {

		@Id String id;
		@DBRef(lazy = true) List<Sample> lazyList;

		public List<Sample> getLazyList() {
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

		public List<Sample> getLazyList() {
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
}
