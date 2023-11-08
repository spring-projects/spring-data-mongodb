/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.data.Index.atIndex;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.RepeatFailedTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * Integration test for index creation via {@link ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 */
@ExtendWith(MongoClientExtension.class)
public class ReactiveMongoTemplateIndexTests {

	private static @Client MongoClient client;

	private SimpleReactiveMongoDatabaseFactory factory;
	private ReactiveMongoTemplate template;

	@BeforeEach
	void setUp() {

		factory = new SimpleReactiveMongoDatabaseFactory(client, "reactive-template-index-tests");
		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setAutoIndexCreation(true);
		template = new ReactiveMongoTemplate(factory, new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext));

		MongoTestUtils.dropCollectionNow("reactive-template-index-tests", "person", client);
		MongoTestUtils.dropCollectionNow("reactive-template-index-tests", "indexfail", client);
		MongoTestUtils.dropCollectionNow("reactive-template-index-tests", "indexedSample", client);
	}

	@AfterEach
	void cleanUp() {}

	@RepeatFailedTest(3) // DATAMONGO-1444
	void testEnsureIndexShouldCreateIndex() {

		Person p1 = new Person("Oliver");
		p1.setAge(25);
		template.insert(p1);
		Person p2 = new Person("Sven");
		p2.setAge(40);
		template.insert(p2);

		template.indexOps(Person.class) //
				.ensureIndex(new Index().on("age", Direction.DESC).unique()) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.getCollection(template.getCollectionName(Person.class)).flatMapMany(MongoCollection::listIndexes)
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(indexInfo -> {

					assertThat(indexInfo).hasSize(2);
					Object indexKey = null;
					boolean unique = false;
					for (Document ix : indexInfo) {

						if ("age_-1".equals(ix.get("name"))) {
							indexKey = ix.get("key");
							unique = (Boolean) ix.get("unique");
						}
					}
					assertThat((Document) indexKey).containsEntry("age", -1);
					assertThat(unique).isTrue();
				}).verifyComplete();
	}

	@RepeatFailedTest(3) // DATAMONGO-1444
	void getIndexInfoShouldReturnCorrectIndex() {

		Person p1 = new Person("Oliver");
		p1.setAge(25);
		template.insert(p1) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.indexOps(Person.class) //
				.ensureIndex(new Index().on("age", Direction.DESC).unique()) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.indexOps(Person.class).getIndexInfo().collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(indexInfos -> {

					assertThat(indexInfos).hasSize(2);

					IndexInfo ii = indexInfos.get(1);
					assertThat(ii.isUnique()).isTrue();
					assertThat(ii.isSparse()).isFalse();

					assertThat(ii.getIndexFields()).contains(IndexField.create("age", Direction.DESC), atIndex(0));
				}).verifyComplete();
	}

	@RepeatFailedTest(3) // DATAMONGO-1444, DATAMONGO-2264
	void testReadIndexInfoForIndicesCreatedViaMongoShellCommands() {

		template.indexOps(Person.class).dropAllIndexes() //
				.as(StepVerifier::create) //
				.verifyComplete();

		template.indexOps(Person.class).getIndexInfo() //
				.as(StepVerifier::create) //
				.verifyComplete();

		factory.getMongoDatabase() //
				.flatMapMany(db -> db.getCollection(template.getCollectionName(Person.class))
						.createIndex(new Document("age", -1), new IndexOptions().unique(true).sparse(true)))
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.getCollection(template.getCollectionName(Person.class)).flatMapMany(MongoCollection::listIndexes)
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(indexInfos -> {

					Document indexKey = null;
					boolean unique = false;

					for (Document document : indexInfos) {

						if ("age_-1".equals(document.get("name"))) {
							indexKey = (org.bson.Document) document.get("key");
							unique = (Boolean) document.get("unique");
						}
					}

					assertThat(indexKey).containsEntry("age", -1);
					assertThat(unique).isTrue();
				}).verifyComplete();

		Flux.from(template.indexOps(Person.class).getIndexInfo().collectList()) //
				.as(StepVerifier::create) //
				.consumeNextWith(indexInfos -> {

					IndexInfo info = indexInfos.get(1);
					assertThat(info.isUnique()).isTrue();
					assertThat(info.isSparse()).isTrue();

					assertThat(info.getIndexFields()).contains(IndexField.create("age", Direction.DESC), atIndex(0));
				}).verifyComplete();
	}

	@RepeatFailedTest(3) // DATAMONGO-1928
	void shouldCreateIndexOnAccess() {

		template.getCollection("indexedSample").flatMapMany(it -> it.listIndexes(Document.class)) //
				.as(StepVerifier::create) //
				.expectNextCount(0) //
				.verifyComplete();

		template.findAll(IndexedSample.class).defaultIfEmpty(new IndexedSample()) //
				.delayElements(Duration.ofMillis(500)) // TODO: check if 4.2.0 server GA still requires this timeout
				.then()
				.as(StepVerifier::create) //
				.verifyComplete();

		template.getCollection("indexedSample").flatMapMany(it -> it.listIndexes(Document.class)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@RepeatFailedTest(3) // DATAMONGO-1928, DATAMONGO-2264
	void indexCreationShouldFail() throws InterruptedException {

		factory.getMongoDatabase() //
				.flatMapMany(db -> db.getCollection("indexfail") //
						.createIndex(new Document("field", 1), new IndexOptions().name("foo").unique(true).sparse(true)))
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		BlockingQueue<Throwable> queue = new LinkedBlockingQueue<>();
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory, this.template.getConverter(), queue::add);

		template.findAll(IndexCreationShouldFail.class).subscribe();

		Throwable failure = queue.poll(10, TimeUnit.SECONDS);

		assertThat(failure).isNotNull().isInstanceOf(DataIntegrityViolationException.class);
	}

	static class Sample {

		@Id String id;
		String field;

		public Sample() {}

		public Sample(String id, String field) {
			this.id = id;
			this.field = field;
		}

		public String getId() {
			return this.id;
		}

		public String getField() {
			return this.field;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setField(String field) {
			this.field = field;
		}

		public String toString() {
			return "ReactiveMongoTemplateIndexTests.Sample(id=" + this.getId() + ", field=" + this.getField() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document
	static class IndexedSample {

		@Id String id;
		@Indexed String field;

		public String getId() {
			return this.id;
		}

		public String getField() {
			return this.field;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setField(String field) {
			this.field = field;
		}

		public String toString() {
			return "ReactiveMongoTemplateIndexTests.IndexedSample(id=" + this.getId() + ", field=" + this.getField() + ")";
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document("indexfail")
	static class IndexCreationShouldFail {

		@Id String id;
		@Indexed(name = "foo") String field;

		public String getId() {
			return this.id;
		}

		public String getField() {
			return this.field;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setField(String field) {
			this.field = field;
		}

		public String toString() {
			return "ReactiveMongoTemplateIndexTests.IndexCreationShouldFail(id=" + this.getId() + ", field=" + this.getField()
					+ ")";
		}
	}
}
