/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.data.Index.atIndex;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.Success;

/**
 * Integration test for index creation via {@link ReactiveMongoTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveMongoTemplateIndexTests {

	@Rule public ExpectedException thrown = ExpectedException.none();

	@Autowired SimpleReactiveMongoDatabaseFactory factory;
	@Autowired ReactiveMongoTemplate template;
	@Autowired MongoClient client;

	@Before
	public void setUp() {

		StepVerifier.create(template.getCollection("person").drop()).expectNext(Success.SUCCESS).verifyComplete();
		StepVerifier.create(template.getCollection("indexfail").drop()).expectNext(Success.SUCCESS).verifyComplete();
		StepVerifier.create(template.getCollection("indexedSample").drop()).expectNext(Success.SUCCESS).verifyComplete();
	}

	@After
	public void cleanUp() {}

	@Test // DATAMONGO-1444
	public void testEnsureIndexShouldCreateIndex() {

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

		Flux.from(template.getCollection(template.getCollectionName(Person.class)).listIndexes()).collectList() //
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

	@Test // DATAMONGO-1444
	public void getIndexInfoShouldReturnCorrectIndex() {

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

	@Test // DATAMONGO-1444
	public void testReadIndexInfoForIndicesCreatedViaMongoShellCommands() {

		String command = "db." + template.getCollectionName(Person.class)
				+ ".createIndex({'age':-1}, {'unique':true, 'sparse':true}), 1";

		template.indexOps(Person.class).dropAllIndexes() //
				.as(StepVerifier::create) //
				.verifyComplete();

		template.indexOps(Person.class).getIndexInfo() //
				.as(StepVerifier::create) //
				.verifyComplete();

		Flux.from(factory.getMongoDatabase().runCommand(new org.bson.Document("eval", command))) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		ListIndexesPublisher<Document> listIndexesPublisher = template
				.getCollection(template.getCollectionName(Person.class)).listIndexes();

		Flux.from(listIndexesPublisher).collectList() //
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

					assertThat(indexKey).containsEntry("age", -1D);
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

	@Test // DATAMONGO-1928
	public void shouldCreateIndexOnAccess() {

		StepVerifier.create(template.getCollection("indexedSample").listIndexes(Document.class)).expectNextCount(0)
				.verifyComplete();

		template.findAll(IndexedSample.class) //
				.as(StepVerifier::create) //
				.verifyComplete();

		StepVerifier.create(template.getCollection("indexedSample").listIndexes(Document.class)).expectNextCount(2)
				.verifyComplete();
	}

	@Test // DATAMONGO-1928
	public void indexCreationShouldFail() throws InterruptedException {

		String command = "db.indexfail" + ".createIndex({'field':1}, {'name':'foo', 'unique':true, 'sparse':true}), 1";

		Flux.from(factory.getMongoDatabase().runCommand(new org.bson.Document("eval", command))) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		BlockingQueue<Throwable> queue = new LinkedBlockingQueue<>();
		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory, this.template.getConverter(), queue::add);

		template.findAll(IndexCreationShouldFail.class).subscribe();

		Throwable failure = queue.poll(10, TimeUnit.SECONDS);

		assertThat(failure).isNotNull().isInstanceOf(DataIntegrityViolationException.class);
	}

	@Data
	static class Sample {

		@Id String id;
		String field;

		public Sample() {}

		public Sample(String id, String field) {
			this.id = id;
			this.field = field;
		}
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document
	static class IndexedSample {

		@Id String id;
		@Indexed String field;
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document("indexfail")
	static class IndexCreationShouldFail {

		@Id String id;
		@Indexed(name = "foo") String field;
	}
}
