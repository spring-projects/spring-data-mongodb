/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * Integration test for {@link MongoTemplate}.
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

	@Before
	public void setUp() {
		StepVerifier.create(template.dropCollection(Person.class)).verifyComplete();
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

		StepVerifier
				.create(template.indexOps(Person.class) //
						.ensureIndex(new Index().on("age", Direction.DESC).unique())) //
				.expectNextCount(1) //
				.verifyComplete();

		MongoCollection<Document> coll = template.getCollection(template.getCollectionName(Person.class));
		StepVerifier.create(Flux.from(coll.listIndexes()).collectList()).consumeNextWith(indexInfo -> {

			assertThat(indexInfo.size(), is(2));
			Object indexKey = null;
			boolean unique = false;
			for (Document ix : indexInfo) {

				if ("age_-1".equals(ix.get("name"))) {
					indexKey = ix.get("key");
					unique = (Boolean) ix.get("unique");
				}
			}
			assertThat(((Document) indexKey), hasEntry("age", -1));
			assertThat(unique, is(true));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void getIndexInfoShouldReturnCorrectIndex() {

		Person p1 = new Person("Oliver");
		p1.setAge(25);
		StepVerifier.create(template.insert(p1)).expectNextCount(1).verifyComplete();

		StepVerifier
				.create(template.indexOps(Person.class) //
						.ensureIndex(new Index().on("age", Direction.DESC).unique())) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(template.indexOps(Person.class).getIndexInfo().collectList()).consumeNextWith(indexInfos -> {

			assertThat(indexInfos.size(), is(2));

			IndexInfo ii = indexInfos.get(1);
			assertThat(ii.isUnique(), is(true));
			assertThat(ii.isSparse(), is(false));

			List<IndexField> indexFields = ii.getIndexFields();
			IndexField field = indexFields.get(0);

			assertThat(field, is(IndexField.create("age", Direction.DESC)));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void testReadIndexInfoForIndicesCreatedViaMongoShellCommands() {

		String command = "db." + template.getCollectionName(Person.class)
				+ ".createIndex({'age':-1}, {'unique':true, 'sparse':true}), 1";
		StepVerifier.create(template.indexOps(Person.class).dropAllIndexes()).verifyComplete();

		StepVerifier.create(template.indexOps(Person.class).getIndexInfo()).verifyComplete();

		StepVerifier.create(factory.getMongoDatabase().runCommand(new org.bson.Document("eval", command))) //
				.expectNextCount(1) //
				.verifyComplete();

		ListIndexesPublisher<Document> listIndexesPublisher = template
				.getCollection(template.getCollectionName(Person.class)).listIndexes();

		StepVerifier.create(Flux.from(listIndexesPublisher).collectList()).consumeNextWith(indexInfos -> {

			Document indexKey = null;
			boolean unique = false;

			for (Document document : indexInfos) {

				if ("age_-1".equals(document.get("name"))) {
					indexKey = (org.bson.Document) document.get("key");
					unique = (Boolean) document.get("unique");
				}
			}

			assertThat(indexKey, hasEntry("age", -1D));
			assertThat(unique, is(true));
		}).verifyComplete();

		StepVerifier.create(Flux.from(template.indexOps(Person.class).getIndexInfo().collectList()))
				.consumeNextWith(indexInfos -> {

					IndexInfo info = indexInfos.get(1);
					assertThat(info.isUnique(), is(true));
					assertThat(info.isSparse(), is(true));

					List<IndexField> indexFields = info.getIndexFields();
					IndexField field = indexFields.get(0);

					assertThat(field, is(IndexField.create("age", Direction.DESC)));
				}).verifyComplete();
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
}
