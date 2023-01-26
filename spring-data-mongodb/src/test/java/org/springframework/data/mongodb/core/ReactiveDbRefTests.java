/*
 * Copyright 2023 the original author or authors.
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

import lombok.EqualsAndHashCode;
import lombok.ToString;
import reactor.test.StepVerifier;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.test.util.Assertions;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
public class ReactiveDbRefTests {

	private static final String DB_NAME = "reactive-dbref-tests";
	private static @Client MongoClient client;

	ReactiveMongoTemplate template = new ReactiveMongoTemplate(MongoClients.create(), DB_NAME);
	MongoTemplate syncTemplate = new MongoTemplate(com.mongodb.client.MongoClients.create(), DB_NAME);

	@Test // GH-2496
	void loadDbRef() {

		Bar barSource = new Bar();
		barSource.id = "bar-1";
		barSource.value = "bar-1-value";
		syncTemplate.save(barSource);

		Foo fooSource = new Foo();
		fooSource.id = "foo-1";
		fooSource.name = "foo-1-name";
		fooSource.bar = barSource;
		syncTemplate.save(fooSource);

		template.query(Foo.class).matching(Criteria.where("id").is(fooSource.id)).first().as(StepVerifier::create)
				.consumeNextWith(foo -> {
					Assertions.assertThat(foo.bar).isEqualTo(barSource);
				}).verifyComplete();

	}

	@Test // GH-2496
	void loadListOFDbRef() {

		Bar bar1Source = new Bar();
		bar1Source.id = "bar-1";
		bar1Source.value = "bar-1-value";
		syncTemplate.save(bar1Source);

		Bar bar2Source = new Bar();
		bar2Source.id = "bar-1";
		bar2Source.value = "bar-1-value";
		syncTemplate.save(bar2Source);

		Foo fooSource = new Foo();
		fooSource.id = "foo-1";
		fooSource.name = "foo-1-name";
		fooSource.bars = List.of(bar1Source, bar2Source);
		syncTemplate.save(fooSource);

		template.query(Foo.class).matching(Criteria.where("id").is(fooSource.id)).first().as(StepVerifier::create)
				.consumeNextWith(foo -> {
					Assertions.assertThat(foo.bars).containsExactly(bar1Source, bar2Source);
				}).verifyComplete();

	}

	@Test // GH-2496
	void loadDbRefHoldingJetAnotherOne() {

		Roo rooSource = new Roo();
		rooSource.id = "roo-1";
		rooSource.name = "roo-the-kangaroo";
		syncTemplate.save(rooSource);

		Bar barSource = new Bar();
		barSource.id = "bar-1";
		barSource.value = "bar-1-value";
		barSource.roo = rooSource;
		syncTemplate.save(barSource);

		Foo fooSource = new Foo();
		fooSource.id = "foo-1";
		fooSource.name = "foo-1-name";
		fooSource.bar = barSource;
		syncTemplate.save(fooSource);

		template.query(Foo.class).matching(Criteria.where("id").is(fooSource.id)).first().as(StepVerifier::create)
				.consumeNextWith(foo -> {
					Assertions.assertThat(foo.bar).isEqualTo(barSource);
					Assertions.assertThat(foo.bar.roo).isEqualTo(rooSource);
				}).verifyComplete();

	}

	@Test // GH-2496
	void loadListOfDbRefHoldingJetAnotherOne() {

		Roo rooSource = new Roo();
		rooSource.id = "roo-1";
		rooSource.name = "roo-the-kangaroo";
		syncTemplate.save(rooSource);

		Bar bar1Source = new Bar();
		bar1Source.id = "bar-1";
		bar1Source.value = "bar-1-value";
		bar1Source.roo = rooSource;
		syncTemplate.save(bar1Source);

		Bar bar2Source = new Bar();
		bar2Source.id = "bar-2";
		bar2Source.value = "bar-2-value";
		syncTemplate.save(bar2Source);

		Foo fooSource = new Foo();
		fooSource.id = "foo-1";
		fooSource.name = "foo-1-name";
		fooSource.bars = List.of(bar1Source, bar2Source);
		syncTemplate.save(fooSource);

		template.query(Foo.class).matching(Criteria.where("id").is(fooSource.id)).first().as(StepVerifier::create)
				.consumeNextWith(foo -> {
					Assertions.assertThat(foo.bars).containsExactly(bar1Source, bar2Source);
				}).verifyComplete();

	}

	@ToString
	static class Foo {
		String id;
		String name;

		@DBRef //
		Bar bar;

		@DBRef //
		List<Bar> bars;
	}

	@ToString
	@EqualsAndHashCode
	static class Bar {
		String id;
		String value;

		@DBRef Roo roo;
	}

	@ToString
	@EqualsAndHashCode
	static class Roo {
		String id;
		String name;
	}
}
