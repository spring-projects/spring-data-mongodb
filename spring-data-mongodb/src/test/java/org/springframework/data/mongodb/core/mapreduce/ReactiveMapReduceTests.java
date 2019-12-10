/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.mapreduce;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @currentRead Beyond the Shadows - Brent Weeks
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveMapReduceTests {

	@Autowired SimpleReactiveMongoDatabaseFactory factory;
	@Autowired ReactiveMongoTemplate template;

	private String mapFunction = "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }";
	private String reduceFunction = "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}";

	@Before
	public void setUp() {

		template.dropCollection(ValueObject.class) //
				.mergeWith(template.dropCollection("jmr1")) //
				.mergeWith(template.dropCollection("jmr1_out")) //
				.mergeWith(Mono.from(factory.getMongoDatabase("reactive-jrm1-out-db").drop()).then()).as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1890
	public void mapReduceWithInlineResult() {

		createMapReduceData();

		template
				.mapReduce(new Query(), Person.class, "jmr1", ValueObject.class, mapFunction, reduceFunction,
						MapReduceOptions.options())
				.buffer(4).as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result).containsExactlyInAnyOrder(new ValueObject("a", 1), new ValueObject("b", 2),
							new ValueObject("c", 2), new ValueObject("d", 1));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2027
	public void shouldStoreResultInCollection() {

		createMapReduceData();

		template.mapReduce(new Query(), Person.class, "jmr1", ValueObject.class, mapFunction, reduceFunction, //
				MapReduceOptions.options().outputCollection("mapreduceout")).as(StepVerifier::create) //
				.expectNextCount(4) //
				.verifyComplete();

		template.find(new Query(), ValueObject.class, "mapreduceout").buffer(4).as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result).containsExactlyInAnyOrder(new ValueObject("a", 1), new ValueObject("b", 2),
							new ValueObject("c", 2), new ValueObject("d", 1));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1890
	public void mapReduceWithInlineAndFilterQuery() {

		createMapReduceData();

		template
				.mapReduce(query(where("x").ne(new String[] { "a", "b" })), ValueObject.class, "jmr1", ValueObject.class,
						mapFunction, reduceFunction, MapReduceOptions.options())
				.buffer(4).as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result).containsExactlyInAnyOrder(new ValueObject("b", 1), new ValueObject("c", 2),
							new ValueObject("d", 1));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1890, DATAMONGO-2027
	public void mapReduceWithOutputCollection() {

		createMapReduceData();

		template
				.mapReduce(new Query(), ValueObject.class, "jmr1", ValueObject.class, mapFunction, reduceFunction,
						MapReduceOptions.options().outputCollection("jmr1_out"))
				.as(StepVerifier::create).expectNextCount(4).verifyComplete();

		template.find(new Query(), ValueObject.class, "jmr1_out").buffer(4).as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result).containsExactlyInAnyOrder(new ValueObject("a", 1), new ValueObject("b", 2),
							new ValueObject("c", 2), new ValueObject("d", 1));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2027
	public void mapReduceWithOutputDatabase() {

		createMapReduceData();

		template
				.mapReduce(new Query(), ValueObject.class, "jmr1", ValueObject.class, mapFunction, reduceFunction,
						MapReduceOptions.options().outputDatabase("reactive-jrm1-out-db").outputCollection("jmr1_out"))
				.as(StepVerifier::create).expectNextCount(4).verifyComplete();

		Flux.from(factory.getMongoDatabase("reactive-jrm1-out-db").listCollectionNames()).buffer(10)
				.map(list -> list.contains("jmr1_out")).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1890
	public void mapReduceWithInlineAndMappedFilterQuery() {

		createMapReduceData();

		template
				.mapReduce(query(where("values").ne(new String[] { "a", "b" })), MappedFieldsValueObject.class, "jmr1",
						ValueObject.class, mapFunction, reduceFunction, MapReduceOptions.options())
				.buffer(4).as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result).containsExactlyInAnyOrder(new ValueObject("b", 1), new ValueObject("c", 2),
							new ValueObject("d", 1));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1890
	public void mapReduceWithInlineFilterQueryAndExtractedCollection() {

		createMapReduceData();

		template
				.mapReduce(query(where("values").ne(new String[] { "a", "b" })), MappedFieldsValueObject.class,
						ValueObject.class, mapFunction, reduceFunction, MapReduceOptions.options())
				.buffer(4).as(StepVerifier::create) //
				.consumeNextWith(result -> {
					assertThat(result).containsExactlyInAnyOrder(new ValueObject("b", 1), new ValueObject("c", 2),
							new ValueObject("d", 1));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1890
	public void throwsExceptionWhenTryingToLoadFunctionsFromDisk() {

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> template.mapReduce(new Query(),
				Person.class, "foo", ValueObject.class, "classpath:map.js", "classpath:reduce.js", MapReduceOptions.options()))
				.withMessageContaining("classpath:map.js");
	}

	private void createMapReduceData() {

		MongoCollection<Document> collection = factory.getMongoDatabase().getCollection("jmr1", Document.class);

		StepVerifier
				.create(collection.insertMany(Arrays.asList(new Document("x", Arrays.asList("a", "b")),
						new Document("x", Arrays.asList("b", "c")), new Document("x", Arrays.asList("c", "d")))))
				.expectNextCount(1) //
				.verifyComplete();
	}

	@org.springframework.data.mongodb.core.mapping.Document("jmr1")
	@Data
	static class MappedFieldsValueObject {

		@Field("x") String[] values;
	}
}
