/*
 * Copyright 2019-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link ReactiveChangeStreamOperationSupport}.
 *
 * @author Christoph Strobl
 * @currentRead Dawn Cook - The Decoy Princess
 */
@ExtendWith(MockitoExtension.class)
class ReactiveChangeStreamOperationSupportUnitTests {

	@Mock ReactiveMongoTemplate template;
	private ReactiveChangeStreamOperationSupport changeStreamSupport;

	@BeforeEach
	void setUp() {
		when(template.changeStream(any(), any(), any())).thenReturn(Flux.empty());
		changeStreamSupport = new ReactiveChangeStreamOperationSupport(template);
	}

	@Test // DATAMONGO-2089
	void listenWithoutDomainTypeUsesDocumentAsDefault() {

		changeStreamSupport.changeStream(Document.class).listen().subscribe();

		verify(template).changeStream(isNull(), eq(ChangeStreamOptions.empty()), eq(Document.class));
	}

	@Test // DATAMONGO-2089
	void listenWithDomainTypeUsesSourceAsTarget() {

		changeStreamSupport.changeStream(Person.class).listen().subscribe();

		verify(template).changeStream(isNull(), eq(ChangeStreamOptions.empty()), eq(Person.class));
	}

	@Test // DATAMONGO-2089
	void collectionNameIsPassedOnCorrectly() {

		changeStreamSupport.changeStream(Person.class).watchCollection("star-wars").listen().subscribe();

		verify(template).changeStream(eq("star-wars"), eq(ChangeStreamOptions.empty()), eq(Person.class));
	}

	@Test // DATAMONGO-2089
	void listenWithDomainTypeCreatesTypedAggregation() {

		Criteria criteria = where("operationType").is("insert");
		changeStreamSupport.changeStream(Person.class).filter(criteria).listen().subscribe();

		ArgumentCaptor<ChangeStreamOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(ChangeStreamOptions.class);
		verify(template).changeStream(isNull(), optionsArgumentCaptor.capture(), eq(Person.class));

		assertThat(optionsArgumentCaptor.getValue().getFilter()).hasValueSatisfying(it -> {

			assertThat(it).isInstanceOf(TypedAggregation.class);
			TypedAggregation<?> aggregation = (TypedAggregation<?>) it;

			assertThat(aggregation.getInputType()).isEqualTo(Person.class);
			assertThat(extractPipeline(aggregation))
					.containsExactly(new Document("$match", new Document("operationType", "insert")));
		});
	}

	@Test // DATAMONGO-2089
	void listenWithoutDomainTypeCreatesUntypedAggregation() {

		Criteria criteria = where("operationType").is("insert");
		changeStreamSupport.changeStream(Document.class).filter(criteria).listen().subscribe();

		ArgumentCaptor<ChangeStreamOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(ChangeStreamOptions.class);
		verify(template).changeStream(isNull(), optionsArgumentCaptor.capture(), eq(Document.class));

		assertThat(optionsArgumentCaptor.getValue().getFilter()).hasValueSatisfying(it -> {

			assertThat(it).isInstanceOf(Aggregation.class);
			assertThat(it).isNotInstanceOf(TypedAggregation.class);

			Aggregation aggregation = (Aggregation) it;

			assertThat(extractPipeline(aggregation))
					.containsExactly(new Document("$match", new Document("operationType", "insert")));
		});
	}

	@Test // DATAMONGO-2089
	void optionsShouldBePassedOnCorrectly() {

		Document filter = new Document("$match", new Document("operationType", "insert"));

		changeStreamSupport.changeStream(Document.class).withOptions(options -> {
			options.filter(filter);
		}).listen().subscribe();

		ArgumentCaptor<ChangeStreamOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(ChangeStreamOptions.class);
		verify(template).changeStream(isNull(), optionsArgumentCaptor.capture(), eq(Document.class));

		assertThat(optionsArgumentCaptor.getValue()).satisfies(it -> {
			assertThat(it.getFilter().get()).isEqualTo(Collections.singletonList(filter));
		});
	}

	@Test // DATAMONGO-2089
	void optionsShouldBeCombinedCorrectly() {

		Document filter = new Document("$match", new Document("operationType", "insert"));
		Instant resumeTimestamp = Instant.now();

		changeStreamSupport.changeStream(Document.class).withOptions(options -> {
			options.filter(filter);
		}).resumeAt(resumeTimestamp).listen().subscribe();

		ArgumentCaptor<ChangeStreamOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(ChangeStreamOptions.class);
		verify(template).changeStream(isNull(), optionsArgumentCaptor.capture(), eq(Document.class));

		assertThat(optionsArgumentCaptor.getValue()).satisfies(it -> {

			assertThat(it.getFilter().get()).isEqualTo(Collections.singletonList(filter));
			assertThat(it.getResumeTimestamp()).contains(resumeTimestamp);
		});
	}

	private static List<Document> extractPipeline(Aggregation aggregation) {
		return aggregation.toDocument("person", Aggregation.DEFAULT_CONTEXT).get("pipeline", ArrayList.class);
	}
}
