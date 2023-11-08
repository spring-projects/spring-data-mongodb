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
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.repository.Repository;

/**
 * Unit test for {@link ReactiveMongoRepositoryFactory}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ReactiveMongoRepositoryFactoryUnitTests {

	@Mock ReactiveMongoTemplate template;

	MongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, new MongoMappingContext());

	@BeforeEach
	public void setUp() {
		when(template.getConverter()).thenReturn(converter);
	}

	@Test // GH-2971
	void considersCrudMethodMetadata() {

		when(template.findOne(any(), any(), anyString())).thenReturn(Mono.empty());

		ReactiveMongoRepositoryFactory factory = new ReactiveMongoRepositoryFactory(template);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);
		repository.findById(42L);

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(template).findOne(captor.capture(), eq(Person.class), eq("person"));

		Query value = captor.getValue();
		assertThat(value.getReadPreference()).isEqualTo(com.mongodb.ReadPreference.secondary());
	}

	@Test // GH-2971
	void ignoresCrudMethodMetadataOnNonAnnotatedMethods() {

		when(template.find(any(), any(), anyString())).thenReturn(Flux.empty());

		ReactiveMongoRepositoryFactory factory = new ReactiveMongoRepositoryFactory(template);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);
		repository.findAllById(Set.of(42L));

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
		verify(template).find(captor.capture(), eq(Person.class), eq("person"));

		Query value = captor.getValue();
		assertThat(value.getReadPreference()).isNull();
	}

	interface MyPersonRepository extends ReactiveCrudRepository<Person, Long> {

		@ReadPreference("secondary")
		Mono<Person> findById(Long id);
	}
}
