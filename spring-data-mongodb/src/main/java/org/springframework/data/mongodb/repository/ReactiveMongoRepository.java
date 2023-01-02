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
package org.springframework.data.mongodb.repository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.query.ReactiveQueryByExampleExecutor;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;

/**
 * Mongo specific {@link org.springframework.data.repository.Repository} interface with reactive support.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@NoRepositoryBean
public interface ReactiveMongoRepository<T, ID>
		extends ReactiveCrudRepository<T, ID>, ReactiveSortingRepository<T, ID>, ReactiveQueryByExampleExecutor<T> {

	/**
	 * Inserts the given entity. Assumes the instance to be new to be able to apply insertion optimizations. Use the
	 * returned instance for further operations as the save operation might have changed the entity instance completely.
	 * Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Mono<S> insert(S entity);

	/**
	 * Inserts the given entities. Assumes the instance to be new to be able to apply insertion optimizations. Use the
	 * returned instance for further operations as the save operation might have changed the entity instance completely.
	 * Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Flux<S> insert(Iterable<S> entities);

	/**
	 * Inserts the given entities. Assumes the instance to be new to be able to apply insertion optimizations. Use the
	 * returned instance for further operations as the save operation might have changed the entity instance completely.
	 * Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Flux<S> insert(Publisher<S> entities);

	/**
	 * Returns all entities matching the given {@link Example}. In case no match could be found an empty {@link Flux} is
	 * returned. <br />
	 * By default the {@link Example} uses typed matching restricting it to probe assignable types. For example, when
	 * sticking with the default type key ({@code _class}), the query has restrictions such as
	 * <code>_class : &#123; $in : [com.acme.Person] &#125;</code>. <br />
	 * To avoid the above mentioned type restriction use an {@link org.springframework.data.mongodb.core.query.UntypedExampleMatcher} with
	 * {@link Example#of(Object, org.springframework.data.domain.ExampleMatcher)}.
	 *
	 * @see org.springframework.data.repository.query.ReactiveQueryByExampleExecutor#findAll(org.springframework.data.domain.Example)
	 */
	@Override
	<S extends T> Flux<S> findAll(Example<S> example);

	/**
	 * Returns all entities matching the given {@link Example} applying the given {@link Sort}. In case no match could be
	 * found an empty {@link Flux} is returned. <br />
	 * By default the {@link Example} uses typed matching restricting it to probe assignable types. For example, when
	 * sticking with the default type key ({@code _class}), the query has restrictions such as
	 * <code>_class : &#123; $in : [com.acme.Person] &#125;</code>. <br />
	 * To avoid the above mentioned type restriction use an {@link org.springframework.data.mongodb.core.query.UntypedExampleMatcher} with
	 * {@link Example#of(Object, org.springframework.data.domain.ExampleMatcher)}.
	 *
	 * @see org.springframework.data.repository.query.ReactiveQueryByExampleExecutor#findAll(org.springframework.data.domain.Example,
	 *      org.springframework.data.domain.Sort)
	 */
	@Override
	<S extends T> Flux<S> findAll(Example<S> example, Sort sort);

}
