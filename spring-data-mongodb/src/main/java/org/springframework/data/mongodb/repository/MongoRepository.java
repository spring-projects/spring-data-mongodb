/*
 * Copyright 2010-2023 the original author or authors.
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

import java.util.List;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.data.repository.ListPagingAndSortingRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.query.QueryByExampleExecutor;

/**
 * Mongo specific {@link org.springframework.data.repository.Repository} interface.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Khaled Baklouti
 */
@NoRepositoryBean
public interface MongoRepository<T, ID>
		extends ListCrudRepository<T, ID>, ListPagingAndSortingRepository<T, ID>, QueryByExampleExecutor<T> {

	/**
	 * Inserts the given entity. Assumes the instance to be new to be able to apply insertion optimizations. Use the
	 * returned instance for further operations as the save operation might have changed the entity instance completely.
	 * Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the saved entity
	 * @since 1.7
	 */
	<S extends T> S insert(S entity);

	/**
	 * Inserts the given entities. Assumes the given entities to have not been persisted yet and thus will optimize the
	 * insert over a call to {@link #saveAll(Iterable)}. Prefer using {@link #saveAll(Iterable)} to avoid the usage of store
	 * specific API.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the saved entities
	 * @since 1.7
	 */
	<S extends T> List<S> insert(Iterable<S> entities);

	/**
	 * Returns all entities matching the given {@link Example}. In case no match could be found an empty {@link List} is
	 * returned. <br />
	 * By default the {@link Example} uses typed matching restricting it to probe assignable types. For example, when
	 * sticking with the default type key ({@code _class}), the query has restrictions such as
	 * <code>_class : &#123; $in : [com.acme.Person] &#125;</code>. <br />
	 * To avoid the above mentioned type restriction use an {@link org.springframework.data.mongodb.core.query.UntypedExampleMatcher} with
	 * {@link Example#of(Object, org.springframework.data.domain.ExampleMatcher)}.
	 *
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example)
	 */
	@Override
	<S extends T> List<S> findAll(Example<S> example);

	/**
	 * Returns all entities matching the given {@link Example} applying the given {@link Sort}. In case no match could be
	 * found an empty {@link List} is returned. <br />
	 * By default the {@link Example} uses typed matching restricting it to probe assignable types. For example, when
	 * sticking with the default type key ({@code _class}), the query has restrictions such as
	 * <code>_class : &#123; $in : [com.acme.Person] &#125;</code>. <br />
	 * To avoid the above mentioned type restriction use an {@link org.springframework.data.mongodb.core.query.UntypedExampleMatcher} with
	 * {@link Example#of(Object, org.springframework.data.domain.ExampleMatcher)}.
	 *
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example,
	 *      org.springframework.data.domain.Sort)
	 */
	@Override
	<S extends T> List<S> findAll(Example<S> example, Sort sort);
}
