/*
 * Copyright 2014-2019 the original author or authors.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.List;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.repository.CrudRepository;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface UserWithComplexIdRepository extends CrudRepository<UserWithComplexId, MyId> {

	@Query("{'_id': {$in: ?0}}")
	List<UserWithComplexId> findByUserIds(Collection<MyId> ids);

	@Query("{'_id': ?0}")
	UserWithComplexId getUserByComplexId(MyId id);

	@ComposedQueryAnnotation
	UserWithComplexId getUserUsingComposedAnnotationByComplexId(MyId id);

	@ComposedMetaAnnotation
	@Query("{'_id': {$in: ?0}}")
	List<UserWithComplexId> findUsersUsingComposedMetaAnnotationByUserIds(Collection<MyId> ids);

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.METHOD })
	@Document
	@Query
	@interface ComposedQueryAnnotation {

		@AliasFor(annotation = Query.class, attribute = "value")
		String myQuery() default "{'_id': ?0}";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.METHOD })
	@Meta
	@interface ComposedMetaAnnotation {

		@AliasFor(annotation = Meta.class, attribute = "maxExecutionTimeMs")
		long execTime() default -1;
	}
}
