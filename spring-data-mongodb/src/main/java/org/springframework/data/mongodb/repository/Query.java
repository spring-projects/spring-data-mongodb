/*
 * Copyright 2011-2023 the original author or authors.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.annotation.QueryAnnotation;
import org.springframework.data.mongodb.core.annotation.Collation;

/**
 * Annotation to declare finder queries directly on repository methods. Both attributes allow using a placeholder
 * notation of {@code ?0}, {@code ?1} and so on.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Jorge Rodríguez
 */
@Collation
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
@QueryAnnotation
@Hint
@ReadPreference
public @interface Query {

	/**
	 * Takes a MongoDB JSON string to define the actual query to be executed. This one will take precedence over the
	 * method name then.
	 *
	 * @return empty {@link String} by default.
	 */
	String value() default "";

	/**
	 * Defines the fields that should be returned for the given query. Note that only these fields will make it into the
	 * domain object returned.
	 *
	 * @return empty {@link String} by default.
	 */
	String fields() default "";

	/**
	 * Returns whether the query defined should be executed as count projection.
	 *
	 * @since 1.3
	 * @return {@literal false} by default.
	 */
	boolean count() default false;

	/**
	 * Returns whether the query defined should be executed as exists projection.
	 *
	 * @since 1.10
	 * @return {@literal false} by default.
	 */
	boolean exists() default false;

	/**
	 * Returns whether the query should delete matching documents.
	 *
	 * @since 1.5
	 * @return {@literal false} by default.
	 */
	boolean delete() default false;

	/**
	 * Defines a default sort order for the given query. <strong>NOTE:</strong> The so set defaults can be altered /
	 * overwritten using an explicit {@link org.springframework.data.domain.Sort} argument of the query method.
	 *
	 * <pre>
	 * <code>
	 *
	 * 		&#64;Query(sort = "{ age : -1 }") // order by age descending
	 * 		List&lt;Person&gt; findByFirstname(String firstname);
	 * </code>
	 * </pre>
	 *
	 * @return empty {@link String} by default.
	 * @since 2.1
	 */
	String sort() default "";

	/**
	 * Defines the collation to apply when executing the query.
	 *
	 * <pre class="code">
	 * // Fixed value
	 * &#64;Query(collation = "en_US")
	 * List&lt;Entry&gt; findAllByFixedCollation();
	 *
	 * // Fixed value as Document
	 * &#64;Query(collation = "{ 'locale' :  'en_US' }")
	 * List&lt;Entry&gt; findAllByFixedJsonCollation();
	 *
	 * // Dynamic value as String
	 * &#64;Query(collation = "?0")
	 * List&lt;Entry&gt; findAllByDynamicCollation(String collation);
	 *
	 * // Dynamic value as Document
	 * &#64;Query(collation = "{ 'locale' :  ?0 }")
	 * List&lt;Entry&gt; findAllByDynamicJsonCollation(String collation);
	 *
	 * // SpEL expression
	 * &#64;Query(collation = "?#{[0]}")
	 * List&lt;Entry&gt; findAllByDynamicSpElCollation(String collation);
	 * </pre>
	 *
	 * @return an empty {@link String} by default.
	 * @since 2.2
	 */
	@AliasFor(annotation = Collation.class, attribute = "value")
	String collation() default "";

	/**
	 * The name of the index to use. {@code @Query(value = "...", hint = "lastname-idx")} can be used as shortcut for:
	 *
	 * <pre class="code">
	 * &#64;Query(...)
	 * &#64;Hint("lastname-idx")
	 * List&lt;User&gt; findAllByLastname(String collation);
	 * </pre>
	 *
	 * @return the index name.
	 * @since 4.1
	 * @see Hint#indexName()
	 */
	@AliasFor(annotation = Hint.class, attribute = "indexName")
	String hint() default "";

	/**
	 * The mode of the read preference to use. This attribute
	 * ({@code @Query(value = "...", readPreference = "secondary")}) is an alias for:
	 *
	 * <pre class="code">
	 * &#64;Query(...)
	 * &#64;ReadPreference("secondary")
	 * List&lt;User&gt; findAllByLastname(String lastname);
	 * </pre>
	 *
	 * @return the index name.
	 * @since 4.2
	 * @see ReadPreference#value()
	 */
	@AliasFor(annotation = ReadPreference.class, attribute = "value")
	String readPreference() default "";
}
