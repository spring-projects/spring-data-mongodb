/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mongodb.core.annotation.Collation;

/**
 * Annotation for an entity or property that should be used as key for a
 * <a href="https://docs.mongodb.com/manual/core/index-wildcard/">Wildcard Index</a>. <br />
 * If placed on a {@link ElementType#TYPE type} that is a root level domain entity (one having an
 * {@link org.springframework.data.mongodb.core.mapping.Document} annotation) will advise the index creator to create a
 * wildcard index for it.
 *
 * <pre class="code">
 *
 * &#64;Document
 * &#64;WildcardIndexed
 * public class Product {
 *     ...
 * }
 *
 * db.product.createIndex({ "$**" : 1 } , {})
 * </pre>
 *
 * {@literal wildcardProjection} can be used to specify keys to in-/exclude in the index.
 *
 * <pre class="code">
 *
 * &#64;Document
 * &#64;WildcardIndexed(wildcardProjection = "{ 'userMetadata.age' : 0 }")
 * public class User {
 *     private &#64;Id String id;
 *     private UserMetadata userMetadata;
 * }
 *
 *
 * db.user.createIndex(
 *   { "$**" : 1 },
 *   { "wildcardProjection" :
 *     { "userMetadata.age" : 0 }
 *   }
 * )
 * </pre>
 *
 * Wildcard indexes can also be expressed by adding the annotation directly to the field. Please note that
 * {@literal wildcardProjection} is not allowed on nested paths.
 *
 * <pre class="code">
 * &#64;Document
 * public class User {
 *
 *     private &#64;Id String id;
 *
 *     &#64;WildcardIndexed
 *     private UserMetadata userMetadata;
 * }
 *
 *
 * db.user.createIndex({ "userMetadata.$**" : 1 }, {})
 * </pre>
 *
 * @author Christoph Strobl
 * @since 3.3
 */
@Collation
@Documented
@Target({ ElementType.TYPE, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface WildcardIndexed {

	/**
	 * Index name either as plain value or as {@link org.springframework.expression.spel.standard.SpelExpression template
	 * expression}. <br />
	 * <br />
	 * The name will only be applied as is when defined on root level. For usage on nested or embedded structures the
	 * provided name will be prefixed with the path leading to the entity.
	 *
	 * @return empty by default.
	 */
	String name() default "";

	/**
	 * If set to {@literal true} then MongoDB will ignore the given index name and instead generate a new name. Defaults
	 * to {@literal false}.
	 *
	 * @return {@literal false} by default.
	 */
	boolean useGeneratedName() default false;

	/**
	 * Only index the documents in a collection that meet a specified {@link IndexFilter filter expression}. <br />
	 *
	 * @return empty by default.
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/core/index-partial/">https://docs.mongodb.com/manual/core/index-partial/</a>
	 */
	String partialFilter() default "";

	/**
	 * Explicitly specify sub fields to be in-/excluded as a {@link org.bson.Document#parse(String) prasable} String.
	 * <br />
	 * <strong>NOTE:</strong> Can only be applied on root level documents.
	 *
	 * @return empty by default.
	 */
	String wildcardProjection() default "";

	/**
	 * Defines the collation to apply.
	 *
	 * @return an empty {@link String} by default.
	 */
	@AliasFor(annotation = Collation.class, attribute = "value")
	String collation() default "";
}
