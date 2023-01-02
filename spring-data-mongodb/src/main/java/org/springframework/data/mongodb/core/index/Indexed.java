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
package org.springframework.data.mongodb.core.index;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mongodb.core.annotation.Collation;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Mark a field to be indexed using MongoDB's indexing feature.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Johno Crawford
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Jordi Llach
 * @author Mark Paluch
 * @author Stefan Tirea
 */
@Collation
@Target({ ElementType.ANNOTATION_TYPE, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexed {

	/**
	 * If set to true reject all documents that contain a duplicate value for the indexed field.
	 *
	 * @return {@literal false} by default.
	 * @see <a href=
	 *      "https://docs.mongodb.org/manual/core/index-unique/">https://docs.mongodb.org/manual/core/index-unique/</a>
	 */
	boolean unique() default false;

	/**
	 * The index sort direction.
	 *
	 * @return {@link IndexDirection#ASCENDING} by default.
	 */
	IndexDirection direction() default IndexDirection.ASCENDING;

	/**
	 * If set to true index will skip over any document that is missing the indexed field. <br />
	 * Must not be used with {@link #partialFilter()}.
	 *
	 * @return {@literal false} by default.
	 * @see <a href=
	 *      "https://docs.mongodb.org/manual/core/index-sparse/">https://docs.mongodb.org/manual/core/index-sparse/</a>
	 */
	boolean sparse() default false;

	/**
	 * Index name either as plain value or as {@link org.springframework.expression.spel.standard.SpelExpression template
	 * expression}. <br />
	 * <br />
	 * The name will only be applied as is when defined on root level. For usage on nested or embedded structures the
	 * provided name will be prefixed with the path leading to the entity. <br />
	 * <br />
	 * The structure below
	 *
	 * <pre>
	 * <code>
	 * &#64;Document
	 * class Root {
	 *   Hybrid hybrid;
	 *   Nested nested;
	 * }
	 *
	 * &#64;Document
	 * class Hybrid {
	 *   &#64;Indexed(name="index") String h1;
	 *   &#64;Indexed(name="#{&#64;myBean.indexName}") String h2;
	 * }
	 *
	 * class Nested {
	 *   &#64;Indexed(name="index") String n1;
	 * }
	 * </code>
	 * </pre>
	 *
	 * resolves in the following index structures
	 *
	 * <pre>
	 * <code>
	 * db.root.createIndex( { hybrid.h1: 1 } , { name: "hybrid.index" } )
	 * db.root.createIndex( { nested.n1: 1 } , { name: "nested.index" } )
	 * db.hybrid.createIndex( { h1: 1} , { name: "index" } )
	 * db.hybrid.createIndex( { h2: 1} , { name: the value myBean.getIndexName() returned } )
	 * </code>
	 * </pre>
	 *
	 * @return empty String by default.
	 */
	String name() default "";

	/**
	 * If set to {@literal true} then MongoDB will ignore the given index name and instead generate a new name. Defaults
	 * to {@literal false}.
	 *
	 * @return {@literal false} by default.
	 * @since 1.5
	 */
	boolean useGeneratedName() default false;

	/**
	 * If {@literal true} the index will be created in the background.
	 *
	 * @return {@literal false} by default.
	 * @see <a href=
	 *      "https://docs.mongodb.org/manual/core/indexes/#background-construction">https://docs.mongodb.org/manual/core/indexes/#background-construction</a>
	 */
	boolean background() default false;

	/**
	 * Configures the number of seconds after which the collection should expire. Defaults to -1 for no expiry.
	 *
	 * @return {@literal -1} by default.
	 * @see <a href=
	 *      "https://docs.mongodb.org/manual/tutorial/expire-data/">https://docs.mongodb.org/manual/tutorial/expire-data/</a>
	 */
	int expireAfterSeconds() default -1;

	/**
	 * Alternative for {@link #expireAfterSeconds()} to configure the timeout after which the document should expire.
	 * Defaults to an empty {@link String} for no expiry. Accepts numeric values followed by their unit of measure:
	 * <ul>
	 * <li><b>d</b>: Days</li>
	 * <li><b>h</b>: Hours</li>
	 * <li><b>m</b>: Minutes</li>
	 * <li><b>s</b>: Seconds</li>
	 * <li>Alternatively: A Spring {@literal template expression}. The expression can result in a
	 * {@link java.time.Duration} or a valid expiration {@link String} according to the already mentioned
	 * conventions.</li>
	 * </ul>
	 * Supports ISO-8601 style.
	 *
	 * <pre class="code">
	 *
	 * &#0064;Indexed(expireAfter = "10s") String expireAfterTenSeconds;
	 *
	 * &#0064;Indexed(expireAfter = "1d") String expireAfterOneDay;
	 *
	 * &#0064;Indexed(expireAfter = "P2D") String expireAfterTwoDays;
	 *
	 * &#0064;Indexed(expireAfter = "#{&#0064;mySpringBean.timeout}") String expireAfterTimeoutObtainedFromSpringBean;
	 * </pre>
	 *
	 * @return empty by default.
	 * @since 2.2
	 */
	String expireAfter() default "";

	/**
	 * Only index the documents in a collection that meet a specified {@link IndexFilter filter expression}. <br />
	 * Must not be used with {@link #sparse() sparse = true}.
	 *
	 * @return empty by default.
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/core/index-partial/">https://docs.mongodb.com/manual/core/index-partial/</a>
	 * @since 3.1
	 */
	String partialFilter() default "";

	/**
	 * The actual collation definition in JSON format or a
	 * {@link org.springframework.expression.spel.standard.SpelExpression template expression} resolving to either a JSON
	 * String or a {@link org.bson.Document}. The keys of the JSON document are configuration options for the collation
	 * (language-specific rules for string comparison) applied to the indexed based on the field value.
	 * <p>
	 * <strong>NOTE:</strong> Overrides {@link Document#collation()}.
	 *
	 * @return empty by default.
	 * @see <a href="https://www.mongodb.com/docs/manual/reference/collation/">https://www.mongodb.com/docs/manual/reference/collation/</a>
	 * @since 4.0
	 */
	@AliasFor(annotation = Collation.class, attribute = "value")
	String collation() default "";
}
