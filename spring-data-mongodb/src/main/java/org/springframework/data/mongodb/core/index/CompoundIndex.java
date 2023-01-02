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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mongodb.core.annotation.Collation;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Mark a class to use compound indexes. <br />
 * <p>
 * <b>NOTE: This annotation is repeatable according to Java 8 conventions using {@link CompoundIndexes#value()} as
 * container.</b>
 *
 * <pre class="code">
 * &#64;Document
 * &#64;CompoundIndex(def = "{'firstname': 1, 'lastname': 1}")
 * &#64;CompoundIndex(def = "{'address.city': 1, 'address.street': 1}")
 * class Person {
 * 	String firstname;
 * 	String lastname;
 *
 * 	Address address;
 * }
 * </pre>
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Johno Crawford
 * @author Christoph Strobl
 * @author Dave Perryman
 * @author Stefan Tirea
 */
@Collation
@Target({ ElementType.TYPE })
@Documented
@Repeatable(CompoundIndexes.class)
@Retention(RetentionPolicy.RUNTIME)
public @interface CompoundIndex {

	/**
	 * The actual index definition in JSON format or a {@link org.springframework.expression.spel.standard.SpelExpression
	 * template expression} resolving to either a JSON String or a {@link org.bson.Document}. The keys of the JSON
	 * document are the fields to be indexed, the values define the index direction (1 for ascending, -1 for descending).
	 * <br />
	 * If left empty on nested document, the whole document will be indexed.
	 *
	 * <pre class="code">
	 * &#64;Document
	 * &#64;CompoundIndex(def = "{'h1': 1, 'h2': 1}")
	 * class JsonStringIndexDefinition {
	 *   String h1, h2;
	 * }
	 *
	 * &#64;Document
	 * &#64;CompoundIndex(def = "#{T(org.bson.Document).parse("{ 'h1': 1, 'h2': 1 }")}")
	 * class ExpressionIndexDefinition {
	 *   String h1, h2;
	 * }
	 * </pre>
	 *
	 * @return empty String by default.
	 */
	String def() default "";

	/**
	 * @return {@literal false} by default.
	 * @see <a href=
	 *      "https://docs.mongodb.org/manual/core/index-unique/">https://docs.mongodb.org/manual/core/index-unique/</a>
	 */
	boolean unique() default false;

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
	 * Index name of the index to be created either as plain value or as
	 * {@link org.springframework.expression.spel.standard.SpelExpression template expression}. <br />
	 * <br />
	 * The name will only be applied as is when defined on root level. For usage on nested or embedded structures the
	 * provided name will be prefixed with the path leading to the entity. <br />
	 * <br />
	 * The structure below
	 *
	 * <pre class="code">
	 * &#64;Document
	 * class Root {
	 * 	Hybrid hybrid;
	 * 	Nested nested;
	 * }
	 *
	 * &#64;Document
	 * &#64;CompoundIndex(name = "compound_index", def = "{'h1': 1, 'h2': 1}")
	 * class Hybrid {
	 * 	String h1, h2;
	 * }
	 *
	 * &#64;CompoundIndex(name = "compound_index", def = "{'n1': 1, 'n2': 1}")
	 * class Nested {
	 * 	String n1, n2;
	 * }
	 * </pre>
	 *
	 * resolves in the following index structures
	 *
	 * <pre class="code">
	 * db.root.createIndex( { hybrid.h1: 1, hybrid.h2: 1 } , { name: "hybrid.compound_index" } )
	 * db.root.createIndex( { nested.n1: 1, nested.n2: 1 } , { name: "nested.compound_index" } )
	 * db.hybrid.createIndex( { h1: 1, h2: 1 } , { name: "compound_index" } )
	 * </pre>
	 *
	 * @return empty String by default.
	 */
	String name() default "";

	/**
	 * If set to {@literal true} then MongoDB will ignore the given index name and instead generate a new name. Defaults
	 * to {@literal false}.
	 *
	 * @return {@literal false} by default
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
	 * (language-specific rules for string comparison) to be applied on string properties being part of the index.
	 * <p>
	 * <strong>NOTE:</strong> Overrides {@link Document#collation()}.
	 *
	 * @return empty String by default.
	 * @see <a href=
	 *      "https://www.mongodb.com/docs/manual/reference/collation/">https://www.mongodb.com/docs/manual/reference/collation/</a>
	 * @since 4.0
	 */
	@AliasFor(annotation = Collation.class, attribute = "value")
	String collation() default "";
}
