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

import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark a class to use compound wildcard indexes. <br />
 *
 * <pre class="code">
 * &#64;Document
 * &#64;CompoundWildcardIndexed(wildcardFieldName = "address", fields = "{'firstname': 1}")
 * class Person {
 * 	String firstname;
 * 	Address address;
 * }
 *
 * db.product.createIndex({"address.$**": 1, "firstname": 1})
 * </pre>
 *
 * {@literal wildcardProjection} can be used to specify keys to in-/exclude in the index.
 *
 * <pre class="code">
 *
 * &#64;Document
 * &#64;CompoundWildcardIndexed(wildcardProjection = "{'address.zip': 0}", fields = "{'firstname': 1}")
 * class Person {
 * 	String firstname;
 * 	Address address;
 * }
 *
 * db.user.createIndex({"$**": 1, "firstname": 1}, {"wildcardProjection": {"address.zip": 0}})
 * </pre>
 *
 * @author Julia Lee
 */
@Target({ ElementType.TYPE })
@Documented
@WildcardIndexed
@CompoundIndex
@Retention(RetentionPolicy.RUNTIME)
public @interface CompoundWildcardIndexed {

	/**
	 * The name of the sub-field to which a wildcard index is applied. If empty, the wildcard term will resolve to "$**".
	 *
	 * @return empty by default.
	 */
	String wildcardFieldName() default "";

	/**
	 * Explicitly specify sub-fields to be in-/excluded as a {@link org.bson.Document#parse(String) parsable} String.
	 * <br />
	 * <strong>NOTE:</strong> Can only be applied on when wildcard term is "$**"
	 *
	 * @return empty by default.
	 */
	@AliasFor(annotation = WildcardIndexed.class, attribute = "wildcardProjection")
	String wildcardProjection() default "";

	/**
	 * Definition of non-wildcard index(es) in JSON format, wherein the keys are the fields to be indexed and the values
	 * define the index direction (1 for ascending, -1 for descending). <br />
	 *
	 * <pre class="code">
   * &#64;Document
   * &#64;CompoundWildcardIndexed(wildcardProjection = "{ 'address.zip' : 0 }", fields = "{'firstname': 1}")
   * class Person {
   * 	String firstname;
   * 	Address address;
   * }
	 * </pre>
	 *
	 * @return empty String by default.
	 */
	@AliasFor(annotation = CompoundIndex.class, attribute = "def")
	String fields();

	/**
	 * Index name either as plain value or as {@link org.springframework.expression.spel.standard.SpelExpression template
	 * expression}. <br />
	 *
	 * @return empty by default.
	 */
	@AliasFor(annotation = WildcardIndexed.class, attribute = "name")
	String name() default "";

	/**
	 * If set to {@literal true} then MongoDB will ignore the given index name and instead generate a new name. Defaults
	 * to {@literal false}.
	 *
	 * @return {@literal false} by default
	 */
	@AliasFor(annotation = WildcardIndexed.class, attribute = "useGeneratedName")
	boolean useGeneratedName() default false;

	/**
	 * Only index the documents in a collection that meet a specified {@link IndexFilter filter expression}. <br />
	 *
	 * @return empty by default.
	 */
	@AliasFor(annotation = WildcardIndexed.class, attribute = "partialFilter")
	String partialFilter() default "";

	/**
	 * Defines the collation to apply.
	 *
	 * @return an empty {@link String} by default.
	 */
	@AliasFor(annotation = WildcardIndexed.class, attribute = "collation")
	String collation() default "";
}
