/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.Reference;

/**
 * A {@link DocumentReference} offers an alternative way of linking entities in MongoDB. While the goal is the same as
 * when using {@link DBRef}, the store representation is different and can be literally anything, a single value, an
 * entire {@link org.bson.Document}, basically everything that can be stored in MongoDB. By default, the mapping layer
 * will use the referenced entities {@literal id} value for storage and retrieval.
 * 
 * <pre class="code">
 * public class Account {
 *   private String id;
 *   private Float total;
 * }
 *
 * public class Person {
 *   private String id;
 *   &#64;DocumentReference
 *   private List&lt;Account&gt; accounts;
 * }
 * 
 * Account account = ...
 *
 * mongoTemplate.insert(account);
 *
 * template.update(Person.class)
 *   .matching(where("id").is(...))
 *   .apply(new Update().push("accounts").value(account))
 *   .first();
 * </pre>
 * 
 * {@link #lookup()} allows to define custom queries that are independent from the {@literal id} field and in
 * combination with {@link org.springframework.data.convert.WritingConverter writing converters} offer a flexible way of
 * defining links between entities.
 * 
 * <pre class="code">
 * public class Book {
 * 	 private ObjectId id;
 * 	 private String title;
 *
 * 	 &#64;Field("publisher_ac")
 * 	 &#64;DocumentReference(lookup = "{ 'acronym' : ?#{#target} }")
 * 	 private Publisher publisher;
 * }
 *
 * public class Publisher {
 *
 * 	 private ObjectId id;
 * 	 private String acronym;
 * 	 private String name;
 *
 * 	 &#64;DocumentReference(lazy = true)
 * 	 private List&lt;Book&gt; books;
 * }
 *
 * &#64;WritingConverter
 * public class PublisherReferenceConverter implements Converter&lt;Publisher, DocumentPointer&lt;String&gt;&gt; {
 *
 *    public DocumentPointer&lt;String&gt; convert(Publisher source) {
 * 		return () -> source.getAcronym();
 *    }
 * }
 * </pre>
 *
 * @author Christoph Strobl
 * @since 3.3
 * @see <a href="https://docs.mongodb.com/manual/reference/database-references/#std-label-document-references">MongoDB Reference Documentation</a>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Reference
public @interface DocumentReference {

	/**
	 * The database the linked entity resides in.
	 *
	 * @return empty String by default. Uses the default database provided buy the {@link org.springframework.data.mongodb.MongoDatabaseFactory}.
	 */
	String db() default "";

	/**
	 * The database the linked entity resides in.
	 *
	 * @return empty String by default. Uses the property type for collection resolution.
	 */
	String collection() default "";

	/**
	 * The single document lookup query. In case of an {@link java.util.Collection} or {@link java.util.Map} property
	 * the individual lookups are combined via an `$or` operator.
	 *
	 * @return an {@literal _id} based lookup.
	 */
	String lookup() default "{ '_id' : ?#{#target} }";

	/**
	 * A specific sort.
	 *
	 * @return empty String by default.
	 */
	String sort() default "";

	/**
	 * Controls whether the referenced entity should be loaded lazily. This defaults to {@literal false}.
	 *
	 * @return {@literal false} by default.
	 */
	boolean lazy() default false;
}
