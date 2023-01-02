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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.Reference;
import org.springframework.data.mongodb.MongoDatabaseFactory;

/**
 * A {@link DocumentReference} allows referencing entities in MongoDB using a flexible schema. While the goal is the
 * same as when using {@link DBRef}, the store representation is different. The reference can be anything, a single
 * value, an entire {@link org.bson.Document}, basically everything that can be stored in MongoDB. By default, the
 * mapping layer will use the referenced entities {@literal id} value for storage and retrieval.
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
 * {@link #lookup()} allows defining a query filter that is independent from the {@literal _id} field and in combination
 * with {@link org.springframework.data.convert.WritingConverter writing converters} offers a flexible way of defining
 * references between entities.
 *
 * <pre class="code">
 * public class Book {
 * 	private ObjectId id;
 * 	private String title;
 *
 * 	&#64;Field("publisher_ac") &#64;DocumentReference(lookup = "{ 'acronym' : ?#{#target} }") private Publisher publisher;
 * }
 *
 * public class Publisher {
 *
 * 	private ObjectId id;
 * 	private String acronym;
 * 	private String name;
 *
 * 	&#64;DocumentReference(lazy = true) private List&lt;Book&gt; books;
 * }
 *
 * &#64;WritingConverter
 * public class PublisherReferenceConverter implements Converter&lt;Publisher, DocumentPointer&lt;String&gt;&gt; {
 *
 * 	public DocumentPointer&lt;String&gt; convert(Publisher source) {
 * 		return () -> source.getAcronym();
 * 	}
 * }
 * </pre>
 *
 * @author Christoph Strobl
 * @since 3.3
 * @see <a href="https://docs.mongodb.com/manual/reference/database-references/#std-label-document-references">MongoDB
 *      Reference Documentation</a>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Reference
public @interface DocumentReference {

	/**
	 * The database the referenced entity resides in. Uses the default database provided by
	 * {@link org.springframework.data.mongodb.MongoDatabaseFactory} if empty.
	 *
	 * @see MongoDatabaseFactory#getMongoDatabase()
	 * @see MongoDatabaseFactory#getMongoDatabase(String)
	 */
	String db() default "";

	/**
	 * The collection the referenced entity resides in. Defaults to the collection of the referenced entity type.
	 *
	 * @see MongoPersistentEntity#getCollection()
	 */
	String collection() default "";

	/**
	 * The single document lookup query. In case of an {@link java.util.Collection} or {@link java.util.Map} property the
	 * individual lookups are combined via an {@code $or} operator. {@code target} points to the source value (or
	 * document) stored at the reference property. Properties of {@code target} can be used to define the reference query.
	 *
	 * @return an {@literal _id} based lookup.
	 */
	String lookup() default "{ '_id' : ?#{#target} }";

	/**
	 * A specific sort.
	 */
	String sort() default "";

	/**
	 * Controls whether the referenced entity should be loaded lazily. This defaults to {@literal false}.
	 *
	 * @return {@literal false} by default.
	 */
	boolean lazy() default false;
}
