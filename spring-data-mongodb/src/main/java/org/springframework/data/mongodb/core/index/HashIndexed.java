/*
 * Copyright 2019-2023 the original author or authors.
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

/**
 * Annotation for a property that should be used as key for a
 * <a href="https://docs.mongodb.com/manual/core/index-hashed/">Hashed Index</a>. If used on a simple property, the
 * index uses a hashing function to compute the hash of the value of the index field. Added to a property of complex
 * type the embedded document is collapsed and the hash computed for the entire object.
 * <br />
 *
 * <pre class="code">
 * &#64;Document
 * public class DomainType {
 *
 * 	&#64;HashIndexed @Id String id;
 * }
 * </pre>
 *
 * {@link HashIndexed} can also be used as meta {@link java.lang.annotation.Annotation} to create composed annotations:
 *
 * <pre class="code">
 * &#64;Indexed
 * &#64;HashIndexed
 * &#64;Retention(RetentionPolicy.RUNTIME)
 * public @interface IndexAndHash {
 *
 * 	&#64;AliasFor(annotation = Indexed.class, attribute = "name")
 * 	String name() default "";
 * }
 *
 * &#64;Document
 * public class DomainType {
 *
 * 	&#64;ComposedHashIndexed(name = "idx-name") String value;
 * }
 * </pre>
 *
 * @author Christoph Strobl
 * @since 2.2
 * @see HashedIndex
 */
@Target({ ElementType.ANNOTATION_TYPE, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface HashIndexed {
}
