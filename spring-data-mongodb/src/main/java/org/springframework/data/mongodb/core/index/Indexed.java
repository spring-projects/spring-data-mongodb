/*
 * Copyright 2011-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * Mark a field to be indexed using MongoDB's indexing feature.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Johno Crawford
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Jordi Llach
 */
@Target({ElementType.ANNOTATION_TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexed {

	/**
	 * If set to true reject all documents that contain a duplicate value for the indexed field.
	 * 
	 * @return
	 * @see <a href="https://docs.mongodb.org/manual/core/index-unique/">https://docs.mongodb.org/manual/core/index-unique/</a>
	 */
	boolean unique() default false;

	IndexDirection direction() default IndexDirection.ASCENDING;

	/**
	 * If set to true index will skip over any document that is missing the indexed field.
	 * 
	 * @return
	 * @see <a href="https://docs.mongodb.org/manual/core/index-sparse/">https://docs.mongodb.org/manual/core/index-sparse/</a>
	 */
	boolean sparse() default false;

	/**
	 * @return
	 * @see <a href="https://docs.mongodb.org/manual/core/index-creation/#index-creation-duplicate-dropping">https://docs.mongodb.org/manual/core/index-creation/#index-creation-duplicate-dropping</a>
	 */
	boolean dropDups() default false;

	/**
	 * Index name. <br />
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
	 * </code>
	 * </pre>
	 * 
	 * @return
	 */
	String name() default "";

	/**
	 * If set to {@literal true} then MongoDB will ignore the given index name and instead generate a new name. Defaults
	 * to {@literal false}.
	 * 
	 * @return
	 * @since 1.5
	 */
	boolean useGeneratedName() default false;

	/**
	 * Collection name for index to be created on.
	 * 
	 * @return
	 * @deprecated The collection name is derived from the domain type. Fixing the collection via this attribute might
	 *             result in broken definitions. Will be removed in 1.7.
	 */
	@Deprecated
	String collection() default "";

	/**
	 * If {@literal true} the index will be created in the background.
	 * 
	 * @return
	 * @see <a href="https://docs.mongodb.org/manual/core/indexes/#background-construction">https://docs.mongodb.org/manual/core/indexes/#background-construction</a>
	 */
	boolean background() default false;

	/**
	 * Configures the number of seconds after which the collection should expire. Defaults to -1 for no expiry.
	 * 
	 * @return
	 * @see <a href="https://docs.mongodb.org/manual/tutorial/expire-data/">https://docs.mongodb.org/manual/tutorial/expire-data/</a>
	 */
	int expireAfterSeconds() default -1;



	boolean propagateUnique() default true;
}
