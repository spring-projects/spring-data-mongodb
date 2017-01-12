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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark a class to use compound indexes.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Johno Crawford
 * @author Christoph Strobl
 */
@Target({ ElementType.TYPE })
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface CompoundIndex {

	/**
	 * The actual index definition in JSON format. The keys of the JSON document are the fields to be indexed, the values
	 * define the index direction (1 for ascending, -1 for descending). <br />
	 * If left empty on nested document, the whole document will be indexed.
	 * 
	 * @return
	 */
	String def() default "";

	/**
	 * It does not actually make sense to use that attribute as the direction has to be defined in the {@link #def()}
	 * attribute actually.
	 * 
	 * @return
	 */
	@Deprecated
	IndexDirection direction() default IndexDirection.ASCENDING;

	/**
	 * @return
	 * @see <a href="https://docs.mongodb.org/manual/core/index-unique/">https://docs.mongodb.org/manual/core/index-unique/</a>
	 */
	boolean unique() default false;

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
	 * The name of the index to be created. <br />
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
	 * &#64;CompoundIndex(name = "compound_index", def = "{'h1': 1, 'h2': 1}")
	 * class Hybrid {
	 *   String h1, h2;
	 * }
	 * 
	 * &#64;CompoundIndex(name = "compound_index", def = "{'n1': 1, 'n2': 1}")
	 * class Nested {
	 *   String n1, n2;
	 * }
	 * </code>
	 * </pre>
	 * 
	 * resolves in the following index structures
	 * 
	 * <pre>
	 * <code>
	 * db.root.createIndex( { hybrid.h1: 1, hybrid.h2: 1 } , { name: "hybrid.compound_index" } )
	 * db.root.createIndex( { nested.n1: 1, nested.n2: 1 } , { name: "nested.compound_index" } )
	 * db.hybrid.createIndex( { h1: 1, h2: 1 } , { name: "compound_index" } )
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
	 * The collection the index will be created in. Will default to the collection the annotated domain class will be
	 * stored in.
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

}
