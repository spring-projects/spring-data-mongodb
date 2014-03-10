/*
 * Copyright 2011-2014 the original author or authors.
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
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexed {

	/**
	 * If set to true reject all documents that contain a duplicate value for the indexed field.
	 * 
	 * @see http://docs.mongodb.org/manual/core/index-unique/
	 * @return
	 */
	boolean unique() default false;

	IndexDirection direction() default IndexDirection.ASCENDING;

	/**
	 * If set to true index will skip over any document that is missing the indexed field.
	 * 
	 * @see http://docs.mongodb.org/manual/core/index-sparse/
	 * @return
	 */
	boolean sparse() default false;

	/**
	 * @see http://docs.mongodb.org/manual/core/index-creation/#index-creation-duplicate-dropping
	 * @return
	 */
	boolean dropDups() default false;

	/**
	 * Index name.
	 * 
	 * @return
	 */
	String name() default "";

	/**
	 * If {@literal true} then MongoDB will ignore the given index name and instead generate a new name. Defaults to
	 * {@literal false}.
	 * 
	 * @return
	 */
	boolean useGeneratedName() default false;

	/**
	 * Collection name for index to be created on.
	 * 
	 * @return
	 */
	String collection() default "";

	/**
	 * If {@literal true} the index will be created in the background.
	 * 
	 * @see http://docs.mongodb.org/manual/core/indexes/#background-construction
	 * @return
	 */
	boolean background() default false;

	/**
	 * Configures the number of seconds after which the collection should expire. Defaults to -1 for no expiry.
	 * 
	 * @see http://docs.mongodb.org/manual/tutorial/expire-data/
	 * @return
	 */
	int expireAfterSeconds() default -1;
}
