/*
 * Copyright 2010-2015 the original author or authors.
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
 * Mark a field to be indexed using MongoDB's geospatial indexing feature.
 * 
 * @author Jon Brisbin
 * @author Laurent Canet
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GeoSpatialIndexed {

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
	 *   &#64;GeoSpatialIndexed(name="index") Point h1;
	 * }
	 * 
	 * class Nested {
	 *   &#64;GeoSpatialIndexed(name="index") Point n1;
	 * }
	 * </code>
	 * </pre>
	 * 
	 * resolves in the following index structures
	 * 
	 * <pre>
	 * <code>
	 * db.root.createIndex( { hybrid.h1: "2d" } , { name: "hybrid.index" } )
	 * db.root.createIndex( { nested.n1: "2d" } , { name: "nested.index" } )
	 * db.hybrid.createIndex( { h1: "2d" } , { name: "index" } )
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
	 * Name of the collection in which to create the index.
	 * 
	 * @return
	 * @deprecated The collection name is derived from the domain type. Fixing the collection via this attribute might
	 *             result in broken definitions. Will be removed in 1.7.
	 */
	@Deprecated
	String collection() default "";

	/**
	 * Minimum value for indexed values.
	 * 
	 * @return
	 */
	int min() default -180;

	/**
	 * Maximum value for indexed values.
	 * 
	 * @return
	 */
	int max() default 180;

	/**
	 * Bits of precision for boundary calculations.
	 * 
	 * @return
	 */
	int bits() default 26;

	/**
	 * The type of the geospatial index. Default is {@link GeoSpatialIndexType#GEO_2D}
	 * 
	 * @since 1.4
	 * @return
	 */
	GeoSpatialIndexType type() default GeoSpatialIndexType.GEO_2D;

	/**
	 * The bucket size for {@link GeoSpatialIndexType#GEO_HAYSTACK} indexes, in coordinate units.
	 * 
	 * @since 1.4
	 * @return
	 */
	double bucketSize() default 1.0;

	/**
	 * The name of the additional field to use for {@link GeoSpatialIndexType#GEO_HAYSTACK} indexes
	 * 
	 * @since 1.4
	 * @return
	 */
	String additionalField() default "";
}
