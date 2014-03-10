/*
 * Copyright 2010-2014 the original author or authors.
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
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GeoSpatialIndexed {

	/**
	 * Name of the property in the document that contains the [x, y] or radial coordinates to index.
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
	 * Name of the collection in which to create the index.
	 * 
	 * @return
	 */
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
