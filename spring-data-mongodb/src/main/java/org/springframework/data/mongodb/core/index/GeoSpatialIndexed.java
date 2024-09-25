/*
 * Copyright 2010-2024 the original author or authors.
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
 * Mark a field to be indexed using MongoDB's geospatial indexing feature.
 *
 * @author Jon Brisbin
 * @author Laurent Canet
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface GeoSpatialIndexed {

	/**
	 * Index name either as plain value or as {@link org.springframework.expression.spel.standard.SpelExpression template
	 * expression}. <br />
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
	 *   &#64;GeoSpatialIndexed(name="#{&#64;myBean.indexName}") Point h2;
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
	 * db.hybrid.createIndex( { h2: "2d"} , { name: the value myBean.getIndexName() returned } )
	 * </code>
	 * </pre>
	 *
	 * @return empty {@link String} by default.
	 */
	String name() default "";

	/**
	 * If set to {@literal true} then MongoDB will ignore the given index name and instead generate a new name. Defaults
	 * to {@literal false}.
	 *
	 * @return {@literal false} by default.
	 * @since 1.5
	 */
	boolean useGeneratedName() default false;

	/**
	 * Minimum value for indexed values.
	 *
	 * @return {@literal -180} by default.
	 */
	int min() default -180;

	/**
	 * Maximum value for indexed values.
	 *
	 * @return {@literal +180} by default.
	 */
	int max() default 180;

	/**
	 * Bits of precision for boundary calculations.
	 *
	 * @return {@literal 26} by default.
	 */
	int bits() default 26;

	/**
	 * The type of the geospatial index. Default is {@link GeoSpatialIndexType#GEO_2D}
	 *
	 * @since 1.4
	 * @return {@link GeoSpatialIndexType#GEO_2D} by default.
	 */
	GeoSpatialIndexType type() default GeoSpatialIndexType.GEO_2D;

	/**
	 * The bucket size for {@link GeoSpatialIndexType#GEO_HAYSTACK} indexes, in coordinate units.
	 *
	 * @since 1.4
	 * @return {@literal 1.0} by default.
	 * @deprecated since MongoDB server version 4.4
	 */
	@Deprecated
	double bucketSize() default 1.0;

	/**
	 * The name of the additional field to use for {@link GeoSpatialIndexType#GEO_HAYSTACK} indexes
	 *
	 * @since 1.4
	 * @return empty {@link String} by default.
	 */
	String additionalField() default "";
}
