/*
 * Copyright 2010-2016 the original author or authors.
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

import org.bson.Document;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Value object to capture data to create a geo index.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Laurent Canet
 * @author Christoph Strobl
 */
public class GeospatialIndex implements IndexDefinition {

	private final String field;
	private String name;
	private Integer min;
	private Integer max;
	private Integer bits;
	private GeoSpatialIndexType type = GeoSpatialIndexType.GEO_2D;
	private Double bucketSize = 1.0;
	private String additionalField;
	private IndexFilter filter;

	/**
	 * Creates a new {@link GeospatialIndex} for the given field.
	 * 
	 * @param field must not be empty or {@literal null}.
	 */
	public GeospatialIndex(String field) {

		Assert.hasText(field, "Field must have text!");

		this.field = field;
	}

	/**
	 * @param name must not be {@literal null} or empty.
	 * @return
	 */
	public GeospatialIndex named(String name) {

		this.name = name;
		return this;
	}

	/**
	 * @param min
	 * @return
	 */
	public GeospatialIndex withMin(int min) {
		this.min = Integer.valueOf(min);
		return this;
	}

	/**
	 * @param max
	 * @return
	 */
	public GeospatialIndex withMax(int max) {
		this.max = Integer.valueOf(max);
		return this;
	}

	/**
	 * @param bits
	 * @return
	 */
	public GeospatialIndex withBits(int bits) {
		this.bits = Integer.valueOf(bits);
		return this;
	}

	/**
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public GeospatialIndex typed(GeoSpatialIndexType type) {

		Assert.notNull(type, "Type must not be null!");

		this.type = type;
		return this;
	}

	/**
	 * @param bucketSize
	 * @return
	 */
	public GeospatialIndex withBucketSize(double bucketSize) {
		this.bucketSize = bucketSize;
		return this;
	}

	/**
	 * @param fieldName.
	 * @return
	 */
	public GeospatialIndex withAdditionalField(String fieldName) {
		this.additionalField = fieldName;
		return this;
	}

	/**
	 * Only index the documents in a collection that meet a specified {@link IndexFilter filter expression}.
	 *
	 * @param filter can be {@literal null}.
	 * @return
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/core/index-partial/">https://docs.mongodb.com/manual/core/index-partial/</a>
	 * @since 1.10
	 */
	public GeospatialIndex partial(IndexFilter filter) {

		this.filter = filter;
		return this;
	}

	public Document getIndexKeys() {

		Document document = new Document();

		switch (type) {

			case GEO_2D:
				document.put(field, "2d");
				break;

			case GEO_2DSPHERE:
				document.put(field, "2dsphere");
				break;

			case GEO_HAYSTACK:
				document.put(field, "geoHaystack");
				if (!StringUtils.hasText(additionalField)) {
					throw new IllegalArgumentException("When defining geoHaystack index, an additionnal field must be defined");
				}
				document.put(additionalField, 1);
				break;

			default:
				throw new IllegalArgumentException("Unsupported geospatial index " + type);
		}

		return document;
	}

	public Document getIndexOptions() {

		if (!StringUtils.hasText(name) && min == null && max == null && bucketSize == null) {
			return null;
		}

		Document dbo = new Document();
		if (StringUtils.hasText(name)) {
			dbo.put("name", name);
		}

		switch (type) {

			case GEO_2D:

				if (min != null) {
					dbo.put("min", min);
				}
				if (max != null) {
					dbo.put("max", max);
				}
				if (bits != null) {
					dbo.put("bits", bits);
				}
				break;

			case GEO_2DSPHERE:

				break;

			case GEO_HAYSTACK:

				if (bucketSize != null) {
					dbo.put("bucketSize", bucketSize);
				}
				break;
		}

		if (filter != null) {
			dbo.put("partialFilterExpression", filter.getFilterObject());
		}

		return dbo;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Geo index: %s - Options: %s", getIndexKeys(), getIndexOptions());
	}
}
