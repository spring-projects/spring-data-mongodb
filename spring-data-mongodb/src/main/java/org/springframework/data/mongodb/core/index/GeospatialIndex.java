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

import java.util.Optional;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Value object to capture data to create a geo index.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Laurent Canet
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class GeospatialIndex implements IndexDefinition {

	private final String field;
	private @Nullable String name;
	private @Nullable Integer min;
	private @Nullable Integer max;
	private @Nullable Integer bits;
	private GeoSpatialIndexType type = GeoSpatialIndexType.GEO_2D;
	private Double bucketSize = MongoClientVersion.isVersion5orNewer() ? null : 1.0;
	private @Nullable String additionalField;
	private Optional<IndexFilter> filter = Optional.empty();
	private Optional<Collation> collation = Optional.empty();

	/**
	 * Creates a new {@link GeospatialIndex} for the given field.
	 *
	 * @param field must not be empty or {@literal null}.
	 */
	public GeospatialIndex(String field) {

		Assert.hasText(field, "Field must have text");

		this.field = field;
	}

	/**
	 * @param name must not be {@literal null} or empty.
	 * @return this.
	 */
	public GeospatialIndex named(String name) {

		this.name = name;
		return this;
	}

	/**
	 * @param min
	 * @return this.
	 */
	public GeospatialIndex withMin(int min) {
		this.min = min;
		return this;
	}

	/**
	 * @param max
	 * @return this.
	 */
	public GeospatialIndex withMax(int max) {
		this.max = max;
		return this;
	}

	/**
	 * @param bits
	 * @return this.
	 */
	public GeospatialIndex withBits(int bits) {
		this.bits = bits;
		return this;
	}

	/**
	 * @param type must not be {@literal null}.
	 * @return this.
	 */
	public GeospatialIndex typed(GeoSpatialIndexType type) {

		Assert.notNull(type, "Type must not be null");

		this.type = type;
		return this;
	}

	/**
	 * @param bucketSize
	 * @return this.
	 * @deprecated since MongoDB server version 4.4
	 */
	@Deprecated
	public GeospatialIndex withBucketSize(double bucketSize) {
		this.bucketSize = bucketSize;
		return this;
	}

	/**
	 * @param fieldName
	 * @return this.
	 */
	public GeospatialIndex withAdditionalField(String fieldName) {
		this.additionalField = fieldName;
		return this;
	}

	/**
	 * Only index the documents in a collection that meet a specified {@link IndexFilter filter expression}.
	 *
	 * @param filter can be {@literal null}.
	 * @return this.
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/core/index-partial/">https://docs.mongodb.com/manual/core/index-partial/</a>
	 * @since 1.10
	 */
	public GeospatialIndex partial(@Nullable IndexFilter filter) {

		this.filter = Optional.ofNullable(filter);
		return this;
	}

	/**
	 * Set the {@link Collation} to specify language-specific rules for string comparison, such as rules for lettercase
	 * and accent marks.<br />
	 * <strong>NOTE:</strong> Only queries using the same {@link Collation} as the {@link Index} actually make use of the
	 * index.
	 *
	 * @param collation can be {@literal null}.
	 * @return this.
	 * @since 2.0
	 */
	public GeospatialIndex collation(@Nullable Collation collation) {

		this.collation = Optional.ofNullable(collation);
		return this;
	}

	@Override
	public Document getIndexKeys() {

		Document document = new Document();

		switch (type) {
			case GEO_2D -> document.put(field, "2d");
			case GEO_2DSPHERE -> document.put(field, "2dsphere");
			case GEO_HAYSTACK -> {
				document.put(field, "geoHaystack");
				if (!StringUtils.hasText(additionalField)) {
					throw new IllegalArgumentException("When defining geoHaystack index, an additional field must be defined");
				}
				document.put(additionalField, 1);
			}
			default -> throw new IllegalArgumentException("Unsupported geospatial index " + type);
		}

		return document;
	}

	@Override
	public Document getIndexOptions() {

		Document document = new Document();
		if (StringUtils.hasText(name)) {
			document.put("name", name);
		}

		switch (type) {

			case GEO_2D:

				if (min != null) {
					document.put("min", min);
				}
				if (max != null) {
					document.put("max", max);
				}
				if (bits != null) {
					document.put("bits", bits);
				}
				break;

			case GEO_2DSPHERE:

				break;

			case GEO_HAYSTACK:

				if (bucketSize != null) {
					document.put("bucketSize", bucketSize);
				}
				break;
		}

		filter.ifPresent(val -> document.put("partialFilterExpression", val.getFilterObject()));
		collation.ifPresent(val -> document.append("collation", val.toDocument()));

		return document;
	}

	@Override
	public String toString() {
		return String.format("Geo index: %s - Options: %s", getIndexKeys(), getIndexOptions());
	}
}
