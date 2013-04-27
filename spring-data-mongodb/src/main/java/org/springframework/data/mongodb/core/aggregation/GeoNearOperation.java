/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $geoNear} operation.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/geoNear/
 * @author Sebastian Herold
 * @since 1.3
 */
public class GeoNearOperation implements AggregationOperation {

	private final double[] near;
	private final String distanceField;

	private final Map<String, Object> options = new LinkedHashMap<String, Object>();

	public GeoNearOperation(double x, double y, String distanceField) {
		Assert.hasText(distanceField, "Distance field is empty.");

		this.near = new double[] { x, y };
		this.distanceField = distanceField;
	}

	public DBObject getDBObject() {
		BasicDBObject geoNear = new BasicDBObject();
		geoNear.put("near", near);
		geoNear.put("distanceField", distanceField);
		geoNear.putAll(options);
		return new BasicDBObject("$geoNear", geoNear);
	}

	/**
	 * Specifies the maximum number of documents to return. The default value is 100. See also the {@link #num(long)}
	 * option.
	 * 
	 * @param value limit
	 * @return this operation
	 */
	public GeoNearOperation limit(long value) {
		options.put("limit", value);
		return this;
	}

	/**
	 * Synonym for the {@link #limit(long)} option. If both {@link #num(long)} and {@link #limit(long)} are included, the
	 * {@link #num(long)} value overrides the limit value.
	 * 
	 * @param value limit
	 * @return this operation
	 */
	public GeoNearOperation num(long value) {
		options.put("num", value);
		return this;
	}

	/**
	 * Limits the results to the documents within the specified distance from the center coordinates.
	 * 
	 * @param distance distance
	 * @return this operation
	 */
	public GeoNearOperation maxDistance(double distance) {
		options.put("maxDistance", distance);
		return this;
	}

	/**
	 * Limits the results to the documents that match the query. The query syntax is identical to the read operation query
	 * syntax.
	 * 
	 * @param criteria criteria
	 * @return this operation
	 */
	public GeoNearOperation query(Criteria criteria) {
		options.put("query", criteria.getCriteriaObject());
		return this;
	}

	/**
	 * Default value is <code>false</code>. When <code>true</code>, MongoDB performs calculation using spherical geometry.
	 * 
	 * @param spherical <code>true</code> or <code>false</code>
	 * @return this operation
	 */
	public GeoNearOperation spherical(boolean spherical) {
		options.put("spherical", spherical);
		return this;
	}

	/**
	 * Specifies a factor to multiply all distances returned by <code>$geoNear<code>. For example, 
	 * use distanceMultiplier to convert from spherical queries returned in radians to linear units 
	 * (i.e. miles or kilometers) by multiplying by the radius of the Earth.
	 * 
	 * @param distanceMultiplier distance multiplier
	 * @return this operation
	 */
	public GeoNearOperation distanceMultiplier(double distanceMultiplier) {
		options.put("distanceMultiplier", distanceMultiplier);
		return this;
	}

	/**
	 * Specifies the output field that identifies the location used to calculate the distance. This option is useful when
	 * a location field contains multiple locations. You can use the dot notation to specify a field within a subdocument.
	 * 
	 * @param includeLocs include locations
	 * @return this operation
	 */
	public GeoNearOperation includeLocs(String includeLocs) {
		Assert.hasText(includeLocs, "Include locations are empty.");

		options.put("includeLocs", includeLocs);
		return this;
	}

	/**
	 * Default value is <code>false</code>. If a location field contains multiple locations, the default settings will
	 * return the document multiple times if more than one location meets the criteria. When <code>true</code>, the
	 * document will only return once even if the document has multiple locations that meet the criteria.
	 * 
	 * @param uniqueDocs unique documents <code>true</code> or <code>false</code>
	 * @return this operation
	 */
	public GeoNearOperation uniqueDocs(boolean uniqueDocs) {
		options.put("uniqueDocs", uniqueDocs);
		return this;
	}

	/**
	 * Creates a {@code $geoNear} operation as described <a
	 * href="http://docs.mongodb.org/manual/reference/aggregation/geoNear/">here</a>.
	 * 
	 * @param x Specifies the x coordinate to use as the center of a geospatial query.
	 * @param y Specifies the y coordinate to use as the center of a geospatial query.
	 * @param distanceField Specifies the output field that will contain the calculated distance. You can use the dot
	 *          notation to specify a field within a subdocument.
	 * @return the geoNear operation
	 * 
	 * @see http://docs.mongodb.org/manual/reference/aggregation/geoNear/
	 */
	public static GeoNearOperation geoNear(double x, double y, String distanceField) {
		return new GeoNearOperation(x, y, distanceField);
	}
}
