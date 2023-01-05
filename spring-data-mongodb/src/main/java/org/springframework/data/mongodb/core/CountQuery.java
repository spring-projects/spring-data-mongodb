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
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.query.MetricConversion;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Value object representing a count query. Count queries using {@code $near} or {@code $nearSphere} require a rewrite
 * to {@code $geoWithin}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.0
 */
class CountQuery {

	private final Document source;

	private CountQuery(Document source) {
		this.source = source;
	}

	public static CountQuery of(Document source) {
		return new CountQuery(source);
	}

	/**
	 * Returns the query {@link Document} that can be used with {@code countDocuments()}. Potentially rewrites the query
	 * to be usable with {@code countDocuments()}.
	 *
	 * @return the query {@link Document} that can be used with {@code countDocuments()}.
	 */
	public Document toQueryDocument() {

		if (!requiresRewrite(source)) {
			return source;
		}

		Document target = new Document();

		for (Map.Entry<String, Object> entry : source.entrySet()) {

			if (entry.getValue() instanceof Document document && requiresRewrite(entry.getValue())) {

				target.putAll(createGeoWithin(entry.getKey(), document, source.get("$and")));
				continue;
			}

			if (entry.getValue() instanceof Collection<?> collection && requiresRewrite(entry.getValue())) {

				target.put(entry.getKey(), rewriteCollection(collection));
				continue;
			}

			if ("$and".equals(entry.getKey()) && target.containsKey("$and")) {
				// Expect $and to be processed with Document and createGeoWithin.
				continue;
			}

			target.put(entry.getKey(), entry.getValue());
		}

		return target;
	}

	/**
	 * @param valueToInspect
	 * @return {@code true} if the enclosing element needs to be rewritten.
	 */
	private boolean requiresRewrite(Object valueToInspect) {

		if (valueToInspect instanceof Document document) {
			return requiresRewrite(document);
		}

		if (valueToInspect instanceof Collection<?> collection) {
			return requiresRewrite(collection);
		}

		return false;
	}

	private boolean requiresRewrite(Collection<?> collection) {

		for (Object o : collection) {
			if (o instanceof Document document && requiresRewrite(document)) {
				return true;
			}
		}

		return false;
	}

	private boolean requiresRewrite(Document document) {

		if (containsNear(document)) {
			return true;
		}

		for (Object entry : document.values()) {

			if (requiresRewrite(entry)) {
				return true;
			}
		}

		return false;
	}

	private Collection<Object> rewriteCollection(Collection<?> source) {

		Collection<Object> rewrittenCollection = new ArrayList<>(source.size());

		for (Object item : source) {
			if (item instanceof Document document && requiresRewrite(item)) {
				rewrittenCollection.add(CountQuery.of(document).toQueryDocument());
			} else {
				rewrittenCollection.add(item);
			}
		}

		return rewrittenCollection;
	}

	/**
	 * Rewrite the near query for field {@code key} to {@code $geoWithin}.
	 *
	 * @param key the queried field.
	 * @param source source {@link Document}.
	 * @param $and potentially existing {@code $and} condition.
	 * @return the rewritten query {@link Document}.
	 */
	@SuppressWarnings("unchecked")
	private static Document createGeoWithin(String key, Document source, @Nullable Object $and) {

		boolean spheric = source.containsKey("$nearSphere");
		Object $near = spheric ? source.get("$nearSphere") : source.get("$near");

		Number maxDistance = getMaxDistance(source, $near, spheric);

		List<Object> $centerMax = Arrays.asList(toCenterCoordinates($near), maxDistance);
		Document $geoWithinMax = new Document("$geoWithin",
				new Document(spheric ? "$centerSphere" : "$center", $centerMax));

		if (!containsNearWithMinDistance(source)) {
			return new Document(key, $geoWithinMax);
		}

		Number minDistance = (Number) source.get("$minDistance");
		List<Object> $centerMin = Arrays.asList(toCenterCoordinates($near), minDistance);
		Document $geoWithinMin = new Document("$geoWithin",
				new Document(spheric ? "$centerSphere" : "$center", $centerMin));

		List<Document> criteria;

		if ($and != null) {
			if ($and instanceof Collection) {
				Collection<Document> andElements = (Collection<Document>) $and;
				criteria = new ArrayList<>(andElements.size() + 2);
				criteria.addAll(andElements);
			} else {
				throw new IllegalArgumentException(
						"Cannot rewrite query as it contains an '$and' element that is not a Collection: Offending element: "
								+ $and);
			}
		} else {
			criteria = new ArrayList<>(2);
		}

		criteria.add(new Document("$nor", Collections.singletonList(new Document(key, $geoWithinMin))));
		criteria.add(new Document(key, $geoWithinMax));

		return new Document("$and", criteria);
	}

	private static Number getMaxDistance(Document source, Object $near, boolean spheric) {

		Number maxDistance = Double.MAX_VALUE;

		if (source.containsKey("$maxDistance")) { // legacy coordinate pair
			return (Number) source.get("$maxDistance");
		}

		if ($near instanceof Document nearDoc) {

			if (nearDoc.containsKey("$maxDistance")) {

				maxDistance = (Number) nearDoc.get("$maxDistance");
				// geojson is in Meters but we need radians x/(6378.1*1000)
				if (spheric && nearDoc.containsKey("$geometry")) {
					maxDistance = MetricConversion.metersToRadians(maxDistance.doubleValue());
				}
			}
		}

		return maxDistance;
	}

	private static boolean containsNear(Document source) {
		return source.containsKey("$near") || source.containsKey("$nearSphere");
	}

	private static boolean containsNearWithMinDistance(Document source) {

		if (!containsNear(source)) {
			return false;
		}

		return source.containsKey("$minDistance");
	}

	private static Object toCenterCoordinates(Object value) {

		if (ObjectUtils.isArray(value)) {
			return value;
		}

		if (value instanceof Point point) {
			return Arrays.asList(point.getX(), point.getY());
		}

		if (value instanceof Document document) {

			if (document.containsKey("x")) {
				return Arrays.asList(document.get("x"), document.get("y"));
			}

			if (document.containsKey("$geometry")) {
				Document geoJsonPoint = document.get("$geometry", Document.class);
				return geoJsonPoint.get("coordinates");
			}
		}

		return value;
	}
}
