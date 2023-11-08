/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Term;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Mongo-specific {@link ParametersParameterAccessor} to allow access to the {@link Distance} parameter.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class MongoParametersParameterAccessor extends ParametersParameterAccessor implements MongoParameterAccessor {

	final MongoQueryMethod method;

	/**
	 * Creates a new {@link MongoParametersParameterAccessor}.
	 *
	 * @param method must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 */
	public MongoParametersParameterAccessor(MongoQueryMethod method, Object[] values) {

		super(method.getParameters(), values);

		this.method = method;
	}

	public Range<Distance> getDistanceRange() {

		MongoParameters mongoParameters = method.getParameters();

		int rangeIndex = mongoParameters.getRangeIndex();

		if (rangeIndex != -1) {
			return getValue(rangeIndex);
		}

		int maxDistanceIndex = mongoParameters.getMaxDistanceIndex();
		Bound<Distance> maxDistance = maxDistanceIndex == -1 ? Bound.unbounded()
				: Bound.inclusive((Distance) getValue(maxDistanceIndex));

		return Range.of(Bound.unbounded(), maxDistance);
	}

	public Point getGeoNearLocation() {

		int nearIndex = method.getParameters().getNearIndex();

		if (nearIndex == -1) {
			return null;
		}

		Object value = getValue(nearIndex);

		if (value == null) {
			return null;
		}

		if (value instanceof double[] typedValue) {
			if (typedValue.length != 2) {
				throw new IllegalArgumentException("The given double[] must have exactly 2 elements");
			} else {
				return new Point(typedValue[0], typedValue[1]);
			}
		}

		return (Point) value;
	}

	@Nullable
	@Override
	public TextCriteria getFullText() {
		int index = method.getParameters().getFullTextParameterIndex();
		return index >= 0 ? potentiallyConvertFullText(getValue(index)) : null;
	}

	protected TextCriteria potentiallyConvertFullText(Object fullText) {

		Assert.notNull(fullText, "Fulltext parameter must not be 'null'.");

		if (fullText instanceof String stringValue) {
			return TextCriteria.forDefaultLanguage().matching(stringValue);
		}

		if (fullText instanceof Term term) {
			return TextCriteria.forDefaultLanguage().matching(term);
		}

		if (fullText instanceof TextCriteria textCriteria) {
			return textCriteria;
		}

		throw new IllegalArgumentException(
				String.format("Expected full text parameter to be one of String, Term or TextCriteria but found %s.",
						ClassUtils.getShortName(fullText.getClass())));
	}

	@Override
	public Collation getCollation() {

		if (method.getParameters().getCollationParameterIndex() == -1) {
			return null;
		}

		return getValue(method.getParameters().getCollationParameterIndex());
	}

	@Override
	public Object[] getValues() {
		return super.getValues();
	}

	@Override
	public UpdateDefinition getUpdate() {

		int updateIndex = method.getParameters().getUpdateIndex();
		return updateIndex == -1 ? null : (UpdateDefinition) getValue(updateIndex);
	}
}
