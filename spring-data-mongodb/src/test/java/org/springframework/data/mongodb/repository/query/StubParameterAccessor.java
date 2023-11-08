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

import java.util.Arrays;
import java.util.Iterator;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.lang.Nullable;

/**
 * Simple {@link ParameterAccessor} that returns the given parameters unfiltered.
 *
 * @author Oliver Gierke
 * @author Christoh Strobl
 * @author Thomas Darimont
 */
class StubParameterAccessor implements MongoParameterAccessor {

	private final Object[] values;
	private Range<Distance> range = Range.unbounded();
	private @Nullable Collation colllation;

	/**
	 * Creates a new {@link ConvertingParameterAccessor} backed by a {@link StubParameterAccessor} simply returning the
	 * given parameters converted but unfiltered.
	 *
	 * @param converter
	 * @param parameters
	 * @return
	 */
	public static ConvertingParameterAccessor getAccessor(MongoWriter<Object> converter, Object... parameters) {
		return new ConvertingParameterAccessor(converter, new StubParameterAccessor(parameters));
	}

	@SuppressWarnings("unchecked")
	public StubParameterAccessor(Object... values) {

		this.values = values;

		for (Object value : values) {
			if (value instanceof Range) {
				this.range = (Range<Distance>) value;
			} else if (value instanceof Distance) {
				this.range = Range.from(Bound.<Distance> unbounded()).to(Bound.inclusive((Distance) value));
			} else if (value instanceof Collation) {
				this.colllation = Collation.class.cast(value);
			}
		}
	}

	@Override
	public ScrollPosition getScrollPosition() {
		return null;
	}

	public Pageable getPageable() {
		return null;
	}

	public Object getBindableValue(int index) {
		return values[index];
	}

	public boolean hasBindableNullValue() {
		return false;
	}

	public Sort getSort() {
		return Sort.unsorted();
	}

	@Override
	public Range<Distance> getDistanceRange() {
		return range;
	}

	public Iterator<Object> iterator() {
		return Arrays.asList(values).iterator();
	}

	public Point getGeoNearLocation() {
		return null;
	}

	@Override
	public TextCriteria getFullText() {
		return null;
	}

	@Override
	public Collation getCollation() {
		return this.colllation;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.MongoParameterAccessor#getValues()
	 */
	@Override
	public Object[] getValues() {
		return this.values;
	}

	@Override
	public Class<?> findDynamicProjection() {
		return null;
	}

	@Override
	public UpdateDefinition getUpdate() {
		return null;
	}
}
