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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.mongodb.DBRef;

/**
 * Custom {@link ParameterAccessor} that uses a {@link MongoWriter} to serialize parameters into Mongo format.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class ConvertingParameterAccessor implements MongoParameterAccessor {

	private final MongoWriter<?> writer;
	private final MongoParameterAccessor delegate;

	/**
	 * Creates a new {@link ConvertingParameterAccessor} with the given {@link MongoWriter} and delegate.
	 *
	 * @param writer must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 */
	public ConvertingParameterAccessor(MongoWriter<?> writer, MongoParameterAccessor delegate) {

		Assert.notNull(writer, "MongoWriter must not be null");
		Assert.notNull(delegate, "MongoParameterAccessor must not be null");

		this.writer = writer;
		this.delegate = delegate;
	}

	public PotentiallyConvertingIterator iterator() {
		return new ConvertingIterator(delegate.iterator());
	}

	@Override
	public ScrollPosition getScrollPosition() {
		return delegate.getScrollPosition();
	}

	public Pageable getPageable() {
		return delegate.getPageable();
	}

	public Sort getSort() {
		return delegate.getSort();
	}

	@Override
	public Class<?> findDynamicProjection() {
		return delegate.findDynamicProjection();
	}

	public Object getBindableValue(int index) {
		return getConvertedValue(delegate.getBindableValue(index), null);
	}

	@Override
	public Range<Distance> getDistanceRange() {
		return delegate.getDistanceRange();
	}

	public Point getGeoNearLocation() {
		return delegate.getGeoNearLocation();
	}

	public TextCriteria getFullText() {
		return delegate.getFullText();
	}

	@Override
	public Collation getCollation() {
		return delegate.getCollation();
	}

	@Override
	public UpdateDefinition getUpdate() {
		return delegate.getUpdate();
	}

	/**
	 * Converts the given value with the underlying {@link MongoWriter}.
	 *
	 * @param value can be {@literal null}.
	 * @param typeInformation can be {@literal null}.
	 * @return can be {@literal null}.
	 */
	@Nullable
	private Object getConvertedValue(Object value, @Nullable TypeInformation<?> typeInformation) {
		return writer.convertToMongoType(value, typeInformation == null ? null : typeInformation.getActualType());
	}

	public boolean hasBindableNullValue() {
		return delegate.hasBindableNullValue();
	}

	/**
	 * Custom {@link Iterator} to convert items before returning them.
	 *
	 * @author Oliver Gierke
	 */
	private class ConvertingIterator implements PotentiallyConvertingIterator {

		private final Iterator<Object> delegate;

		/**
		 * Creates a new {@link ConvertingIterator} for the given delegate.
		 *
		 * @param delegate
		 */
		public ConvertingIterator(Iterator<Object> delegate) {
			this.delegate = delegate;
		}

		public boolean hasNext() {
			return delegate.hasNext();
		}

		public Object next() {
			return delegate.next();
		}

		public Object nextConverted(MongoPersistentProperty property) {

			Object next = next();

			if (next == null) {
				return null;
			}

			if (property.isAssociation()) {
				if (next.getClass().isArray() || next instanceof Iterable) {

					Collection<?> values = asCollection(next);

					List<DBRef> dbRefs = new ArrayList<DBRef>(values.size());
					for (Object element : values) {
						dbRefs.add(writer.toDBRef(element, property));
					}

					return dbRefs;
				} else {
					return writer.toDBRef(next, property);
				}
			}

			return getConvertedValue(next, property.getTypeInformation());
		}

		public void remove() {
			delegate.remove();
		}
	}

	/**
	 * Returns the given object as {@link Collection}. Will do a copy of it if it implements {@link Iterable} or is an
	 * array. Will return an empty {@link Collection} in case {@literal null} is given. Will wrap all other types into a
	 * single-element collection.
	 *
	 * @param source can be {@literal null}, returns an empty {@link List} in that case.
	 * @return never {@literal null}.
	 */
	private static Collection<?> asCollection(@Nullable Object source) {

		if (source instanceof Iterable) {

			if (source instanceof Collection) {
				return new ArrayList<>((Collection<?>) source);
			}

			List<Object> result = new ArrayList<>();
			for (Object element : (Iterable<?>) source) {
				result.add(element);
			}
			return result;
		}

		if (source == null) {
			return Collections.emptySet();
		}

		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}

	@Override
	public Object[] getValues() {
		return delegate.getValues();
	}

	/**
	 * Custom {@link Iterator} that adds a method to access elements in a converted manner.
	 *
	 * @author Oliver Gierke
	 */
	public interface PotentiallyConvertingIterator extends Iterator<Object> {

		/**
		 * Returns the next element which has already been converted.
		 *
		 * @return
		 */
		Object nextConverted(MongoPersistentProperty property);
	}
}
