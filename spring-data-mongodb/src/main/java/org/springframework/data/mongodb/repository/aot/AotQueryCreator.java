/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bson.conversions.Bson;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.CriteriaDefinition.Placeholder;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoParameterAccessor;
import org.springframework.data.mongodb.repository.query.MongoQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.TypeInformation;

import com.mongodb.DBRef;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
class AotQueryCreator {

	private MongoMappingContext mappingContext;

	public AotQueryCreator() {

		MongoMappingContext mongoMappingContext = new MongoMappingContext();
		mongoMappingContext.setSimpleTypeHolder(
				MongoCustomConversions.create((cfg) -> cfg.useNativeDriverJavaTimeCodecs()).getSimpleTypeHolder());
		mongoMappingContext.setAutoIndexCreation(false);
		mongoMappingContext.afterPropertiesSet();

		this.mappingContext = mongoMappingContext;
	}

	@SuppressWarnings("NullAway")
	StringQuery createQuery(PartTree partTree, int parameterCount) {

		Query query = new MongoQueryCreator(partTree,
				new PlaceholderConvertingParameterAccessor(new PlaceholderParameterAccessor(parameterCount)), mappingContext)
				.createQuery();

		if (partTree.isLimiting()) {
			query.limit(partTree.getMaxResults());
		}
		return new StringQuery(query);
	}

	static class PlaceholderConvertingParameterAccessor extends ConvertingParameterAccessor {

		/**
		 * Creates a new {@link ConvertingParameterAccessor} with the given {@link MongoWriter} and delegate.
		 *
		 * @param delegate must not be {@literal null}.
		 */
		public PlaceholderConvertingParameterAccessor(PlaceholderParameterAccessor delegate) {
			super(PlaceholderWriter.INSTANCE, delegate);
		}
	}

	@NullUnmarked
	enum PlaceholderWriter implements MongoWriter<Object> {

		INSTANCE;

		@Override
		public @Nullable Object convertToMongoType(@Nullable Object obj, @Nullable TypeInformation<?> typeInformation) {
			return obj instanceof Placeholder p ? p.getValue() : obj;
		}

		@Override
		public DBRef toDBRef(Object object, @Nullable MongoPersistentProperty referringProperty) {
			return null;
		}

		@Override
		public void write(Object source, Bson sink) {

		}
	}

	@NullUnmarked
	static class PlaceholderParameterAccessor implements MongoParameterAccessor {

		private final List<Placeholder> placeholders;

		public PlaceholderParameterAccessor(int parameterCount) {
			if (parameterCount == 0) {
				placeholders = List.of();
			} else {
				placeholders = IntStream.range(0, parameterCount).mapToObj(Placeholder::indexed).collect(Collectors.toList());
			}
		}

		@Override
		public Range<Distance> getDistanceRange() {
			return null;
		}

		@Override
		public @Nullable Vector getVector() {
			return null;
		}

		@Override
		public @Nullable Score getScore() {
			return null;
		}

		@Override
		public @Nullable Range<Score> getScoreRange() {
			return null;
		}

		@Override
		public @Nullable Point getGeoNearLocation() {
			return null;
		}

		@Override
		public @Nullable TextCriteria getFullText() {
			return null;
		}

		@Override
		public @Nullable Collation getCollation() {
			return null;
		}

		@Override
		public Object[] getValues() {
			return placeholders.toArray();
		}

		@Override
		public @Nullable UpdateDefinition getUpdate() {
			return null;
		}

		@Override
		public @Nullable ScrollPosition getScrollPosition() {
			return null;
		}

		@Override
		public Pageable getPageable() {
			return null;
		}

		@Override
		public Sort getSort() {
			return null;
		}

		@Override
		public @Nullable Class<?> findDynamicProjection() {
			return null;
		}

		@Override
		public @Nullable Object getBindableValue(int index) {
			return placeholders.get(index).getValue();
		}

		@Override
		public boolean hasBindableNullValue() {
			return false;
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Iterator<Object> iterator() {
			return ((List) placeholders).iterator();
		}
	}
}
