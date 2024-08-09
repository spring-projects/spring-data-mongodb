/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Represents a MongoDB path expression as in query and update paths. A MongoPath encapsulates paths consisting of field
 * names, keywords and positional identifiers such as {@code foo}, {@code foo.bar},{@code foo.[0].bar} and allows
 * transformations to {@link PropertyPath} and field-name transformation.
 *
 * @author Mark Paluch
 */
public final class MongoPath {

	private final List<Segment> segments;

	private MongoPath(List<String> segments) {

		this.segments = new ArrayList<>(segments.size());
		for (String segment : segments) {
			this.segments.add(Segment.of(segment));
		}
	}

	/**
	 * Parses a MongoDB path expression into MongoPath.
	 *
	 * @param path
	 * @return
	 */
	public static MongoPath parse(String path) {

		Assert.hasText(path, "Path must not be null or empty");

		return new MongoPath(Arrays.asList(path.split("\\.")));
	}

	/**
	 * Apply field name conversion.
	 *
	 * @param context
	 * @param persistentEntity
	 * @return
	 */
	public MongoPath applyFieldNames(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context,
			MongoPersistentEntity<?> persistentEntity) {

		MongoPersistentEntity<?> entity = persistentEntity;
		List<String> segments = new ArrayList<>(this.segments.size());

		for (Segment segment : this.segments) {

			if (entity != null && !segment.keyword()
					&& (segment.targetType() == TargetType.ANY || segment.targetType() == TargetType.PROPERTY)) {

				MongoPersistentProperty persistentProperty = entity.getPersistentProperty(segment.segment);

				String name = segment.segment();

				if (persistentProperty != null) {

					if (persistentProperty.isEntity()) {
						entity = context.getPersistentEntity(persistentProperty);
					}

					if (persistentProperty.isUnwrapped()) {
						continue;
					}

					name = persistentProperty.getFieldName();
				}

				segments.add(name);
			} else {
				segments.add(segment.segment());
			}
		}

		return new MongoPath(segments);
	}

	/**
	 * Create a {@link PropertyPath} starting at {@link MongoPersistentEntity}.
	 * <p>
	 * Can return {@code null} if the property path contains named segments that are not mapped to the entity.
	 *
	 * @param context
	 * @param persistentEntity
	 * @return
	 */
	@Nullable
	public PropertyPath toPropertyPath(
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context,
			MongoPersistentEntity<?> persistentEntity) {

		StringBuilder path = new StringBuilder();
		MongoPersistentEntity<?> entity = persistentEntity;

		for (Segment segment : this.segments) {

			if (segment.keyword()) {
				continue;
			}

			if (entity == null) {
				return null;
			}

			MongoPersistentProperty persistentProperty = entity.getPersistentProperty(segment.segment);

			if (persistentProperty == null) {

				if (segment.numeric()) {
					continue;

				}

				return null;
			}

			entity = context.getPersistentEntity(persistentProperty);

			String name = segment.segment();

			if (!path.isEmpty()) {
				path.append(".");
			}
			path.append(Pattern.quote(name));
		}

		if (path.isEmpty()) {
			return null;
		}

		return PropertyPath.from(path.toString(), persistentEntity.getType());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof MongoPath mongoPath)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(segments, mongoPath.segments);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(segments);
	}

	@Override
	public String toString() {
		return StringUtils.collectionToDelimitedString(segments, ".");
	}

	record Segment(String segment, boolean keyword, boolean numeric, TargetType targetType) {

		private final static Pattern POSITIONAL = Pattern.compile("\\$\\[\\d+]");

		static Segment of(String segment) {

			Keyword keyword = Keyword.mapping.get(segment);

			if (keyword != null) {
				return new Segment(segment, true, false, keyword.getType());
			}

			if (POSITIONAL.matcher(segment).matches()) {
				return new Segment(segment, true, false, Keyword.$POSITIONAL.getType());
			}

			try {
				// positional paths
				Integer.decode(segment);
				return new Segment(segment, false, true, TargetType.PROPERTY);
			} catch (NumberFormatException e) {

			}

			return new Segment(segment, segment.startsWith("$"), false, TargetType.PROPERTY);
		}

		@Override
		public String toString() {
			return segment;
		}
	}

	enum Keyword {

		$PROJECTION("$", TargetType.PROPERTY), //
		$POSITIONAL("$[n]", TargetType.PROPERTY), //
		$ALL_POSITIONAL("$[]", TargetType.PROPERTY), //
		$IN(TargetType.COLLECTION), //
		$NIN(TargetType.COLLECTION), //
		$EXISTS(TargetType.BOOLEAN), //
		$TYPE(TargetType.ANY), //
		$SIZE(TargetType.NUMERIC), //
		$SET(TargetType.DOCUMENT), //
		$ALL(TargetType.COLLECTION), //
		$ELEM_MATCH("$elemMatch", TargetType.COLLECTION);

		private final String keyword;
		private final TargetType type;

		private static final Map<String, Keyword> mapping;

		static {

			Keyword[] values = Keyword.values();
			mapping = new LinkedHashMap<>(values.length, 1.0f);

			for (Keyword value : values) {
				mapping.put(value.getKeyword(), value);
			}

		}

		Keyword(TargetType type) {
			this.keyword = name().toLowerCase(Locale.ROOT);

			if (!keyword.startsWith("$")) {
				throw new IllegalStateException("Keyword " + name() + " does not start with $");
			}

			this.type = type;
		}

		Keyword(String keyword, TargetType type) {
			this.keyword = keyword;

			if (!keyword.startsWith("$")) {
				throw new IllegalStateException("Keyword " + name() + " does not start with $");
			}

			this.type = type;
		}

		public String getKeyword() {
			return keyword;
		}

		public TargetType getType() {
			return type;
		}
	}

	enum TargetType {
		PROPERTY, NUMERIC, COLLECTION, DOCUMENT, BOOLEAN, ANY;
	}
}
