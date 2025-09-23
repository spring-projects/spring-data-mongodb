/*
 * Copyright 2025. the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.data.mapping.PropertyPath;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentLruCache;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public sealed interface MongoPath permits MongoPath.RawMongoPath, MongoPath.MappedMongoPath {

	static RawMongoPath parse(String path) {
		return RawMongoPath.parse(path);
	}

	String path();

	List<? extends PathSegment> segments();

	interface PathSegment {

		boolean isNumeric();

		boolean isKeyword();

		String segment();

	}

	/**
	 * Represents a MongoDB path expression as in query and update paths. A MongoPath encapsulates paths consisting of
	 * field names, keywords and positional identifiers such as {@code foo}, {@code foo.bar},{@code foo.[0].bar} and
	 * allows transformations to {@link PropertyPath} and field-name transformation.
	 *
	 * @author Mark Paluch
	 */
	final class RawMongoPath implements MongoPath {

		private static final ConcurrentLruCache<String, RawMongoPath> CACHE = new ConcurrentLruCache<>(64,
				RawMongoPath::new);

		private final String path;
		private final List<Segment> segments;

		private RawMongoPath(String path) {
			this(path, segmentsOf(path));
		}

		RawMongoPath(String path, List<Segment> segments) {

			this.path = path;
			this.segments = List.copyOf(segments);
		}

		/**
		 * Parses a MongoDB path expression into MongoPath.
		 *
		 * @param path
		 * @return
		 */
		public static RawMongoPath parse(String path) {

			Assert.hasText(path, "Path must not be null or empty");
			return CACHE.get(path);
		}

		private static List<Segment> segmentsOf(String path) {
			return segmentsOf(path.split("\\."));
		}

		private static List<Segment> segmentsOf(String[] rawSegments) {

			List<Segment> segments = new ArrayList<>(rawSegments.length);
			for (String segment : rawSegments) {
				segments.add(Segment.of(segment));
			}
			return segments;
		}

		public List<Segment> getSegments() {
			return this.segments;
		}

		public List<Segment> segments() {
			return this.segments;
		}

		public String path() {
			return path;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof RawMongoPath mongoPath)) {
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

		public record Segment(String segment, boolean keyword, boolean numeric,
				TargetType targetType) implements PathSegment {

			private final static Pattern POSITIONAL = Pattern.compile("\\$\\[\\d+]");

			static Segment of(String segment) {

				Keyword keyword = Keyword.mapping.get(segment);

				if (keyword != null) {
					return new Segment(segment, true, false, keyword.getType());
				}

				if (POSITIONAL.matcher(segment).matches()) {
					return new Segment(segment, true, false, RawMongoPath.Keyword.$POSITIONAL.getType());
				}

				try {
					// positional paths
					Integer.decode(segment);
					return new Segment(segment, false, true, RawMongoPath.TargetType.PROPERTY);
				} catch (NumberFormatException e) {

				}

				return new Segment(segment, segment.startsWith("$"), false, RawMongoPath.TargetType.PROPERTY);
			}

			@Override
			public String toString() {
				return segment;
			}

			@Override
			public boolean isNumeric() {
				return numeric;
			}

			@Override
			public boolean isKeyword() {
				return keyword;
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

		public enum TargetType {
			PROPERTY, NUMERIC, COLLECTION, DOCUMENT, BOOLEAN, ANY;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	final class MappedMongoPath implements MongoPath {

		private final RawMongoPath source;
		private final List<MappedSegment> mappedSegments;

		public MappedMongoPath(RawMongoPath source, List<MappedSegment> segments) {
			this.source = source;
			this.mappedSegments = segments;
		}

		@Override
		public String path() {
			return StringUtils.collectionToDelimitedString(mappedSegments, ".");
		}

		public String sourcePath() {
			return source.path();
		}

		@Override
		public List<MappedSegment> segments() {
			return mappedSegments;
		}

		public String toString() {
			return path();
		}

		public record MappedSegment(PathSegment source, String mappedName) implements PathSegment {

			@Override
			public boolean isNumeric() {
				return source.isNumeric();
			}

			@Override
			public boolean isKeyword() {
				return source.isKeyword();
			}

			@Override
			public String segment() {
				return mappedName;
			}

			@NonNull
			@Override
			public String toString() {
				return mappedName;
			}
		}
	}
}
