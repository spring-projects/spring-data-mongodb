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
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mongodb.core.mapping.MongoPath.AssociationPath;
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPath;
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPathImpl.MappedPropertySegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.PathSegment.KeywordSegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.PathSegment.PositionSegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.PathSegment.PropertySegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.RawMongoPath;
import org.springframework.data.mongodb.core.mapping.MongoPath.RawMongoPath.Keyword;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentLruCache;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public sealed interface MongoPath permits AssociationPath, MappedMongoPath, RawMongoPath {

	static RawMongoPath parse(String path) {
		return RawMongoPath.parse(path);
	}

	String path();

	List<? extends PathSegment> segments();

	@Nullable
	MongoPath subpath(PathSegment segment);

	interface PathSegment {

		String segment();

		default boolean matches(PathSegment segment) {
			return this.equals(segment);
		}

		static PathSegment of(String segment) {

			Keyword keyword = Keyword.mapping.get(segment);

			if (keyword != null) {
				return new KeywordSegment(keyword, new Segment(segment));
			}

			if (PositionSegment.POSITIONAL.matcher(segment).matches()) {
				return new PositionSegment(new Segment(segment));
			}

			if (segment.startsWith("$")) {
				return new KeywordSegment(null, new Segment(segment));
			}

			return new PropertySegment(new Segment(segment));
		}

		record Segment(String segment) implements PathSegment {

		}

		class KeywordSegment implements PathSegment {

			final @Nullable Keyword keyword;
			final Segment segment;

			public KeywordSegment(@Nullable Keyword keyword, Segment segment) {

				this.keyword = keyword;
				this.segment = segment;
			}

			@Override
			public String segment() {
				return segment.segment();
			}

			@Override
			public String toString() {
				return segment();
			}
		}

		class PositionSegment implements PathSegment {

			/**
			 * n numeric position <br />
			 * $[] all positional operator for update operations, <br />
			 * $[id] filtered positional operator for update operations, <br />
			 * $ positional operator for update operations, <br />
			 * $ projection operator when array index position is unknown <br />
			 */
			private final static Pattern POSITIONAL = Pattern.compile("\\$\\[[a-zA-Z0-9]*]|\\$|\\d+");

			final Segment segment;

			public PositionSegment(Segment segment) {
				this.segment = segment;
			}

			@Override
			public String segment() {
				return segment.segment();
			}

			@Override
			public String toString() {
				return segment();
			}
		}

		class PropertySegment implements PathSegment {

			final Segment segment;

			public PropertySegment(Segment segment) {
				this.segment = segment;
			}

			@Override
			public String segment() {
				return segment.segment();
			}

			@Override
			public String toString() {
				return segment();
			}
		}

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
		private final List<PathSegment> segments;

		private RawMongoPath(String path) {
			this(path, segmentsOf(path));
		}

		RawMongoPath(String path, List<PathSegment> segments) {

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

		private static List<PathSegment> segmentsOf(String path) {
			return segmentsOf(path.split("\\."));
		}

		private static List<PathSegment> segmentsOf(String[] rawSegments) {

			List<PathSegment> segments = new ArrayList<>(rawSegments.length);
			for (String segment : rawSegments) {
				segments.add(PathSegment.of(segment));
			}
			return segments;
		}

		@Override
		public @Nullable RawMongoPath subpath(PathSegment lookup) {

			List<String> segments = new ArrayList<>(this.segments.size());
			for (PathSegment segment : this.segments) {
				segments.add(segment.segment());
				if (segment.equals(lookup)) {
					return MongoPath.parse(StringUtils.collectionToDelimitedString(segments, "."));
				}
			}
			return null;
		}

		public List<PathSegment> getSegments() {
			return this.segments;
		}

		public List<PathSegment> segments() {
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

		public enum Keyword {

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

	sealed interface MappedMongoPath extends MongoPath permits MappedMongoPathImpl {

		static MappedMongoPath just(RawMongoPath source) {
			return new MappedMongoPathImpl(source, TypeInformation.OBJECT,
					source.segments().stream().map(it -> new MappedPropertySegment(it.segment(), it, null)).toList());
		}

		@Nullable
		PropertyPath propertyPath();

		@Nullable
		AssociationPath associationPath();
	}

	sealed interface AssociationPath extends MongoPath permits AssociationPathImpl {

		@Nullable
		PropertyPath propertyPath();

		MappedMongoPath targetPath();

		@Nullable
		PropertyPath targetPropertyPath();
	}

	final class AssociationPathImpl implements AssociationPath {

		final MappedMongoPath source;
		final MappedMongoPath path;

		public AssociationPathImpl(MappedMongoPath source, MappedMongoPath path) {
			this.source = source;
			this.path = path;
		}

		@Override
		public String path() {
			return path.path();
		}

		@Override
		public List<? extends PathSegment> segments() {
			return path.segments();
		}

		@Nullable
		@Override
		public MongoPath subpath(PathSegment segment) {
			return path.subpath(segment);
		}

		@Nullable
		@Override
		public PropertyPath propertyPath() {
			return path.propertyPath();
		}

		@Nullable
		@Override
		public PropertyPath targetPropertyPath() {
			return source.propertyPath();
		}

		@Override
		public MappedMongoPath targetPath() {
			return source;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	final class MappedMongoPathImpl implements MappedMongoPath {

		private final RawMongoPath source;
		private final TypeInformation<?> type;
		private final List<? extends PathSegment> segments;
		private final Lazy<PropertyPath> propertyPath = Lazy.of(this::assemblePropertyPath);
		private final Lazy<String> mappedPath = Lazy.of(this::assembleMappedPath);
		private final Lazy<AssociationPath> associationPath = Lazy.of(this::assembleAssociationPath);

		public MappedMongoPathImpl(RawMongoPath source, TypeInformation<?> type, List<? extends PathSegment> segments) {
			this.source = source;
			this.type = type;
			this.segments = segments;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			MappedMongoPathImpl that = (MappedMongoPathImpl) o;
			return source.equals(that.source) && type.equals(that.type) && segments.equals(that.segments);
		}

		@Nullable
		@Override
		public MappedMongoPath subpath(PathSegment lookup) {

			List<PathSegment> segments = new ArrayList<>(this.segments.size());
			for (PathSegment segment : this.segments) {
				segments.add(segment);
				if (segment.matches(lookup)) {
					break;
				}
			}

			if (segments.isEmpty()) {
				return null;
			}

			return new MappedMongoPathImpl(source, type, segments);
		}

		@Override
		public int hashCode() {
			return Objects.hash(source, type, segments);
		}

		public static MappedMongoPath just(RawMongoPath source) {
			return new MappedMongoPathImpl(source, TypeInformation.OBJECT,
					source.segments().stream().map(it -> new MappedPropertySegment(it.segment(), it, null)).toList());
		}

		public @Nullable PropertyPath propertyPath() {
			return this.propertyPath.getNullable();
		}

		@Nullable
		@Override
		public AssociationPath associationPath() {
			return this.associationPath.getNullable();
		}

		private String assembleMappedPath() {
			return segments.stream().map(PathSegment::segment).filter(StringUtils::hasText).collect(Collectors.joining("."));
		}

		private @Nullable AssociationPath assembleAssociationPath() {

			for (PathSegment segment : this.segments) {
				if (segment instanceof AssociationSegment) {
					MappedMongoPath pathToAssociation = subpath(segment);
					return new AssociationPathImpl(this, pathToAssociation);
				}
			}
			return null;
		}

		private @Nullable PropertyPath assemblePropertyPath() {

			StringBuilder path = new StringBuilder();

			for (PathSegment segment : segments) {

				if (segment instanceof PropertySegment) {
					return null;
				}

				if (segment instanceof KeywordSegment || segment instanceof PositionSegment) {
					continue;
				}

				String name = segment.segment();
				if (segment instanceof MappedPropertySegment mappedSegment) {
					name = mappedSegment.getSource().segment();
				} else if (segment instanceof WrappedSegment wrappedSegment) {
					if (wrappedSegment.getInner() != null) {
						name = wrappedSegment.getOuter().getProperty().getName() + "."
								+ wrappedSegment.getInner().getProperty().getName();
					} else {
						name = wrappedSegment.getOuter().getProperty().getName();
					}
				}

				if (!path.isEmpty()) {
					path.append(".");
				}

				path.append(Pattern.quote(name));
			}

			if (path.isEmpty()) {
				return null;
			}

			return PropertyPath.from(path.toString(), type);
		}

		@Override
		public String path() {
			return mappedPath.get();
		}

		public MongoPath source() {
			return source;
		}

		@Override
		@SuppressWarnings("unchecked")
		public List<PathSegment> segments() {
			return (List<PathSegment>) segments;
		}

		public String toString() {
			return path();
		}

		public static class AssociationSegment extends MappedPropertySegment {
			public AssociationSegment(MappedPropertySegment segment) {
				super(segment.mappedName, segment.source, segment.property);
			}
		}

		public static class WrappedSegment implements PathSegment {

			private final String mappedName;
			private final MappedPropertySegment outer;
			private final MappedPropertySegment inner;

			public WrappedSegment(String mappedName, MappedPropertySegment outer, MappedPropertySegment inner) {
				this.mappedName = mappedName;
				this.outer = outer;
				this.inner = inner;
			}

			public MappedPropertySegment getInner() {
				return inner;
			}

			public MappedPropertySegment getOuter() {
				return outer;
			}

			@Override
			public String segment() {
				return mappedName;
			}

			@Override
			public String toString() {
				return segment();
			}

			@Override
			public boolean matches(PathSegment segment) {

				if (PathSegment.super.matches(segment)) {
					return true;
				}

				return this.outer.matches(segment) || this.inner.matches(segment);
			}
		}

		public static class MappedPropertySegment implements PathSegment {

			PathSegment source;
			String mappedName;
			MongoPersistentProperty property;

			public MappedPropertySegment(String mappedName, PathSegment source, MongoPersistentProperty property) {
				this.source = source;
				this.mappedName = mappedName;
				this.property = property;
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

			public PathSegment getSource() {
				return source;
			}

			public void setSource(PathSegment source) {
				this.source = source;
			}

			public String getMappedName() {
				return mappedName;
			}

			public void setMappedName(String mappedName) {
				this.mappedName = mappedName;
			}

			public MongoPersistentProperty getProperty() {
				return property;
			}

			public void setProperty(MongoPersistentProperty property) {
				this.property = property;
			}

			@Override
			public boolean matches(PathSegment segment) {

				if (PathSegment.super.matches(segment)) {
					return true;
				}

				return source.matches(segment);
			}
		}
	}
}
