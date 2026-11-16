/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.path;

import java.util.List;
import java.util.StringJoiner;

import org.jspecify.annotations.Nullable;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PositionalToken;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * A {@link MongoPath} resolved against a {@link Scope}. Each {@link Segment} carries its semantic meaning and, if
 * resolvable, the {@link MongoPersistentProperty} it refers to.
 *
 * @author Christoph Strobl
 * @since 5.3
 * @see MongoPathBinder
 */
public class BoundMongoPath {

	private final MongoPath source;
	private final Scope scope;
	private final List<Segment> segments;
	private final State state;

	BoundMongoPath(MongoPath source, Scope scope, List<Segment> segments, State state) {

		this.source = source;
		this.scope = scope;
		this.segments = List.copyOf(segments);
		this.state = state;
	}

	/**
	 * @return the {@link MongoPath} this path was bound from.
	 */
	public MongoPath source() {
		return source;
	}

	/**
	 * @return the {@link Scope} this path was bound against.
	 */
	public Scope scope() {
		return scope;
	}

	/**
	 * @return the bound segments.
	 */
	public List<Segment> segments() {
		return segments;
	}

	/**
	 * @return the last segment or {@literal null} if the path has no segments.
	 */
	public @Nullable Segment leaf() {
		return CollectionUtils.lastElement(segments);
	}

	/**
	 * @return {@literal true} if all segments are {@link Segment#isResolved() resolved}.
	 */
	public boolean isResolved() {

		for (Segment segment : segments) {
			if (!segment.isResolved()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * @return the property of the last {@link Segment.Property} segment, or the property referenced by an
	 *         {@link Segment.Exposed} segment, or {@literal null} if none.
	 */
	public @Nullable MongoPersistentProperty leafProperty() {

		for (int i = segments.size() - 1; i >= 0; i--) {

			Segment segment = segments.get(i);
			if (segment instanceof Segment.Property property) {
				return property.property();
			}
			if (segment instanceof Segment.Exposed exposed) {

				Segment.Property reference = exposed.reference();
				if (reference != null) {
					return reference.property();
				}
			}
		}
		return null;
	}

	/**
	 * @return the properties of all {@link Segment.Property} segments in path order.
	 */
	public List<MongoPersistentProperty> properties() {
		return segments.stream().filter(Segment.Property.class::isInstance).map(Segment.Property.class::cast)
				.map(Segment.Property::property).toList();
	}

	/**
	 * @return the type of the value the path points to or {@literal null} if unknown.
	 */
	public @Nullable TypeInformation<?> targetType() {
		return state.type();
	}

	/**
	 * @return the first association property along the path or {@literal null} if none.
	 */
	public @Nullable MongoPersistentProperty association() {
		return state.association();
	}

	/**
	 * @return the mapped path including the {@link MongoPath.Anchor anchor} prefix, rendered as
	 *         {@link Rendering#forType(PathType) default} for the path type.
	 */
	public String toMappedPath() {
		return toMappedPath(Rendering.forType(source.type()));
	}

	/**
	 * @param rendering the {@link Rendering} to apply.
	 * @return the mapped path including the {@link MongoPath.Anchor anchor} prefix.
	 */
	public String toMappedPath(Rendering rendering) {
		return source.anchor().render(toRelativeMappedPath(0, rendering));
	}

	/**
	 * Render the mapped path starting at the given segment index, without the {@link MongoPath.Anchor anchor} prefix.
	 * Used to render keys of nested documents relative to their parent path. Rendered as
	 * {@link Rendering#forType(PathType) default} for the path type.
	 *
	 * @param fromIndex the index of the first segment to render, between {@code 0} and the number of segments.
	 * @return the mapped path.
	 */
	public String toRelativeMappedPath(int fromIndex) {

		Assert.isTrue(fromIndex >= 0 && fromIndex <= segments.size(),
				() -> "Index %d out of range for %d segments".formatted(fromIndex, segments.size()));

		return toRelativeMappedPath(fromIndex, Rendering.forType(source.type()));
	}

	private String toRelativeMappedPath(int fromIndex, Rendering rendering) {

		StringJoiner joiner = new StringJoiner(".");
		for (int i = fromIndex; i < segments.size(); i++) {
			String rendered = segments.get(i).render(rendering);
			if (rendered != null) {
				joiner.add(rendered);
			}
		}
		return joiner.toString();
	}

	State state() {
		return state;
	}

	@Override
	public String toString() {
		return source + " -> " + toMappedPath();
	}

	/**
	 * Binding state after the last segment, used to continue binding appended paths.
	 */
	record State(@Nullable TypeInformation<?> type, @Nullable MongoPersistentProperty property,
			@Nullable MongoPersistentEntity<?> entity, @Nullable MongoPersistentProperty association, boolean aborted) {

		static State of(MongoPersistentEntity<?> entity) {
			return new State(entity.getTypeInformation(), null, entity, null, false);
		}

		static State of(@Nullable TypeInformation<?> type, @Nullable MongoPersistentProperty property,
				@Nullable MongoPersistentProperty association) {
			return new State(type, property, null, association, false);
		}

		static State unknown() {
			return of(null, null, null);
		}

		static State aborted(@Nullable MongoPersistentProperty association) {
			return new State(null, null, null, association, true);
		}
	}

	/**
	 * A semantic path element.
	 */
	public interface Segment {

		/**
		 * @return the raw text of the segment.
		 */
		String text();

		/**
		 * @return {@literal true} if the segment could be resolved.
		 */
		default boolean isResolved() {
			return true;
		}

		/**
		 * @param rendering the {@link Rendering} to apply.
		 * @return the mapped representation or {@literal null} if the segment is omitted from the mapped path.
		 */
		default @Nullable String render(Rendering rendering) {
			return text();
		}

		/**
		 * A segment resolved to a {@link MongoPersistentProperty}.
		 *
		 * @param text the raw text.
		 * @param property the resolved property.
		 * @param match how the property was matched.
		 * @param association the association this segment is reached through or {@literal null}.
		 */
		record Property(String text, MongoPersistentProperty property, Match match,
				@Nullable MongoPersistentProperty association) implements Segment {

			/**
			 * @return {@literal true} if the property is an identifier.
			 */
			public boolean isId() {
				return property.isIdProperty();
			}

			/**
			 * @return {@literal true} if the property is an association.
			 */
			public boolean isAssociation() {
				return property.isAssociation();
			}

			/**
			 * @return {@literal true} if the property is unwrapped.
			 */
			public boolean isUnwrapped() {
				return property.isUnwrapped();
			}

			/**
			 * @return {@literal true} if the segment is reached through an association.
			 */
			public boolean isReferenced() {
				return association != null;
			}

			/**
			 * @return {@literal true} if the segment was matched by its stored field name.
			 */
			public boolean isMapped() {
				return match == Match.FIELD_NAME;
			}

			@Override
			public @Nullable String render(Rendering rendering) {

				if (isUnwrapped()) {
					return null;
				}

				if (association != null && isId()) {
					return rendering == Rendering.EXPRESSION && association.isDbReference() ? "$id" : null;
				}

				return property.getFieldName();
			}
		}

		/**
		 * A field exposed by a previous aggregation stage.
		 *
		 * @param text the raw text.
		 * @param mappedName the name to render.
		 * @param reference the property segment the field plainly references or {@literal null} if computed.
		 */
		record Exposed(String text, String mappedName, @Nullable Property reference) implements Segment {

			@Override
			public String render(Rendering rendering) {
				return mappedName;
			}
		}

		/**
		 * A list or array index.
		 *
		 * @param text the raw text.
		 * @param index the parsed index.
		 */
		record ListIndex(String text, int index) implements Segment {

		}

		/**
		 * A map key.
		 *
		 * @param text the key.
		 */
		record MapKey(String text) implements Segment {

		}

		/**
		 * The {@code $ref} or {@code $db} field of a {@link com.mongodb.DBRef}.
		 *
		 * @param text the field name.
		 */
		record DbRefMember(String text) implements Segment {

		}

		/**
		 * A positional operator ({@code $}, {@code $[]}, {@code $[<identifier>]}).
		 *
		 * @param token the positional token.
		 */
		record PositionalOperator(PositionalToken token) implements Segment {

			@Override
			public String text() {
				return token.text();
			}
		}

		/**
		 * A numeric segment whose meaning cannot be determined from the available type information.
		 *
		 * @param text the raw text.
		 */
		record Ambiguous(String text) implements Segment {

			@Override
			public boolean isResolved() {
				return false;
			}
		}

		/**
		 * A segment that cannot be resolved.
		 *
		 * @param text the raw text.
		 */
		record Unresolved(String text) implements Segment {

			@Override
			public boolean isResolved() {
				return false;
			}
		}
	}

	/**
	 * How a path is rendered.
	 */
	public enum Rendering {

		/**
		 * Keys of query, update and sort documents. Identifiers behind associations are omitted as values are converted
		 * into references.
		 */
		QUERY,

		/**
		 * Field references in aggregation expressions. Identifiers behind associations point to their stored location.
		 */
		EXPRESSION;

		/**
		 * @param type must not be {@literal null}.
		 * @return {@link #EXPRESSION} for {@link PathType#AGGREGATION aggregation} paths, {@link #QUERY} otherwise.
		 */
		public static Rendering forType(PathType type) {
			return type == PathType.AGGREGATION ? EXPRESSION : QUERY;
		}
	}

	/**
	 * How a {@link Segment.Property} was matched.
	 */
	public enum Match {

		/**
		 * Matched by Java property name.
		 */
		PROPERTY_NAME,

		/**
		 * Matched by the stored field name.
		 */
		FIELD_NAME
	}
}
