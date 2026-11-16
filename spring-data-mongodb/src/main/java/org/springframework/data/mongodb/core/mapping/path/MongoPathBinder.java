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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath.Match;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath.Segment;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath.State;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.DbRefField;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Digits;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Name;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PositionalToken;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Token;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ConcurrentLruCache;
import org.springframework.util.ConcurrentReferenceHashMap;

/**
 * Binds {@link MongoPath paths} to a {@link Scope}, resolving each token segment by segment.
 * <ul>
 * <li>Names match the Java property name first, then a unique stored field name.</li>
 * <li>Numeric tokens are list indexes after collections, map keys after maps and names otherwise.</li>
 * <li>Binding continues into collection element and map value types.</li>
 * <li>After the first unresolvable segment, all remaining segments are {@link Segment.Unresolved unresolved}.</li>
 * <li>After a {@link org.springframework.data.mongodb.core.mapping.DBRef} only the identifier may follow. In
 * aggregation paths its {@code $id}, {@code $ref} and {@code $db} fields can be referenced.</li>
 * </ul>
 *
 * @author Christoph Strobl
 * @since 5.3
 */
public class MongoPathBinder {

	private static final Log LOGGER = LogFactory.getLog(MongoPathBinder.class);

	private static final String INVALID_ASSOCIATION_REFERENCE = "Invalid path reference %s; Associations can only be pointed to directly or via their id property";

	private static final Map<MappingContext<?, ?>, MongoPathBinder> BINDERS = new ConcurrentReferenceHashMap<>();

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final ConcurrentLruCache<BindKey, BoundMongoPath> cache;

	/**
	 * Create a new {@link MongoPathBinder}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @see #of(MappingContext)
	 */
	public MongoPathBinder(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		Assert.notNull(mappingContext, "MappingContext must not be null");

		this.mappingContext = mappingContext;
		this.cache = new ConcurrentLruCache<>(4096, key -> doBind(key.path(), key.scope(), key.policy()));
	}

	/**
	 * Obtain the shared {@link MongoPathBinder} for the given {@link MappingContext}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @return the shared {@link MongoPathBinder}.
	 */
	public static MongoPathBinder of(
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		Assert.notNull(mappingContext, "MappingContext must not be null");
		return BINDERS.computeIfAbsent(mappingContext, key -> new MongoPathBinder(mappingContext));
	}

	/**
	 * Parse and bind the given {@code path} using {@link LookupPolicy#RELAXED}.
	 *
	 * @param path must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @param scope must not be {@literal null}.
	 * @return the {@link BoundMongoPath}.
	 * @throws IllegalArgumentException if the path exceeds the maximum number of segments.
	 * @throws MappingException if the path violates association or field name constraints.
	 */
	public BoundMongoPath bind(String path, PathType type, Scope scope) {
		return bind(path, type, scope, LookupPolicy.RELAXED);
	}

	/**
	 * Parse and bind the given {@code path}.
	 *
	 * @param path must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @param scope must not be {@literal null}.
	 * @param policy must not be {@literal null}.
	 * @return the {@link BoundMongoPath}.
	 * @throws IllegalArgumentException if the path exceeds the maximum number of segments.
	 * @throws MappingException if {@link LookupPolicy#STRICT strict} and a segment cannot be resolved, or if the path
	 *           violates association or field name constraints.
	 */
	public BoundMongoPath bind(String path, PathType type, Scope scope, LookupPolicy policy) {
		return bind(MongoPath.parse(path, type), scope, policy);
	}

	/**
	 * Bind the given {@link MongoPath} using {@link LookupPolicy#RELAXED}.
	 *
	 * @param path must not be {@literal null}.
	 * @param scope must not be {@literal null}.
	 * @return the {@link BoundMongoPath}.
	 * @throws MappingException if the path violates association or field name constraints.
	 */
	public BoundMongoPath bind(MongoPath path, Scope scope) {
		return bind(path, scope, LookupPolicy.RELAXED);
	}

	/**
	 * Bind the given {@link MongoPath}. Bindings against an {@link Scope.EntityScope} or {@link Scope#none()} are cached,
	 * other scopes are bound on each use as their identity is not guaranteed to be stable.
	 *
	 * @param path must not be {@literal null}.
	 * @param scope must not be {@literal null}.
	 * @param policy must not be {@literal null}.
	 * @return the {@link BoundMongoPath}.
	 * @throws MappingException if {@link LookupPolicy#STRICT strict} and a segment cannot be resolved, or if the path
	 *           violates association or field name constraints.
	 */
	public BoundMongoPath bind(MongoPath path, Scope scope, LookupPolicy policy) {

		Assert.notNull(path, "Path must not be null");
		Assert.notNull(scope, "Scope must not be null");
		Assert.notNull(policy, "LookupPolicy must not be null");

		if (scope instanceof Scope.ExposedFieldsScope exposedFieldsScope) {
			return bindExposed(path, exposedFieldsScope, policy);
		}

		return isCacheable(path, scope) ? cache.get(new BindKey(path, scope, policy)) : doBind(path, scope, policy);
	}

	private static boolean isCacheable(MongoPath path, Scope scope) {
		return (scope instanceof Scope.EntityScope || scope instanceof Scope.NoScope)
				&& path.length() <= MongoPathParser.MAX_CACHEABLE_PATH_LENGTH;
	}

	private BoundMongoPath bindExposed(MongoPath path, Scope.ExposedFieldsScope scope, LookupPolicy policy) {

		Scope.Exposure exposure = CollectionUtils.firstElement(path.tokens()) instanceof Name first
				? scope.resolve(first.text())
				: null;

		if (exposure == null && scope.parent() != null) {
			return bind(path, scope.parent(), policy);
		}

		List<Segment> segments = new ArrayList<>(path.tokens().size());
		Context context = new Context(path, scope, policy, path.isPropertyReference());

		if (exposure == null) {
			return finish(path, scope, segments, bindTokens(path, State.aborted(null), segments, context));
		}

		BoundMongoPath source = resolved(exposure.source());
		Segment.Property reference = source != null ? referenceOf(source.leaf()) : null;
		segments.add(new Segment.Exposed(path.tokens().get(0).text(), exposure.mappedName(), reference));

		if (source == null) {
			// computed fields have no known structure
			State state = bindTokens(path.tail(), State.aborted(null), segments, context.relaxed());
			return finish(path, scope, segments, state);
		}

		State state = exposure.element() ? elementOf(source.state()) : source.state();
		return finish(path, scope, segments, bindTokens(path.tail(), state, segments, context));
	}

	private static Segment.@Nullable Property referenceOf(@Nullable Segment leaf) {

		if (leaf instanceof Segment.Property property) {
			return property;
		}
		return leaf instanceof Segment.Exposed exposed ? exposed.reference() : null;
	}

	/**
	 * Bind {@code path} relative to the end of {@code parent} using {@link LookupPolicy#RELAXED}.
	 *
	 * @param parent must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return the combined {@link BoundMongoPath}.
	 * @throws MappingException if the path violates association or field name constraints.
	 */
	public BoundMongoPath append(BoundMongoPath parent, MongoPath path) {
		return append(parent, path, LookupPolicy.RELAXED);
	}

	/**
	 * Bind {@code path} relative to the end of {@code parent}.
	 *
	 * @param parent must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param policy must not be {@literal null}.
	 * @return the combined {@link BoundMongoPath}.
	 * @throws MappingException if {@link LookupPolicy#STRICT strict} and a segment cannot be resolved, or if the path
	 *           violates association or field name constraints.
	 */
	public BoundMongoPath append(BoundMongoPath parent, MongoPath path, LookupPolicy policy) {

		Assert.notNull(parent, "Parent must not be null");
		Assert.notNull(path, "Path must not be null");
		Assert.notNull(policy, "LookupPolicy must not be null");

		MongoPath source = parent.source().append(path);
		List<Segment> segments = new ArrayList<>(parent.segments());
		Context context = new Context(source, parent.scope(), policy, path.isPropertyReference());

		return finish(source, parent.scope(), segments, bindTokens(path, parent.state(), segments, context));
	}

	private BoundMongoPath doBind(MongoPath path, Scope scope, LookupPolicy policy) {

		State initial = scope instanceof Scope.EntityScope entityScope ? State.of(entityScope.entity()) : State.unknown();

		List<Segment> segments = new ArrayList<>(path.tokens().size());
		State state = bindTokens(path, initial, segments, new Context(path, scope, policy, path.isPropertyReference()));
		BoundMongoPath result = finish(path, scope, segments, state);

		if (LOGGER.isDebugEnabled() && !result.isResolved()) {
			LOGGER.debug("Could not fully resolve path '%s' against %s: %s".formatted(path, scope, result.segments()));
		}

		return result;
	}

	private State bindTokens(MongoPath path, State state, List<Segment> segments, Context context) {

		for (Token token : path.tokens()) {

			if (state.aborted()) {
				segments.add(new Segment.Unresolved(token.text()));
			} else if (token instanceof PositionalToken positional) {
				state = bindPositional(positional, state, segments);
			} else if (token instanceof Digits digits) {
				state = bindDigits(digits, state, segments, context);
			} else if (token instanceof DbRefField dbRefField) {
				state = bindDbRefField(dbRefField, state, segments, context);
			} else {
				state = bindName((Name) token, state, segments, context);
			}

			Segment last = CollectionUtils.lastElement(segments);
			if (context.policy() == LookupPolicy.STRICT && last != null && !last.isResolved()) {
				throw new MappingException("Cannot resolve segment '%s' of path '%s' against %s".formatted(token.text(),
						context.source(), context.scope()));
			}
		}

		return state;
	}

	private State bindPositional(PositionalToken token, State state, List<Segment> segments) {

		segments.add(new Segment.PositionalOperator(token));

		TypeInformation<?> type = state.type();
		if (type != null && type.isCollectionLike()) {
			return step(type.getComponentType(), null, state.association());
		}
		return State.of(null, null, state.association());
	}

	private State bindDigits(Digits token, State state, List<Segment> segments, Context context) {

		TypeInformation<?> type = state.type();

		if (type != null && type.isMap()) {
			segments.add(new Segment.MapKey(token.text()));
			return step(type.getMapValueType(), null, state.association());
		}

		if (type != null && type.isCollectionLike()) {
			try {
				segments.add(new Segment.ListIndex(token.text(), Integer.parseInt(token.text())));
			} catch (NumberFormatException ex) {
				segments.add(new Segment.Ambiguous(token.text()));
				return State.aborted(state.association());
			}
			return step(type.getComponentType(), null, state.association());
		}

		if (type != null && entityOf(state) != null) {
			return bindProperty(token.text(), state, segments, context);
		}

		segments.add(new Segment.Ambiguous(token.text()));
		return State.aborted(state.association());
	}

	private State bindName(Name token, State state, List<Segment> segments, Context context) {

		TypeInformation<?> type = state.type();

		if (type != null && type.isMap()) {
			segments.add(new Segment.MapKey(token.text()));
			return step(type.getMapValueType(), null, state.association());
		}

		if (type != null && type.isCollectionLike()) {
			state = step(elementType(type), null, state.association());
		}

		return bindProperty(token.text(), state, segments, context);
	}

	private State bindDbRefField(DbRefField token, State state, List<Segment> segments, Context context) {

		MongoPersistentProperty dbRef = state.property();
		TypeInformation<?> type = state.type();

		if (dbRef == null || !dbRef.isDbReference() || type == null) {
			return bindProperty(token.text(), state, segments, context);
		}

		if (!"$id".equals(token.text())) {
			segments.add(new Segment.DbRefMember(token.text()));
			return State.of(TypeInformation.of(String.class), null, state.association());
		}

		State target = type.isCollectionLike() ? step(elementType(type), null, state.association()) : state;
		MongoPersistentEntity<?> entity = entityOf(target);
		MongoPersistentProperty idProperty = entity != null ? entity.getIdProperty() : null;

		if (idProperty == null) {
			segments.add(new Segment.Unresolved(token.text()));
			return State.aborted(state.association());
		}

		segments.add(new Segment.Property(token.text(), idProperty, Match.FIELD_NAME, state.association()));
		return step(idProperty.getTypeInformation(), idProperty, state.association());
	}

	private State bindProperty(String name, State state, List<Segment> segments, Context context) {

		MongoPersistentEntity<?> entity = entityOf(state);
		if (entity == null) {
			segments.add(new Segment.Unresolved(name));
			return State.aborted(state.association());
		}

		MongoPersistentProperty property = entity.getPersistentProperty(name);
		Match match = Match.PROPERTY_NAME;

		if (property == null && !context.propertyReference()) {
			property = findUniqueByFieldName(entity, name);
			match = Match.FIELD_NAME;
		}

		if (property == null) {
			segments.add(new Segment.Unresolved(name));
			return State.aborted(state.association());
		}

		MongoPersistentProperty association = state.association();
		if (association != null && association.isDbReference() && !property.isIdProperty()) {
			throw new MappingException(INVALID_ASSOCIATION_REFERENCE.formatted(context.source()));
		}

		segments.add(new Segment.Property(name, property, match, association));

		if (association == null && property.isAssociation()) {
			association = property;
		}

		return step(property.getTypeInformation(), property, association);
	}

	private static State elementOf(State state) {

		TypeInformation<?> type = state.type();
		return step(type != null && type.isCollectionLike() ? type.getComponentType() : type, null, state.association());
	}

	private static State step(@Nullable TypeInformation<?> type, @Nullable MongoPersistentProperty property,
			@Nullable MongoPersistentProperty association) {

		if (type == null || Class.class.equals(type.getType())) {
			return State.of(null, null, association);
		}
		return State.of(type, property, association);
	}

	private @Nullable MongoPersistentEntity<?> entityOf(State state) {

		if (state.entity() != null) {
			return state.entity();
		}

		TypeInformation<?> type = state.type();
		if (type == null || type.isCollectionLike() || type.isMap()) {
			return null;
		}

		MongoPersistentProperty property = state.property();

		try {
			return property != null && property.isUnwrapped() ? mappingContext.getPersistentEntity(property)
					: mappingContext.getPersistentEntity(type);
		} catch (MappingException ex) {
			return null;
		}
	}

	private BoundMongoPath finish(MongoPath source, Scope scope, List<Segment> segments, State state) {

		for (int i = 0; i < segments.size() - 1; i++) {
			if (segments.get(i) instanceof Segment.Property property && property.property().getMongoField().getName().isKey()) {
				throw new MappingException("Field name '%s' of property '%s' contains dots and cannot be traversed in path '%s'"
						.formatted(property.property().getFieldName(), property.property().getName(), source));
			}
		}

		return new BoundMongoPath(source, scope, segments, state);
	}

	private static @Nullable BoundMongoPath resolved(@Nullable BoundMongoPath path) {
		return path != null && path.isResolved() ? path : null;
	}

	private static @Nullable TypeInformation<?> elementType(TypeInformation<?> type) {

		TypeInformation<?> current = type;
		while (current != null && current.isCollectionLike()) {
			current = current.getComponentType();
		}
		return current;
	}

	private static @Nullable MongoPersistentProperty findUniqueByFieldName(MongoPersistentEntity<?> entity,
			String fieldName) {

		List<MongoPersistentProperty> candidates = new ArrayList<>(1);

		entity.doWithProperties((MongoPersistentProperty property) -> {
			if (fieldName.equals(property.getFieldName())) {
				candidates.add(property);
			}
		});
		entity.doWithAssociations((Association<MongoPersistentProperty> association) -> {
			if (fieldName.equals(association.getInverse().getFieldName())) {
				candidates.add(association.getInverse());
			}
		});

		return candidates.size() == 1 ? candidates.get(0) : null;
	}

	/**
	 * @param source the path reported in exceptions.
	 * @param propertyReference whether names match Java property names only.
	 */
	private record Context(MongoPath source, Scope scope, LookupPolicy policy, boolean propertyReference) {

		Context relaxed() {
			return new Context(source, scope, LookupPolicy.RELAXED, propertyReference);
		}
	}

	private record BindKey(MongoPath path, Scope scope, LookupPolicy policy) {}
}
