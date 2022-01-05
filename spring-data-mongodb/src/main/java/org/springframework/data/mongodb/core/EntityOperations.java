/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.bson.Document;
import org.springframework.core.convert.ConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mongodb.core.CollectionOptions.TimeSeriesOptions;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.mapping.TimeSeries;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.timeseries.Granularity;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Common operations performed on an entity in the context of it's mapping metadata.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 * @see MongoTemplate
 * @see ReactiveMongoTemplate
 */
class EntityOperations {

	private static final String ID_FIELD = "_id";

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context;

	private final EntityProjectionIntrospector introspector;

	EntityOperations(MongoConverter converter) {
		this(converter.getMappingContext(), converter.getCustomConversions(), converter.getProjectionFactory());
	}

	EntityOperations(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context,
			CustomConversions conversions, ProjectionFactory projectionFactory) {
		this.context = context;
		this.introspector = EntityProjectionIntrospector.create(projectionFactory,
				EntityProjectionIntrospector.ProjectionPredicate.typeHierarchy()
						.and(((target, underlyingType) -> !conversions.isSimpleType(target))),
				context);
	}

	/**
	 * Creates a new {@link Entity} for the given bean.
	 *
	 * @param entity must not be {@literal null}.
	 * @return new instance of {@link Entity}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	<T> Entity<T> forEntity(T entity) {

		Assert.notNull(entity, "Bean must not be null!");

		if (entity instanceof String) {
			return new UnmappedEntity(parse(entity.toString()));
		}

		if (entity instanceof Map) {
			return new SimpleMappedEntity((Map<String, Object>) entity);
		}

		return MappedEntity.of(entity, context);
	}

	/**
	 * Creates a new {@link AdaptibleEntity} for the given bean and {@link ConversionService}.
	 *
	 * @param entity must not be {@literal null}.
	 * @param conversionService must not be {@literal null}.
	 * @return new instance of {@link AdaptibleEntity}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	<T> AdaptibleEntity<T> forEntity(T entity, ConversionService conversionService) {

		Assert.notNull(entity, "Bean must not be null!");
		Assert.notNull(conversionService, "ConversionService must not be null!");

		if (entity instanceof String) {
			return new UnmappedEntity(parse(entity.toString()));
		}

		if (entity instanceof Map) {
			return new SimpleMappedEntity((Map<String, Object>) entity);
		}

		return AdaptibleMappedEntity.of(entity, context, conversionService);
	}

	/**
	 * @param source can be {@literal null}.
	 * @return {@literal true} if the given value is an {@literal array}, {@link Collection} or {@link Iterator}.
	 * @since 3.2
	 */
	static boolean isCollectionLike(@Nullable Object source) {

		if (source == null) {
			return false;
		}

		return ObjectUtils.isArray(source) || source instanceof Collection || source instanceof Iterator;
	}

	/**
	 * @param entityClass should not be null.
	 * @return the {@link MongoPersistentEntity#getCollection() collection name}.
	 */
	public String determineCollectionName(@Nullable Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity collection can't be determined!");
		}

		return context.getRequiredPersistentEntity(entityClass).getCollection();
	}

	public Query getByIdInQuery(Collection<?> entities) {

		MultiValueMap<String, Object> byIds = new LinkedMultiValueMap<>();

		entities.stream() //
				.map(this::forEntity) //
				.forEach(it -> byIds.add(it.getIdFieldName(), it.getId()));

		Criteria[] criterias = byIds.entrySet().stream() //
				.map(it -> Criteria.where(it.getKey()).in(it.getValue())) //
				.toArray(Criteria[]::new);

		return new Query(criterias.length == 1 ? criterias[0] : new Criteria().orOperator(criterias));
	}

	/**
	 * Returns the name of the identifier property. Considers mapping information but falls back to the MongoDB default of
	 * {@code _id} if no identifier property can be found.
	 *
	 * @param type must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	public String getIdPropertyName(Class<?> type) {

		Assert.notNull(type, "Type must not be null!");

		MongoPersistentEntity<?> persistentEntity = context.getPersistentEntity(type);

		if (persistentEntity != null && persistentEntity.getIdProperty() != null) {
			return persistentEntity.getRequiredIdProperty().getName();
		}

		return ID_FIELD;
	}

	/**
	 * Return the name used for {@code $geoNear.distanceField} avoiding clashes with potentially existing properties.
	 *
	 * @param domainType must not be {@literal null}.
	 * @return the name of the distanceField to use. {@literal dis} by default.
	 * @since 2.2
	 */
	public String nearQueryDistanceFieldName(Class<?> domainType) {

		MongoPersistentEntity<?> persistentEntity = context.getPersistentEntity(domainType);
		if (persistentEntity == null || persistentEntity.getPersistentProperty("dis") == null) {
			return "dis";
		}

		String distanceFieldName = "calculated-distance";
		int counter = 0;
		while (persistentEntity.getPersistentProperty(distanceFieldName) != null) {
			distanceFieldName += "-" + (counter++);
		}

		return distanceFieldName;
	}

	private static Document parse(String source) {

		try {
			return Document.parse(source);
		} catch (org.bson.json.JsonParseException o_O) {
			throw new MappingException("Could not parse given String to save into a JSON document!", o_O);
		} catch (RuntimeException o_O) {

			// legacy 3.x exception
			if (ClassUtils.matchesTypeName(o_O.getClass(), "JSONParseException")) {
				throw new MappingException("Could not parse given String to save into a JSON document!", o_O);
			}
			throw o_O;
		}
	}

	public <T> TypedOperations<T> forType(@Nullable Class<T> entityClass) {

		if (entityClass != null) {

			MongoPersistentEntity<?> entity = context.getPersistentEntity(entityClass);

			if (entity != null) {
				return new TypedEntityOperations(entity);
			}

		}
		return UntypedOperations.instance();
	}

	/**
	 * Introspect the given {@link Class result type} in the context of the {@link Class entity type} whether the returned
	 * type is a projection and what property paths are participating in the projection.
	 *
	 * @param resultType the type to project on. Must not be {@literal null}.
	 * @param entityType the source domain type. Must not be {@literal null}.
	 * @return the introspection result.
	 * @since 3.4
	 * @see EntityProjectionIntrospector#introspect(Class, Class)
	 */
	public <M, D> EntityProjection<M, D> introspectProjection(Class<M> resultType, Class<D> entityType) {
		return introspector.introspect(resultType, entityType);
	}

	/**
	 * A representation of information about an entity.
	 *
	 * @author Oliver Gierke
	 * @since 2.1
	 */
	interface Entity<T> {

		/**
		 * Returns the field name of the identifier of the entity.
		 *
		 * @return
		 */
		String getIdFieldName();

		/**
		 * Returns the identifier of the entity.
		 *
		 * @return
		 */
		Object getId();

		/**
		 * Returns the {@link Query} to find the entity by its identifier.
		 *
		 * @return
		 */
		Query getByIdQuery();

		/**
		 * Returns the {@link Query} to remove an entity by its {@literal id} and if applicable {@literal version}.
		 *
		 * @return the {@link Query} to use for removing the entity. Never {@literal null}.
		 * @since 2.2
		 */
		default Query getRemoveByQuery() {
			return isVersionedEntity() ? getQueryForVersion() : getByIdQuery();
		}

		/**
		 * Returns the {@link Query} to find the entity in its current version.
		 *
		 * @return
		 */
		Query getQueryForVersion();

		/**
		 * Maps the backing entity into a {@link MappedDocument} using the given {@link MongoWriter}.
		 *
		 * @param writer must not be {@literal null}.
		 * @return
		 */
		MappedDocument toMappedDocument(MongoWriter<? super T> writer);

		/**
		 * Asserts that the identifier type is updatable in case its not already set.
		 */
		default void assertUpdateableIdIfNotSet() {}

		/**
		 * Returns whether the entity is versioned, i.e. if it contains a version property.
		 *
		 * @return
		 */
		default boolean isVersionedEntity() {
			return false;
		}

		/**
		 * Returns the value of the version if the entity {@link #isVersionedEntity() has a version property}.
		 *
		 * @return the entity version. Can be {@literal null}.
		 * @throws IllegalStateException if the entity does not define a {@literal version} property. Make sure to check
		 *           {@link #isVersionedEntity()}.
		 */
		@Nullable
		Object getVersion();

		/**
		 * Returns the underlying bean.
		 *
		 * @return
		 */
		T getBean();

		/**
		 * Returns whether the entity is considered to be new.
		 *
		 * @return
		 * @since 2.1.2
		 */
		boolean isNew();
	}

	/**
	 * Information and commands on an entity.
	 *
	 * @author Oliver Gierke
	 * @since 2.1
	 */
	interface AdaptibleEntity<T> extends Entity<T> {

		/**
		 * Populates the identifier of the backing entity if it has an identifier property and there's no identifier
		 * currently present.
		 *
		 * @param id must not be {@literal null}.
		 * @return
		 */
		@Nullable
		T populateIdIfNecessary(@Nullable Object id);

		/**
		 * Initializes the version property of the of the current entity if available.
		 *
		 * @return the entity with the version property updated if available.
		 */
		T initializeVersionProperty();

		/**
		 * Increments the value of the version property if available.
		 *
		 * @return the entity with the version property incremented if available.
		 */
		T incrementVersion();

		/**
		 * Returns the current version value if the entity has a version property.
		 *
		 * @return the current version or {@literal null} in case it's uninitialized.
		 * @throws IllegalStateException if the entity does not define a {@literal version} property.
		 */
		@Nullable
		Number getVersion();
	}

	private static class UnmappedEntity<T extends Map<String, Object>> implements AdaptibleEntity<T> {

		private final T map;

		protected UnmappedEntity(T map) {
			this.map = map;
		}

		@Override
		public String getIdFieldName() {
			return ID_FIELD;
		}

		@Override
		public Object getId() {
			return map.get(ID_FIELD);
		}

		@Override
		public Query getByIdQuery() {
			return Query.query(Criteria.where(ID_FIELD).is(map.get(ID_FIELD)));
		}

		@Nullable
		@Override
		public T populateIdIfNecessary(@Nullable Object id) {

			map.put(ID_FIELD, id);

			return map;
		}

		@Override
		public Query getQueryForVersion() {
			throw new MappingException("Cannot query for version on plain Documents!");
		}

		@Override
		public MappedDocument toMappedDocument(MongoWriter<? super T> writer) {
			return MappedDocument.of(map instanceof Document //
					? (Document) map //
					: new Document(map));
		}

		@Override
		public T initializeVersionProperty() {
			return map;
		}

		@Override
		@Nullable
		public Number getVersion() {
			return null;
		}

		@Override
		public T incrementVersion() {
			return map;
		}

		@Override
		public T getBean() {
			return map;
		}

		@Override
		public boolean isNew() {
			return map.get(ID_FIELD) != null;
		}
	}

	private static class SimpleMappedEntity<T extends Map<String, Object>> extends UnmappedEntity<T> {

		protected SimpleMappedEntity(T map) {
			super(map);
		}

		@Override
		@SuppressWarnings("unchecked")
		public MappedDocument toMappedDocument(MongoWriter<? super T> writer) {

			T bean = getBean();
			bean = (T) (bean instanceof Document //
					? (Document) bean //
					: new Document(bean));
			Document document = new Document();
			writer.write(bean, document);

			return MappedDocument.of(document);
		}
	}

	private static class MappedEntity<T> implements Entity<T> {

		private final MongoPersistentEntity<?> entity;
		private final IdentifierAccessor idAccessor;
		private final PersistentPropertyAccessor<T> propertyAccessor;

		protected MappedEntity(MongoPersistentEntity<?> entity, IdentifierAccessor idAccessor,
				PersistentPropertyAccessor<T> propertyAccessor) {

			this.entity = entity;
			this.idAccessor = idAccessor;
			this.propertyAccessor = propertyAccessor;
		}

		private static <T> MappedEntity<T> of(T bean,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context) {

			MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(bean.getClass());
			IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(bean);
			PersistentPropertyAccessor<T> propertyAccessor = entity.getPropertyAccessor(bean);

			return new MappedEntity<>(entity, identifierAccessor, propertyAccessor);
		}

		@Override
		public String getIdFieldName() {
			return entity.getRequiredIdProperty().getFieldName();
		}

		@Override
		public Object getId() {
			return idAccessor.getRequiredIdentifier();
		}

		@Override
		public Query getByIdQuery() {

			if (!entity.hasIdProperty()) {
				throw new MappingException("No id property found for object of type " + entity.getType() + "!");
			}

			MongoPersistentProperty idProperty = entity.getRequiredIdProperty();

			return Query.query(Criteria.where(idProperty.getName()).is(getId()));
		}

		@Override
		public Query getQueryForVersion() {

			MongoPersistentProperty idProperty = entity.getRequiredIdProperty();
			MongoPersistentProperty versionProperty = entity.getRequiredVersionProperty();

			return new Query(Criteria.where(idProperty.getName()).is(getId())//
					.and(versionProperty.getName()).is(getVersion()));
		}

		@Override
		public MappedDocument toMappedDocument(MongoWriter<? super T> writer) {

			T bean = propertyAccessor.getBean();

			Document document = new Document();
			writer.write(bean, document);

			if (document.containsKey(ID_FIELD) && document.get(ID_FIELD) == null) {
				document.remove(ID_FIELD);
			}

			return MappedDocument.of(document);
		}

		public void assertUpdateableIdIfNotSet() {

			if (!entity.hasIdProperty()) {
				return;
			}

			MongoPersistentProperty property = entity.getRequiredIdProperty();
			Object propertyValue = idAccessor.getIdentifier();

			if (propertyValue != null) {
				return;
			}

			if (!MongoSimpleTypes.AUTOGENERATED_ID_TYPES.contains(property.getType())) {
				throw new InvalidDataAccessApiUsageException(
						String.format("Cannot autogenerate id of type %s for entity of type %s!", property.getType().getName(),
								entity.getType().getName()));
			}
		}

		@Override
		public boolean isVersionedEntity() {
			return entity.hasVersionProperty();
		}

		@Override
		@Nullable
		public Object getVersion() {
			return propertyAccessor.getProperty(entity.getRequiredVersionProperty());
		}

		@Override
		public T getBean() {
			return propertyAccessor.getBean();
		}

		@Override
		public boolean isNew() {
			return entity.isNew(propertyAccessor.getBean());
		}
	}

	private static class AdaptibleMappedEntity<T> extends MappedEntity<T> implements AdaptibleEntity<T> {

		private final MongoPersistentEntity<?> entity;
		private final ConvertingPropertyAccessor<T> propertyAccessor;
		private final IdentifierAccessor identifierAccessor;

		private AdaptibleMappedEntity(MongoPersistentEntity<?> entity, IdentifierAccessor identifierAccessor,
				ConvertingPropertyAccessor<T> propertyAccessor) {

			super(entity, identifierAccessor, propertyAccessor);

			this.entity = entity;
			this.propertyAccessor = propertyAccessor;
			this.identifierAccessor = identifierAccessor;
		}

		private static <T> AdaptibleEntity<T> of(T bean,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> context,
				ConversionService conversionService) {

			MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(bean.getClass());
			IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(bean);
			PersistentPropertyAccessor<T> propertyAccessor = entity.getPropertyAccessor(bean);

			return new AdaptibleMappedEntity<>(entity, identifierAccessor,
					new ConvertingPropertyAccessor<>(propertyAccessor, conversionService));
		}

		@Nullable
		@Override
		public T populateIdIfNecessary(@Nullable Object id) {

			if (id == null) {
				return propertyAccessor.getBean();
			}

			MongoPersistentProperty idProperty = entity.getIdProperty();
			if (idProperty == null) {
				return propertyAccessor.getBean();
			}

			if (identifierAccessor.getIdentifier() != null) {
				return propertyAccessor.getBean();
			}

			propertyAccessor.setProperty(idProperty, id);
			return propertyAccessor.getBean();
		}

		@Override
		@Nullable
		public Number getVersion() {

			MongoPersistentProperty versionProperty = entity.getRequiredVersionProperty();

			return propertyAccessor.getProperty(versionProperty, Number.class);
		}

		@Override
		public T initializeVersionProperty() {

			if (!entity.hasVersionProperty()) {
				return propertyAccessor.getBean();
			}

			MongoPersistentProperty versionProperty = entity.getRequiredVersionProperty();

			propertyAccessor.setProperty(versionProperty, versionProperty.getType().isPrimitive() ? 1 : 0);

			return propertyAccessor.getBean();
		}

		@Override
		public T incrementVersion() {

			MongoPersistentProperty versionProperty = entity.getRequiredVersionProperty();
			Number version = getVersion();
			Number nextVersion = version == null ? 0 : version.longValue() + 1;

			propertyAccessor.setProperty(versionProperty, nextVersion);

			return propertyAccessor.getBean();
		}
	}

	/**
	 * Type-specific operations abstraction.
	 *
	 * @author Mark Paluch
	 * @param <T>
	 * @since 2.2
	 */
	interface TypedOperations<T> {

		/**
		 * Return the optional {@link Collation} for the underlying entity.
		 *
		 * @return
		 */
		Optional<Collation> getCollation();

		/**
		 * Return the optional {@link Collation} from the given {@link Query} and fall back to the collation configured for
		 * the underlying entity.
		 *
		 * @return
		 */
		Optional<Collation> getCollation(Query query);

		/**
		 * Derive the applicable {@link CollectionOptions} for the given type.
		 *
		 * @return never {@literal null}.
		 * @since 3.3
		 */
		CollectionOptions getCollectionOptions();

		/**
		 * Map the fields of a given {@link TimeSeriesOptions} against the target domain type to consider potentially
		 * annotated field names.
		 *
		 * @param options must not be {@literal null}.
		 * @return never {@literal null}.
		 * @since 3.3
		 */
		TimeSeriesOptions mapTimeSeriesOptions(TimeSeriesOptions options);
	}

	/**
	 * {@link TypedOperations} for generic entities that are not represented with {@link PersistentEntity} (e.g. custom
	 * conversions).
	 */
	enum UntypedOperations implements TypedOperations<Object> {

		INSTANCE;

		UntypedOperations() {}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public static <T> TypedOperations<T> instance() {
			return (TypedOperations) INSTANCE;
		}

		@Override
		public Optional<Collation> getCollation() {
			return Optional.empty();
		}

		@Override
		public Optional<Collation> getCollation(Query query) {

			if (query == null) {
				return Optional.empty();
			}

			return query.getCollation();
		}

		@Override
		public CollectionOptions getCollectionOptions() {
			return CollectionOptions.empty();
		}

		@Override
		public TimeSeriesOptions mapTimeSeriesOptions(TimeSeriesOptions options) {
			return options;
		}
	}

	/**
	 * {@link TypedOperations} backed by {@link MongoPersistentEntity}.
	 *
	 * @param <T>
	 */
	static class TypedEntityOperations<T> implements TypedOperations<T> {

		private final MongoPersistentEntity<T> entity;

		protected TypedEntityOperations(MongoPersistentEntity<T> entity) {
			this.entity = entity;
		}

		@Override
		public Optional<Collation> getCollation() {
			return Optional.ofNullable(entity.getCollation());
		}

		@Override
		public Optional<Collation> getCollation(Query query) {

			if (query.getCollation().isPresent()) {
				return query.getCollation();
			}

			return Optional.ofNullable(entity.getCollation());
		}

		@Override
		public CollectionOptions getCollectionOptions() {

			CollectionOptions collectionOptions = CollectionOptions.empty();
			if (entity.hasCollation()) {
				collectionOptions = collectionOptions.collation(entity.getCollation());
			}

			if (entity.isAnnotationPresent(TimeSeries.class)) {

				TimeSeries timeSeries = entity.getRequiredAnnotation(TimeSeries.class);

				if (entity.getPersistentProperty(timeSeries.timeField()) == null) {
					throw new MappingException(String.format("Time series field '%s' does not exist in type %s",
							timeSeries.timeField(), entity.getName()));
				}

				TimeSeriesOptions options = TimeSeriesOptions.timeSeries(timeSeries.timeField());
				if (StringUtils.hasText(timeSeries.metaField())) {

					if (entity.getPersistentProperty(timeSeries.metaField()) == null) {
						throw new MappingException(
								String.format("Meta field '%s' does not exist in type %s", timeSeries.metaField(), entity.getName()));
					}

					options = options.metaField(timeSeries.metaField());
				}
				if (!Granularity.DEFAULT.equals(timeSeries.granularity())) {
					options = options.granularity(timeSeries.granularity());
				}
				collectionOptions = collectionOptions.timeSeries(options);
			}

			return collectionOptions;
		}

		@Override
		public TimeSeriesOptions mapTimeSeriesOptions(TimeSeriesOptions source) {

			TimeSeriesOptions target = TimeSeriesOptions.timeSeries(mappedNameOrDefault(source.getTimeField()));

			if (StringUtils.hasText(source.getMetaField())) {
				target = target.metaField(mappedNameOrDefault(source.getMetaField()));
			}
			return target.granularity(source.getGranularity());
		}

		private String mappedNameOrDefault(String name) {
			MongoPersistentProperty persistentProperty = entity.getPersistentProperty(name);
			return persistentProperty != null ? persistentProperty.getFieldName() : name;
		}
	}

}
