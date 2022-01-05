/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.conversions.Bson;
import org.bson.json.JsonReader;
import org.bson.types.ObjectId;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.annotation.Reference;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mapping.AccessOptions;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionParameterValueProvider;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.DocumentPointer;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.PersistentPropertyTranslator;
import org.springframework.data.mongodb.core.mapping.Unwrapped;
import org.springframework.data.mongodb.core.mapping.Unwrapped.OnEmpty;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertCallback;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.Predicates;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

/**
 * {@link MongoConverter} that uses a {@link MappingContext} to do sophisticated mapping of domain objects to
 * {@link Document}.
 *
 * @author Oliver Gierke
 * @author Jon Brisbin
 * @author Patrik Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Jordi Llach
 * @author Mark Paluch
 * @author Roman Puchkovskiy
 * @author Heesu Jung
 * @author Divya Srivastava
 */
public class MappingMongoConverter extends AbstractMongoConverter implements ApplicationContextAware {

	private static final String INCOMPATIBLE_TYPES = "Cannot convert %1$s of type %2$s into an instance of %3$s! Implement a custom Converter<%2$s, %3$s> and register it with the CustomConversions. Parent object was: %4$s";
	private static final String INVALID_TYPE_TO_READ = "Expected to read Document %s into type %s but didn't find a PersistentEntity for the latter!";

	public static final ClassTypeInformation<Bson> BSON = ClassTypeInformation.from(Bson.class);

	protected static final Log LOGGER = LogFactory.getLog(MappingMongoConverter.class);

	protected final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	protected final QueryMapper idMapper;
	protected final DbRefResolver dbRefResolver;
	protected final DefaultDbRefProxyHandler dbRefProxyHandler;
	protected final ReferenceLookupDelegate referenceLookupDelegate;

	protected @Nullable ApplicationContext applicationContext;
	protected MongoTypeMapper typeMapper;
	protected @Nullable String mapKeyDotReplacement = null;
	protected @Nullable CodecRegistryProvider codecRegistryProvider;

	private MongoTypeMapper defaultTypeMapper;
	private SpELContext spELContext;
	private @Nullable EntityCallbacks entityCallbacks;
	private final DocumentPointerFactory documentPointerFactory;
	private final SpelAwareProxyProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();

	/**
	 * Creates a new {@link MappingMongoConverter} given the new {@link DbRefResolver} and {@link MappingContext}.
	 *
	 * @param dbRefResolver must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingMongoConverter(DbRefResolver dbRefResolver,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		super(new DefaultConversionService());

		Assert.notNull(dbRefResolver, "DbRefResolver must not be null!");
		Assert.notNull(mappingContext, "MappingContext must not be null!");

		this.dbRefResolver = dbRefResolver;

		this.mappingContext = mappingContext;
		this.defaultTypeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, mappingContext,
				this::getWriteTarget);
		this.idMapper = new QueryMapper(this);

		this.spELContext = new SpELContext(DocumentPropertyAccessor.INSTANCE);
		this.dbRefProxyHandler = new DefaultDbRefProxyHandler(spELContext, mappingContext,
				(prop, bson, evaluator, path) -> {

					ConversionContext context = getConversionContext(path);
					return MappingMongoConverter.this.getValueInternal(context, prop, bson, evaluator);
				});

		this.referenceLookupDelegate = new ReferenceLookupDelegate(mappingContext, spELContext);
		this.documentPointerFactory = new DocumentPointerFactory(conversionService, mappingContext);
	}

	/**
	 * Creates a new {@link ConversionContext} given {@link ObjectPath}.
	 *
	 * @param path the current {@link ObjectPath}, must not be {@literal null}.
	 * @return the {@link ConversionContext}.
	 */
	protected ConversionContext getConversionContext(ObjectPath path) {

		Assert.notNull(path, "ObjectPath must not be null");

		return new ConversionContext(conversions, path, this::readDocument, this::readCollectionOrArray, this::readMap,
				this::readDBRef, this::getPotentiallyConvertedSimpleRead);
	}

	/**
	 * Creates a new {@link MappingMongoConverter} given the new {@link MongoDatabaseFactory} and {@link MappingContext}.
	 *
	 * @deprecated use the constructor taking a {@link DbRefResolver} instead.
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	@Deprecated
	public MappingMongoConverter(MongoDatabaseFactory mongoDbFactory,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		this(new DefaultDbRefResolver(mongoDbFactory), mappingContext);
		setCodecRegistryProvider(mongoDbFactory);
	}

	/**
	 * Configures the {@link MongoTypeMapper} to be used to add type information to {@link Document}s created by the
	 * converter and how to lookup type information from {@link Document}s when reading them. Uses a
	 * {@link DefaultMongoTypeMapper} by default. Setting this to {@literal null} will reset the {@link TypeMapper} to the
	 * default one.
	 *
	 * @param typeMapper the typeMapper to set. Can be {@literal null}.
	 */
	public void setTypeMapper(@Nullable MongoTypeMapper typeMapper) {
		this.typeMapper = typeMapper;
	}

	@Override
	public MongoTypeMapper getTypeMapper() {
		return this.typeMapper == null ? this.defaultTypeMapper : this.typeMapper;
	}

	@Override
	public ProjectionFactory getProjectionFactory() {
		return projectionFactory;
	}

	@Override
	public CustomConversions getCustomConversions() {
		return conversions;
	}

	/**
	 * Configure the characters dots potentially contained in a {@link Map} shall be replaced with. By default we don't do
	 * any translation but rather reject a {@link Map} with keys containing dots causing the conversion for the entire
	 * object to fail. If further customization of the translation is needed, have a look at
	 * {@link #potentiallyEscapeMapKey(String)} as well as {@link #potentiallyUnescapeMapKey(String)}.
	 * <p>
	 * {@code mapKeyDotReplacement} is used as-is during replacement operations without further processing (i.e. regex or
	 * normalization).
	 *
	 * @param mapKeyDotReplacement the mapKeyDotReplacement to set. Can be {@literal null}.
	 */
	public void setMapKeyDotReplacement(@Nullable String mapKeyDotReplacement) {
		this.mapKeyDotReplacement = mapKeyDotReplacement;
	}

	/**
	 * Configure a {@link CodecRegistryProvider} that provides native MongoDB {@link org.bson.codecs.Codec codecs} for
	 * reading values.
	 *
	 * @param codecRegistryProvider can be {@literal null}.
	 * @since 2.2
	 */
	public void setCodecRegistryProvider(@Nullable CodecRegistryProvider codecRegistryProvider) {
		this.codecRegistryProvider = codecRegistryProvider;
	}

	public MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		this.applicationContext = applicationContext;
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
		this.projectionFactory.setBeanFactory(applicationContext);
		this.projectionFactory.setBeanClassLoader(applicationContext.getClassLoader());

		if (entityCallbacks == null) {
			setEntityCallbacks(EntityCallbacks.create(applicationContext));
		}

		ClassLoader classLoader = applicationContext.getClassLoader();
		if (this.defaultTypeMapper instanceof BeanClassLoaderAware && classLoader != null) {
			((BeanClassLoaderAware) this.defaultTypeMapper).setBeanClassLoader(classLoader);
		}
	}

	/**
	 * Set the {@link EntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.EntityCallback callbacks} like the {@link AfterConvertCallback}.
	 * <br />
	 * Overrides potentially existing {@link EntityCallbacks}.
	 *
	 * @param entityCallbacks must not be {@literal null}.
	 * @throws IllegalArgumentException if the given instance is {@literal null}.
	 * @since 3.0
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {

		Assert.notNull(entityCallbacks, "EntityCallbacks must not be null!");
		this.entityCallbacks = entityCallbacks;
	}

	@Override
	public <R> R project(EntityProjection<R, ?> projection, Bson bson) {

		if (!projection.isProjection()) { // backed by real object

			TypeInformation<?> typeToRead = projection.getMappedType().getType().isInterface() ? projection.getDomainType()
					: projection.getMappedType();
			return (R) read(typeToRead, bson);
		}

		ProjectingConversionContext context = new ProjectingConversionContext(conversions, ObjectPath.ROOT,
				this::readCollectionOrArray, this::readMap, this::readDBRef, this::getPotentiallyConvertedSimpleRead,
				projection);

		return doReadProjection(context, bson, projection);
	}

	@SuppressWarnings("unchecked")
	private <R> R doReadProjection(ConversionContext context, Bson bson,
			EntityProjection<R, ?> projection) {

		MongoPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(projection.getActualDomainType());
		TypeInformation<?> mappedType = projection.getActualMappedType();
		MongoPersistentEntity<R> mappedEntity = (MongoPersistentEntity<R>) getMappingContext()
				.getPersistentEntity(mappedType);
		SpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(bson, spELContext);

		boolean isInterfaceProjection = mappedType.getType().isInterface();
		if (isInterfaceProjection) {

			PersistentPropertyTranslator propertyTranslator = PersistentPropertyTranslator.create(mappedEntity);
			DocumentAccessor documentAccessor = new DocumentAccessor(bson);
			PersistentPropertyAccessor<?> accessor = new MapPersistentPropertyAccessor();

			PersistentPropertyAccessor<?> convertingAccessor = PropertyTranslatingPropertyAccessor
					.create(new ConvertingPropertyAccessor<>(accessor, conversionService), propertyTranslator);
			MongoDbPropertyValueProvider valueProvider = new MongoDbPropertyValueProvider(context, documentAccessor,
					evaluator);

			readProperties(context, entity, convertingAccessor, documentAccessor, valueProvider, evaluator,
					Predicates.isTrue());
			return (R) projectionFactory.createProjection(mappedType.getType(), accessor.getBean());
		}

		// DTO projection
		if (mappedEntity == null) {
			throw new MappingException(String.format("No mapping metadata found for %s", mappedType.getType().getName()));
		}

		// create target instance, merge metadata from underlying DTO type
		PersistentPropertyTranslator propertyTranslator = PersistentPropertyTranslator.create(entity,
				Predicates.negate(MongoPersistentProperty::hasExplicitFieldName));
		DocumentAccessor documentAccessor = new DocumentAccessor(bson) {
			@Override
			String getFieldName(MongoPersistentProperty prop) {
				return propertyTranslator.translate(prop).getFieldName();
			}
		};

		PreferredConstructor<?, MongoPersistentProperty> persistenceConstructor = mappedEntity.getPersistenceConstructor();
		ParameterValueProvider<MongoPersistentProperty> provider = persistenceConstructor != null
				&& persistenceConstructor.hasParameters()
						? getParameterProvider(context, mappedEntity, documentAccessor, evaluator)
						: NoOpParameterValueProvider.INSTANCE;

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(mappedEntity);
		R instance = instantiator.createInstance(mappedEntity, provider);
		PersistentPropertyAccessor<R> accessor = mappedEntity.getPropertyAccessor(instance);

		populateProperties(context, mappedEntity, documentAccessor, evaluator, instance);

		PersistentPropertyAccessor<?> convertingAccessor = new ConvertingPropertyAccessor<>(accessor, conversionService);
		MongoDbPropertyValueProvider valueProvider = new MongoDbPropertyValueProvider(context, documentAccessor, evaluator);

		readProperties(context, mappedEntity, convertingAccessor, documentAccessor, valueProvider, evaluator,
				Predicates.isTrue());

		return accessor.getBean();
	}

	private Object doReadOrProject(ConversionContext context, Bson source, TypeInformation<?> typeHint,
			EntityProjection<?, ?> typeDescriptor) {

		if (typeDescriptor.isProjection()) {
			return doReadProjection(context, BsonUtils.asDocument(source), typeDescriptor);
		}

		return readDocument(context, source, typeHint);
	}

	class ProjectingConversionContext extends ConversionContext {

		private final EntityProjection<?, ?> returnedTypeDescriptor;

		ProjectingConversionContext(CustomConversions customConversions, ObjectPath path,
				ContainerValueConverter<Collection<?>> collectionConverter, ContainerValueConverter<Bson> mapConverter,
				ContainerValueConverter<DBRef> dbRefConverter, ValueConverter<Object> elementConverter,
				EntityProjection<?, ?> projection) {
			super(customConversions, path,
					(context, source, typeHint) -> doReadOrProject(context, source, typeHint, projection),

					collectionConverter, mapConverter, dbRefConverter, elementConverter);
			this.returnedTypeDescriptor = projection;
		}

		@Override
		public ConversionContext forProperty(String name) {

			EntityProjection<?, ?> property = returnedTypeDescriptor.findProperty(name);
			if (property == null) {
				return super.forProperty(name);
			}

			return new ProjectingConversionContext(conversions, path, collectionConverter, mapConverter, dbRefConverter,
					elementConverter, property);
		}

		@Override
		public ConversionContext withPath(ObjectPath currentPath) {
			return new ProjectingConversionContext(conversions, currentPath, collectionConverter, mapConverter,
					dbRefConverter, elementConverter, returnedTypeDescriptor);
		}
	}

	static class MapPersistentPropertyAccessor implements PersistentPropertyAccessor<Map<String, Object>> {

		Map<String, Object> map = new LinkedHashMap<>();

		@Override
		public void setProperty(PersistentProperty<?> persistentProperty, Object o) {
			map.put(persistentProperty.getName(), o);
		}

		@Override
		public Object getProperty(PersistentProperty<?> persistentProperty) {
			return map.get(persistentProperty.getName());
		}

		@Override
		public Map<String, Object> getBean() {
			return map;
		}
	}

	public <S extends Object> S read(Class<S> clazz, Bson bson) {
		return read(ClassTypeInformation.from(clazz), bson);
	}

	protected <S extends Object> S read(TypeInformation<S> type, Bson bson) {
		return readDocument(getConversionContext(ObjectPath.ROOT), bson, type);
	}

	/**
	 * Conversion method to materialize an object from a {@link Bson document}. Can be overridden by subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param bson must not be {@literal null}
	 * @param typeHint the {@link TypeInformation} to be used to unmarshall this {@link Document}.
	 * @return the converted object, will never be {@literal null}.
	 * @since 3.2
	 */
	@SuppressWarnings("unchecked")
	protected <S extends Object> S readDocument(ConversionContext context, Bson bson,
			TypeInformation<? extends S> typeHint) {

		Document document = bson instanceof BasicDBObject ? new Document((BasicDBObject) bson) : (Document) bson;
		TypeInformation<? extends S> typeToRead = getTypeMapper().readType(document, typeHint);
		Class<? extends S> rawType = typeToRead.getType();

		if (conversions.hasCustomReadTarget(bson.getClass(), rawType)) {
			return doConvert(bson, rawType, typeHint.getType());
		}

		if (Document.class.isAssignableFrom(rawType)) {
			return (S) bson;
		}

		if (DBObject.class.isAssignableFrom(rawType)) {

			if (bson instanceof DBObject) {
				return (S) bson;
			}

			if (bson instanceof Document) {
				return (S) new BasicDBObject((Document) bson);
			}

			return (S) bson;
		}

		if (typeToRead.isMap()) {
			return context.convert(bson, typeToRead);
		}

		if (BSON.isAssignableFrom(typeHint)) {
			return (S) bson;
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToRead);

		if (entity == null) {

			if (codecRegistryProvider != null) {

				Optional<? extends Codec<? extends S>> codec = codecRegistryProvider.getCodecFor(rawType);
				if (codec.isPresent()) {
					return codec.get().decode(new JsonReader(document.toJson()), DecoderContext.builder().build());
				}
			}

			throw new MappingException(String.format(INVALID_TYPE_TO_READ, document, rawType));
		}

		return read(context, (MongoPersistentEntity<S>) entity, document);
	}

	private ParameterValueProvider<MongoPersistentProperty> getParameterProvider(ConversionContext context,
			MongoPersistentEntity<?> entity, DocumentAccessor source, SpELExpressionEvaluator evaluator) {

		AssociationAwareMongoDbPropertyValueProvider provider = new AssociationAwareMongoDbPropertyValueProvider(context,
				source, evaluator);
		PersistentEntityParameterValueProvider<MongoPersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<>(
				entity, provider, context.getPath().getCurrentObject());

		return new ConverterAwareSpELExpressionParameterValueProvider(context, evaluator, conversionService,
				parameterProvider);
	}

	private <S> S read(ConversionContext context, MongoPersistentEntity<S> entity, Document bson) {

		SpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(bson, spELContext);
		DocumentAccessor documentAccessor = new DocumentAccessor(bson);

		if (hasIdentifier(bson)) {
			S existing = findContextualEntity(context, entity, bson);
			if (existing != null) {
				return existing;
			}
		}

		PreferredConstructor<S, MongoPersistentProperty> persistenceConstructor = entity.getPersistenceConstructor();

		ParameterValueProvider<MongoPersistentProperty> provider = persistenceConstructor != null
				&& persistenceConstructor.hasParameters() ? getParameterProvider(context, entity, documentAccessor, evaluator)
						: NoOpParameterValueProvider.INSTANCE;

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, provider);

		if (entity.requiresPropertyPopulation()) {

			return populateProperties(context, entity, documentAccessor, evaluator, instance);
		}

		return instance;
	}

	private boolean hasIdentifier(Document bson) {
		return bson.get(BasicMongoPersistentProperty.ID_FIELD_NAME) != null;
	}

	@Nullable
	private <S> S findContextualEntity(ConversionContext context, MongoPersistentEntity<S> entity, Document bson) {
		return context.getPath().getPathItem(bson.get(BasicMongoPersistentProperty.ID_FIELD_NAME), entity.getCollection(),
				entity.getType());
	}

	private <S> S populateProperties(ConversionContext context, MongoPersistentEntity<S> entity,
			DocumentAccessor documentAccessor, SpELExpressionEvaluator evaluator, S instance) {

		PersistentPropertyAccessor<S> accessor = new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(instance),
				conversionService);

		// Make sure id property is set before all other properties

		Object rawId = readAndPopulateIdentifier(context, accessor, documentAccessor, entity, evaluator);
		ObjectPath currentPath = context.getPath().push(accessor.getBean(), entity, rawId);
		ConversionContext contextToUse = context.withPath(currentPath);

		MongoDbPropertyValueProvider valueProvider = new MongoDbPropertyValueProvider(contextToUse, documentAccessor,
				evaluator);

		Predicate<MongoPersistentProperty> propertyFilter = isIdentifier(entity).or(isConstructorArgument(entity)).negate();
		readProperties(contextToUse, entity, accessor, documentAccessor, valueProvider, evaluator, propertyFilter);

		return accessor.getBean();
	}

	/**
	 * Reads the identifier from either the bean backing the {@link PersistentPropertyAccessor} or the source document in
	 * case the identifier has not be populated yet. In this case the identifier is set on the bean for further reference.
	 */
	@Nullable
	private Object readAndPopulateIdentifier(ConversionContext context, PersistentPropertyAccessor<?> accessor,
			DocumentAccessor document, MongoPersistentEntity<?> entity, SpELExpressionEvaluator evaluator) {

		Object rawId = document.getRawId(entity);

		if (!entity.hasIdProperty() || rawId == null) {
			return rawId;
		}

		MongoPersistentProperty idProperty = entity.getRequiredIdProperty();

		if (idProperty.isImmutable() && entity.isConstructorArgument(idProperty)) {
			return rawId;
		}

		accessor.setProperty(idProperty, readIdValue(context, evaluator, idProperty, rawId));

		return rawId;
	}

	@Nullable
	private Object readIdValue(ConversionContext context, SpELExpressionEvaluator evaluator,
			MongoPersistentProperty idProperty, Object rawId) {

		String expression = idProperty.getSpelExpression();
		Object resolvedValue = expression != null ? evaluator.evaluate(expression) : rawId;

		return resolvedValue != null ? readValue(context, resolvedValue, idProperty.getTypeInformation()) : null;
	}

	private void readProperties(ConversionContext context, MongoPersistentEntity<?> entity,
			PersistentPropertyAccessor<?> accessor, DocumentAccessor documentAccessor,
			MongoDbPropertyValueProvider valueProvider, SpELExpressionEvaluator evaluator,
			Predicate<MongoPersistentProperty> propertyFilter) {

		DbRefResolverCallback callback = null;

		for (MongoPersistentProperty prop : entity) {

			if (!propertyFilter.test(prop)) {
				continue;
			}

			ConversionContext propertyContext = context.forProperty(prop.getName());
			MongoDbPropertyValueProvider valueProviderToUse = valueProvider.withContext(propertyContext);

			if (prop.isAssociation() && !entity.isConstructorArgument(prop)) {

				if (callback == null) {
					callback = getDbRefResolverCallback(propertyContext, documentAccessor, evaluator);
				}

				readAssociation(prop.getRequiredAssociation(), accessor, documentAccessor, dbRefProxyHandler, callback,
						propertyContext,
						evaluator);
				continue;
			}

			if (prop.isUnwrapped()) {

				accessor.setProperty(prop,
						readUnwrapped(propertyContext, documentAccessor, prop, mappingContext.getRequiredPersistentEntity(prop)));
				continue;
			}

			if (!documentAccessor.hasValue(prop)) {
				continue;
			}

			if (prop.isAssociation()) {

				if (callback == null) {
					callback = getDbRefResolverCallback(propertyContext, documentAccessor, evaluator);
				}

				readAssociation(prop.getRequiredAssociation(), accessor, documentAccessor, dbRefProxyHandler, callback,
						propertyContext,
						evaluator);
				continue;
			}

			accessor.setProperty(prop, valueProviderToUse.getPropertyValue(prop));
		}
	}

	private DbRefResolverCallback getDbRefResolverCallback(ConversionContext context, DocumentAccessor documentAccessor,
			SpELExpressionEvaluator evaluator) {

		return new DefaultDbRefResolverCallback(documentAccessor.getDocument(), context.getPath(), evaluator,
				(prop, bson, e, path) -> MappingMongoConverter.this.getValueInternal(context, prop, bson, e));
	}

	private void readAssociation(Association<MongoPersistentProperty> association, PersistentPropertyAccessor<?> accessor,
			DocumentAccessor documentAccessor, DbRefProxyHandler handler, DbRefResolverCallback callback,
			ConversionContext context, SpELExpressionEvaluator evaluator) {

		MongoPersistentProperty property = association.getInverse();
		Object value = documentAccessor.get(property);

		if (property.isDocumentReference()
				|| (!property.isDbReference() && property.findAnnotation(Reference.class) != null)) {

			// quite unusual but sounds like worth having?

			if (conversionService.canConvert(DocumentPointer.class, property.getActualType())) {

				if (value == null) {
					return;
				}

				DocumentPointer<?> pointer = () -> value;

				// collection like special treatment
				accessor.setProperty(property, conversionService.convert(pointer, property.getActualType()));
			} else {

				accessor.setProperty(property,
						dbRefResolver.resolveReference(property,
								new DocumentReferenceSource(documentAccessor.getDocument(), documentAccessor.get(property)),
								referenceLookupDelegate, context::convert));
			}
			return;
		}

		if (value == null) {
			return;
		}

		DBRef dbref = value instanceof DBRef ? (DBRef) value : null;

		accessor.setProperty(property, dbRefResolver.resolveDbRef(property, dbref, callback, handler));
	}

	@Nullable
	private Object readUnwrapped(ConversionContext context, DocumentAccessor documentAccessor,
			MongoPersistentProperty prop, MongoPersistentEntity<?> unwrappedEntity) {

		if (prop.findAnnotation(Unwrapped.class).onEmpty().equals(OnEmpty.USE_EMPTY)) {
			return read(context, unwrappedEntity, (Document) documentAccessor.getDocument());
		}

		for (MongoPersistentProperty persistentProperty : unwrappedEntity) {
			if (documentAccessor.hasValue(persistentProperty)) {
				return read(context, unwrappedEntity, (Document) documentAccessor.getDocument());
			}
		}
		return null;
	}

	public DBRef toDBRef(Object object, @Nullable MongoPersistentProperty referringProperty) {

		org.springframework.data.mongodb.core.mapping.DBRef annotation;

		if (referringProperty != null) {
			annotation = referringProperty.getDBRef();
			Assert.isTrue(annotation != null, "The referenced property has to be mapped with @DBRef!");
		}

		// DATAMONGO-913
		if (object instanceof LazyLoadingProxy) {
			return ((LazyLoadingProxy) object).toDBRef();
		}

		return createDBRef(object, referringProperty);
	}

	@Override
	public DocumentPointer toDocumentPointer(Object source, @Nullable MongoPersistentProperty referringProperty) {

		if (source instanceof LazyLoadingProxy) {
			return () -> ((LazyLoadingProxy) source).getSource();
		}

		Assert.notNull(referringProperty, "Cannot create DocumentReference. The referringProperty must not be null!");

		if (referringProperty.isDbReference()) {
			return () -> toDBRef(source, referringProperty);
		}

		if (referringProperty.isDocumentReference() || referringProperty.findAnnotation(Reference.class) != null) {
			return createDocumentPointer(source, referringProperty);
		}

		throw new IllegalArgumentException("The referringProperty is neither a DBRef nor a document reference");
	}

	DocumentPointer<?> createDocumentPointer(Object source, @Nullable MongoPersistentProperty referringProperty) {

		if (referringProperty == null) {
			return () -> source;
		}

		if (source instanceof DocumentPointer) {
			return (DocumentPointer<?>) source;
		}

		if (ClassUtils.isAssignableValue(referringProperty.getType(), source)
				&& conversionService.canConvert(referringProperty.getType(), DocumentPointer.class)) {
			return conversionService.convert(source, DocumentPointer.class);
		}

		if (ClassUtils.isAssignableValue(referringProperty.getAssociationTargetType(), source)) {
			return documentPointerFactory.computePointer(mappingContext, referringProperty, source,
					referringProperty.getActualType());
		}

		return () -> source;
	}

	/**
	 * Root entry method into write conversion. Adds a type discriminator to the {@link Document}. Shouldn't be called for
	 * nested conversions.
	 *
	 * @see org.springframework.data.mongodb.core.convert.MongoWriter#write(java.lang.Object, java.lang.Object)
	 */
	public void write(Object obj, Bson bson) {

		if (null == obj) {
			return;
		}

		Class<?> entityType = ClassUtils.getUserClass(obj.getClass());
		TypeInformation<? extends Object> type = ClassTypeInformation.from(entityType);

		Object target = obj instanceof LazyLoadingProxy ? ((LazyLoadingProxy) obj).getTarget() : obj;

		writeInternal(target, bson, type);
		BsonUtils.removeNullId(bson);

		if (requiresTypeHint(entityType)) {
			getTypeMapper().writeType(type, bson);
		}
	}

	/**
	 * Check if a given type requires a type hint (aka {@literal _class} attribute) when writing to the document.
	 *
	 * @param type must not be {@literal null}.
	 * @return {@literal true} if not a simple type, {@link Collection} or type with custom write target.
	 */
	private boolean requiresTypeHint(Class<?> type) {

		return !conversions.isSimpleType(type) && !ClassUtils.isAssignable(Collection.class, type)
				&& !conversions.hasCustomWriteTarget(type, Document.class);
	}

	/**
	 * Internal write conversion method which should be used for nested invocations.
	 */
	@SuppressWarnings("unchecked")
	protected void writeInternal(@Nullable Object obj, Bson bson, @Nullable TypeInformation<?> typeHint) {

		if (null == obj) {
			return;
		}

		Class<?> entityType = obj.getClass();
		Optional<Class<?>> customTarget = conversions.getCustomWriteTarget(entityType, Document.class);

		if (customTarget.isPresent()) {
			Document result = doConvert(obj, Document.class);
			BsonUtils.addAllToMap(bson, result);
			return;
		}

		if (Map.class.isAssignableFrom(entityType)) {
			writeMapInternal((Map<Object, Object>) obj, bson, ClassTypeInformation.MAP);
			return;
		}

		if (Collection.class.isAssignableFrom(entityType)) {
			writeCollectionInternal((Collection<?>) obj, ClassTypeInformation.LIST, (Collection<?>) bson);
			return;
		}

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityType);
		writeInternal(obj, bson, entity);
		addCustomTypeKeyIfNecessary(typeHint, obj, bson);
	}

	protected void writeInternal(@Nullable Object obj, Bson bson, @Nullable MongoPersistentEntity<?> entity) {

		if (obj == null) {
			return;
		}

		if (null == entity) {
			throw new MappingException("No mapping metadata found for entity of type " + obj.getClass().getName());
		}

		PersistentPropertyAccessor<?> accessor = entity.getPropertyAccessor(obj);
		DocumentAccessor dbObjectAccessor = new DocumentAccessor(bson);
		MongoPersistentProperty idProperty = entity.getIdProperty();

		if (idProperty != null && !dbObjectAccessor.hasValue(idProperty)) {

			Object value = idMapper.convertId(accessor.getProperty(idProperty), idProperty.getFieldType());

			if (value != null) {
				dbObjectAccessor.put(idProperty, value);
			}
		}

		writeProperties(bson, entity, accessor, dbObjectAccessor, idProperty);
	}

	private void writeProperties(Bson bson, MongoPersistentEntity<?> entity, PersistentPropertyAccessor<?> accessor,
			DocumentAccessor dbObjectAccessor, @Nullable MongoPersistentProperty idProperty) {

		// Write the properties
		for (MongoPersistentProperty prop : entity) {

			if (prop.equals(idProperty) || !prop.isWritable()) {
				continue;
			}
			if (prop.isAssociation()) {

				writeAssociation(prop.getRequiredAssociation(), accessor, dbObjectAccessor);
				continue;
			}

			Object value = accessor.getProperty(prop);

			if (value == null) {
				if (prop.writeNullValues()) {
					dbObjectAccessor.put(prop, null);
				}
			} else if (!conversions.isSimpleType(value.getClass())) {
				writePropertyInternal(value, dbObjectAccessor, prop);
			} else {
				writeSimpleInternal(value, bson, prop);
			}
		}
	}

	private void writeAssociation(Association<MongoPersistentProperty> association,
			PersistentPropertyAccessor<?> accessor, DocumentAccessor dbObjectAccessor) {

		MongoPersistentProperty inverseProp = association.getInverse();

		Object value = accessor.getProperty(inverseProp);

		if (value == null && !inverseProp.isUnwrapped() && inverseProp.writeNullValues()) {
			dbObjectAccessor.put(inverseProp, null);
			return;
		}

		writePropertyInternal(value, dbObjectAccessor, inverseProp);
	}

	@SuppressWarnings({ "unchecked" })
	protected void writePropertyInternal(@Nullable Object obj, DocumentAccessor accessor, MongoPersistentProperty prop) {

		if (obj == null) {
			return;
		}

		TypeInformation<?> valueType = ClassTypeInformation.from(obj.getClass());
		TypeInformation<?> type = prop.getTypeInformation();

		if (prop.isUnwrapped()) {

			Document target = new Document();
			writeInternal(obj, target, mappingContext.getPersistentEntity(prop));

			accessor.putAll(target);
			return;
		}

		if (valueType.isCollectionLike()) {

			List<Object> collectionInternal = createCollection(BsonUtils.asCollection(obj), prop);
			accessor.put(prop, collectionInternal);
			return;
		}

		if (valueType.isMap()) {

			Bson mapDbObj = createMap((Map<Object, Object>) obj, prop);
			accessor.put(prop, mapDbObj);
			return;
		}

		if (prop.isDbReference()) {

			DBRef dbRefObj = null;

			/*
			 * If we already have a LazyLoadingProxy, we use it's cached DBRef value instead of
			 * unnecessarily initializing it only to convert it to a DBRef a few instructions later.
			 */
			if (obj instanceof LazyLoadingProxy) {
				dbRefObj = ((LazyLoadingProxy) obj).toDBRef();
			}

			dbRefObj = dbRefObj != null ? dbRefObj : createDBRef(obj, prop);

			accessor.put(prop, dbRefObj);
			return;
		}

		if (prop.isAssociation() && prop.isAnnotationPresent(Reference.class)) {

			accessor.put(prop, new DocumentPointerFactory(conversionService, mappingContext)
					.computePointer(mappingContext, prop, obj, valueType.getType()).getPointer());
			return;
		}

		/*
		 * If we have a LazyLoadingProxy we make sure it is initialized first.
		 */
		if (obj instanceof LazyLoadingProxy) {
			obj = ((LazyLoadingProxy) obj).getTarget();
		}

		// Lookup potential custom target type
		Optional<Class<?>> basicTargetType = conversions.getCustomWriteTarget(obj.getClass());

		if (basicTargetType.isPresent()) {

			accessor.put(prop, doConvert(obj, basicTargetType.get()));
			return;
		}

		MongoPersistentEntity<?> entity = valueType.isSubTypeOf(prop.getType())
				? mappingContext.getRequiredPersistentEntity(obj.getClass())
				: mappingContext.getRequiredPersistentEntity(type);

		Object existingValue = accessor.get(prop);
		Document document = existingValue instanceof Document ? (Document) existingValue : new Document();

		writeInternal(obj, document, entity);
		addCustomTypeKeyIfNecessary(ClassTypeInformation.from(prop.getRawType()), obj, document);
		accessor.put(prop, document);
	}

	/**
	 * Writes the given {@link Collection} using the given {@link MongoPersistentProperty} information.
	 *
	 * @param collection must not be {@literal null}.
	 * @param property must not be {@literal null}.
	 */
	protected List<Object> createCollection(Collection<?> collection, MongoPersistentProperty property) {

		if (!property.isDbReference()) {

			if (property.isAssociation()) {

				List<Object> targetCollection = collection.stream().map(it -> {
					return documentPointerFactory.computePointer(mappingContext, property, it, property.getActualType())
							.getPointer();
				}).collect(Collectors.toList());

				return writeCollectionInternal(targetCollection, ClassTypeInformation.from(DocumentPointer.class),
						new ArrayList<>());
			}

			if (property.hasExplicitWriteTarget()) {
				return writeCollectionInternal(collection, new FieldTypeInformation<>(property), new ArrayList<>());
			}

			return writeCollectionInternal(collection, property.getTypeInformation(), new ArrayList<>());
		}

		List<Object> dbList = new ArrayList<>(collection.size());

		for (Object element : collection) {

			if (element == null) {
				continue;
			}

			DBRef dbRef = createDBRef(element, property);
			dbList.add(dbRef);
		}

		return dbList;
	}

	/**
	 * Writes the given {@link Map} using the given {@link MongoPersistentProperty} information.
	 *
	 * @param map must not {@literal null}.
	 * @param property must not be {@literal null}.
	 */
	protected Bson createMap(Map<Object, Object> map, MongoPersistentProperty property) {

		Assert.notNull(map, "Given map must not be null!");
		Assert.notNull(property, "PersistentProperty must not be null!");

		if (!property.isAssociation()) {
			return writeMapInternal(map, new Document(), property.getTypeInformation());
		}

		Document document = new Document();

		for (Map.Entry<Object, Object> entry : map.entrySet()) {

			Object key = entry.getKey();
			Object value = entry.getValue();

			if (conversions.isSimpleType(key.getClass())) {

				String simpleKey = prepareMapKey(key.toString());
				if (property.isDbReference()) {
					document.put(simpleKey, value != null ? createDBRef(value, property) : null);
				} else {
					document.put(simpleKey, documentPointerFactory
							.computePointer(mappingContext, property, value, property.getActualType()).getPointer());
				}

			} else {
				throw new MappingException("Cannot use a complex object as a key value.");
			}
		}

		return document;
	}

	/**
	 * Populates the given {@link Collection sink} with converted values from the given {@link Collection source}.
	 *
	 * @param source the collection to create a {@link Collection} for, must not be {@literal null}.
	 * @param type the {@link TypeInformation} to consider or {@literal null} if unknown.
	 * @param sink the {@link Collection} to write to.
	 */
	@SuppressWarnings("unchecked")
	private List<Object> writeCollectionInternal(Collection<?> source, @Nullable TypeInformation<?> type,
			Collection<?> sink) {

		TypeInformation<?> componentType = null;

		List<Object> collection = sink instanceof List ? (List<Object>) sink : new ArrayList<>(sink);

		if (type != null) {
			componentType = type.getComponentType();
		}

		for (Object element : source) {

			Class<?> elementType = element == null ? null : element.getClass();

			if (elementType == null || conversions.isSimpleType(elementType)) {
				collection.add(getPotentiallyConvertedSimpleWrite(element,
						componentType != null ? componentType.getType() : Object.class));
			} else if (element instanceof Collection || elementType.isArray()) {
				collection.add(writeCollectionInternal(BsonUtils.asCollection(element), componentType, new ArrayList<>()));
			} else {
				Document document = new Document();
				writeInternal(element, document, componentType);
				collection.add(document);
			}
		}

		return collection;
	}

	/**
	 * Writes the given {@link Map} to the given {@link Document} considering the given {@link TypeInformation}.
	 *
	 * @param obj must not be {@literal null}.
	 * @param bson must not be {@literal null}.
	 * @param propertyType must not be {@literal null}.
	 */
	protected Bson writeMapInternal(Map<Object, Object> obj, Bson bson, TypeInformation<?> propertyType) {

		for (Map.Entry<Object, Object> entry : obj.entrySet()) {

			Object key = entry.getKey();
			Object val = entry.getValue();

			if (conversions.isSimpleType(key.getClass())) {

				String simpleKey = prepareMapKey(key);
				if (val == null || conversions.isSimpleType(val.getClass())) {
					writeSimpleInternal(val, bson, simpleKey);
				} else if (val instanceof Collection || val.getClass().isArray()) {
					BsonUtils.addToMap(bson, simpleKey,
							writeCollectionInternal(BsonUtils.asCollection(val), propertyType.getMapValueType(), new ArrayList<>()));
				} else {
					Document document = new Document();
					TypeInformation<?> valueTypeInfo = propertyType.isMap() ? propertyType.getMapValueType()
							: ClassTypeInformation.OBJECT;
					writeInternal(val, document, valueTypeInfo);
					BsonUtils.addToMap(bson, simpleKey, document);
				}
			} else {
				throw new MappingException("Cannot use a complex object as a key value.");
			}
		}

		return bson;
	}

	/**
	 * Prepares the given {@link Map} key to be converted into a {@link String}. Will invoke potentially registered custom
	 * conversions and escape dots from the result as they're not supported as {@link Map} key in MongoDB.
	 *
	 * @param key must not be {@literal null}.
	 */
	private String prepareMapKey(Object key) {

		Assert.notNull(key, "Map key must not be null!");

		String convertedKey = potentiallyConvertMapKey(key);
		return potentiallyEscapeMapKey(convertedKey);
	}

	/**
	 * Potentially replaces dots in the given map key with the configured map key replacement if configured or aborts
	 * conversion if none is configured.
	 *
	 * @see #setMapKeyDotReplacement(String)
	 * @param source must not be {@literal null}.
	 */
	protected String potentiallyEscapeMapKey(String source) {

		if (!source.contains(".")) {
			return source;
		}

		if (mapKeyDotReplacement == null) {
			throw new MappingException(String.format(
					"Map key %s contains dots but no replacement was configured! Make "
							+ "sure map keys don't contain dots in the first place or configure an appropriate replacement!",
					source));
		}

		return StringUtils.replace(source, ".", mapKeyDotReplacement);
	}

	/**
	 * Returns a {@link String} representation of the given {@link Map} key
	 *
	 * @param key
	 */
	private String potentiallyConvertMapKey(Object key) {

		if (key instanceof String) {
			return (String) key;
		}

		return conversions.hasCustomWriteTarget(key.getClass(), String.class)
				? (String) getPotentiallyConvertedSimpleWrite(key, Object.class)
				: key.toString();
	}

	/**
	 * Translates the map key replacements in the given key just read with a dot in case a map key replacement has been
	 * configured.
	 *
	 * @param source must not be {@literal null}.
	 */
	protected String potentiallyUnescapeMapKey(String source) {
		return mapKeyDotReplacement == null ? source : StringUtils.replace(source, mapKeyDotReplacement, ".");
	}

	/**
	 * Adds custom type information to the given {@link Document} if necessary. That is if the value is not the same as
	 * the one given. This is usually the case if you store a subtype of the actual declared type of the property.
	 *
	 * @param type can be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param bson must not be {@literal null}.
	 */
	protected void addCustomTypeKeyIfNecessary(@Nullable TypeInformation<?> type, Object value, Bson bson) {

		Class<?> reference = type != null ? type.getRequiredActualType().getType() : Object.class;
		Class<?> valueType = ClassUtils.getUserClass(value.getClass());

		boolean notTheSameClass = !valueType.equals(reference);
		if (notTheSameClass) {
			getTypeMapper().writeType(valueType, bson);
		}
	}

	/**
	 * Writes the given simple value to the given {@link Document}. Will store enum names for enum values.
	 *
	 * @param value can be {@literal null}.
	 * @param bson must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 */
	private void writeSimpleInternal(@Nullable Object value, Bson bson, String key) {
		BsonUtils.addToMap(bson, key, getPotentiallyConvertedSimpleWrite(value, Object.class));
	}

	private void writeSimpleInternal(@Nullable Object value, Bson bson, MongoPersistentProperty property) {
		DocumentAccessor accessor = new DocumentAccessor(bson);
		accessor.put(property, getPotentiallyConvertedSimpleWrite(value,
				property.hasExplicitWriteTarget() ? property.getFieldType() : Object.class));
	}

	/**
	 * Checks whether we have a custom conversion registered for the given value into an arbitrary simple Mongo type.
	 * Returns the converted value if so. If not, we perform special enum handling or simply return the value as is.
	 */
	@Nullable
	private Object getPotentiallyConvertedSimpleWrite(@Nullable Object value, @Nullable Class<?> typeHint) {

		if (value == null) {
			return null;
		}

		if (typeHint != null && Object.class != typeHint) {

			if (conversionService.canConvert(value.getClass(), typeHint)) {
				value = doConvert(value, typeHint);
			}
		}

		Optional<Class<?>> customTarget = conversions.getCustomWriteTarget(value.getClass());

		if (customTarget.isPresent()) {
			return doConvert(value, customTarget.get());
		}

		if (ObjectUtils.isArray(value)) {

			if (value instanceof byte[]) {
				return value;
			}
			return BsonUtils.asCollection(value);
		}

		return Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value;
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is. Can be overridden by subclasses.
	 *
	 * @since 3.2
	 */
	protected Object getPotentiallyConvertedSimpleRead(Object value, TypeInformation<?> target) {
		return getPotentiallyConvertedSimpleRead(value, target.getType());
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object getPotentiallyConvertedSimpleRead(Object value, @Nullable Class<?> target) {

		if (target == null) {
			return value;
		}

		if (conversions.hasCustomReadTarget(value.getClass(), target)) {
			return doConvert(value, target);
		}

		if (ClassUtils.isAssignableValue(target, value)) {
			return value;
		}

		if (Enum.class.isAssignableFrom(target)) {
			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return doConvert(value, target);
	}

	protected DBRef createDBRef(Object target, @Nullable MongoPersistentProperty property) {

		Assert.notNull(target, "Target object must not be null!");

		if (target instanceof DBRef) {
			return (DBRef) target;
		}

		MongoPersistentEntity<?> targetEntity = mappingContext.getPersistentEntity(target.getClass());
		targetEntity = targetEntity != null ? targetEntity : mappingContext.getPersistentEntity(property);

		if (null == targetEntity) {
			throw new MappingException("No mapping metadata found for " + target.getClass());
		}

		MongoPersistentEntity<?> entity = targetEntity;

		MongoPersistentProperty idProperty = entity.getIdProperty();

		if (idProperty != null) {

			Object id = target.getClass().equals(idProperty.getType()) ? target
					: entity.getPropertyAccessor(target).getProperty(idProperty);

			if (null == id) {
				throw new MappingException("Cannot create a reference to an object with a NULL id.");
			}

			return dbRefResolver.createDbRef(property == null ? null : property.getDBRef(), entity,
					idMapper.convertId(id, idProperty != null ? idProperty.getFieldType() : ObjectId.class));
		}

		throw new MappingException("No id property found on class " + entity.getType());
	}

	@Nullable
	private Object getValueInternal(ConversionContext context, MongoPersistentProperty prop, Bson bson,
			SpELExpressionEvaluator evaluator) {
		return new MongoDbPropertyValueProvider(context, bson, evaluator).getPropertyValue(prop);
	}

	/**
	 * Reads the given {@link Collection} into a collection of the given {@link TypeInformation}. Can be overridden by
	 * subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param source must not be {@literal null}
	 * @param targetType the {@link Map} {@link TypeInformation} to be used to unmarshall this {@link Document}.
	 * @since 3.2
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	protected Object readCollectionOrArray(ConversionContext context, Collection<?> source,
			TypeInformation<?> targetType) {

		Assert.notNull(targetType, "Target type must not be null!");

		Class<?> collectionType = targetType.isSubTypeOf(Collection.class) //
				? targetType.getType() //
				: List.class;

		TypeInformation<?> componentType = targetType.getComponentType() != null //
				? targetType.getComponentType() //
				: ClassTypeInformation.OBJECT;
		Class<?> rawComponentType = componentType.getType();

		Collection<Object> items = targetType.getType().isArray() //
				? new ArrayList<>(source.size()) //
				: CollectionFactory.createCollection(collectionType, rawComponentType, source.size());

		if (source.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(items, targetType.getType());
		}

		if (!DBRef.class.equals(rawComponentType) && isCollectionOfDbRefWhereBulkFetchIsPossible(source)) {

			List<Object> objects = bulkReadAndConvertDBRefs(context, (List<DBRef>) source, componentType);
			return getPotentiallyConvertedSimpleRead(objects, targetType.getType());
		}

		for (Object element : source) {
			items.add(element != null ? context.convert(element, componentType) : element);
		}

		return getPotentiallyConvertedSimpleRead(items, targetType.getType());
	}

	/**
	 * Reads the given {@link Document} into a {@link Map}. will recursively resolve nested {@link Map}s as well.
	 *
	 * @param type the {@link Map} {@link TypeInformation} to be used to unmarshall this {@link Document}.
	 * @param bson must not be {@literal null}
	 * @param path must not be {@literal null}
	 * @return
	 * @deprecated since 3.2. Use {@link #readMap(ConversionContext, Bson, TypeInformation)} instead.
	 */
	@Deprecated
	protected Map<Object, Object> readMap(TypeInformation<?> type, Bson bson, ObjectPath path) {
		return readMap(getConversionContext(path), bson, type);
	}

	/**
	 * Reads the given {@link Document} into a {@link Map}. will recursively resolve nested {@link Map}s as well. Can be
	 * overridden by subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param bson must not be {@literal null}
	 * @param targetType the {@link Map} {@link TypeInformation} to be used to unmarshall this {@link Document}.
	 * @return the converted {@link Map}, will never be {@literal null}.
	 * @since 3.2
	 */
	protected Map<Object, Object> readMap(ConversionContext context, Bson bson, TypeInformation<?> targetType) {

		Assert.notNull(bson, "Document must not be null!");
		Assert.notNull(targetType, "TypeInformation must not be null!");

		Class<?> mapType = getTypeMapper().readType(bson, targetType).getType();

		TypeInformation<?> keyType = targetType.getComponentType();
		TypeInformation<?> valueType = targetType.getMapValueType() == null ? ClassTypeInformation.OBJECT
				: targetType.getRequiredMapValueType();

		Class<?> rawKeyType = keyType != null ? keyType.getType() : Object.class;
		Class<?> rawValueType = valueType.getType();

		Map<String, Object> sourceMap = BsonUtils.asMap(bson);
		Map<Object, Object> map = CollectionFactory.createMap(mapType, rawKeyType, sourceMap.keySet().size());

		if (!DBRef.class.equals(rawValueType) && isCollectionOfDbRefWhereBulkFetchIsPossible(sourceMap.values())) {
			bulkReadAndConvertDBRefMapIntoTarget(context, valueType, sourceMap, map);
			return map;
		}

		sourceMap.forEach((k, v) -> {

			if (getTypeMapper().isTypeKey(k)) {
				return;
			}

			Object key = potentiallyUnescapeMapKey(k);

			if (!rawKeyType.isAssignableFrom(key.getClass())) {
				key = doConvert(key, rawKeyType);
			}

			Object value = v;
			map.put(key, value == null ? value : context.convert(value, valueType));

		});

		return map;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	@Override
	public Object convertToMongoType(@Nullable Object obj, @Nullable TypeInformation<?> typeInformation) {

		if (obj == null) {
			return null;
		}

		Optional<Class<?>> target = conversions.getCustomWriteTarget(obj.getClass());
		if (target.isPresent()) {
			return doConvert(obj, target.get());
		}

		if (conversions.isSimpleType(obj.getClass())) {

			Class<?> conversionTargetType;

			if (typeInformation != null && conversions.isSimpleType(typeInformation.getType())) {
				conversionTargetType = typeInformation.getType();
			} else {
				conversionTargetType = Object.class;
			}

			return getPotentiallyConvertedSimpleWrite(obj, conversionTargetType);
		}

		if (obj instanceof List) {
			return maybeConvertList((List<Object>) obj, typeInformation);
		}

		if (obj instanceof Document) {

			Document newValueDocument = new Document();
			for (String vk : ((Document) obj).keySet()) {
				Object o = ((Document) obj).get(vk);
				newValueDocument.put(vk, convertToMongoType(o, typeInformation));
			}
			return newValueDocument;
		}

		if (obj instanceof DBObject) {

			Document newValueDbo = new Document();
			for (String vk : ((DBObject) obj).keySet()) {

				Object o = ((DBObject) obj).get(vk);
				newValueDbo.put(vk, convertToMongoType(o, typeInformation));
			}

			return newValueDbo;
		}

		if (obj instanceof Map) {

			Document result = new Document();

			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) obj).entrySet()) {
				result.put(entry.getKey().toString(), convertToMongoType(entry.getValue(), typeInformation));
			}

			return result;
		}

		if (obj.getClass().isArray()) {
			return maybeConvertList(Arrays.asList((Object[]) obj), typeInformation);
		}

		if (obj instanceof Collection) {
			return maybeConvertList((Collection<?>) obj, typeInformation);
		}

		Document newDocument = new Document();
		this.write(obj, newDocument);

		if (typeInformation == null) {
			return removeTypeInfo(newDocument, true);
		}

		if (typeInformation.getType().equals(NestedDocument.class)) {
			return removeTypeInfo(newDocument, false);
		}

		return !obj.getClass().equals(typeInformation.getType()) ? newDocument : removeTypeInfo(newDocument, true);
	}

	@Override
	public Object convertToMongoType(@Nullable Object obj, MongoPersistentEntity entity) {
		Document newDocument = new Document();
		writeInternal(obj, newDocument, entity);
		return newDocument;
	}

	// TODO: hide in 4.0
	public List<Object> maybeConvertList(Iterable<?> source, @Nullable TypeInformation<?> typeInformation) {

		List<Object> newDbl = new ArrayList<>();

		for (Object element : source) {
			newDbl.add(convertToMongoType(element, typeInformation));
		}

		return newDbl;
	}

	/**
	 * Removes the type information from the entire conversion result.
	 *
	 * @param object
	 * @param recursively whether to apply the removal recursively
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Object removeTypeInfo(Object object, boolean recursively) {

		if (!(object instanceof Document)) {
			return object;
		}

		Document document = (Document) object;
		String keyToRemove = null;

		for (String key : document.keySet()) {

			if (recursively) {

				Object value = document.get(key);

				if (value instanceof BasicDBList) {
					for (Object element : (BasicDBList) value) {
						removeTypeInfo(element, recursively);
					}
				} else if (value instanceof List) {
					for (Object element : (List<Object>) value) {
						removeTypeInfo(element, recursively);
					}
				} else {
					removeTypeInfo(value, recursively);
				}
			}

			if (getTypeMapper().isTypeKey(key)) {

				keyToRemove = key;

				if (!recursively) {
					break;
				}
			}
		}

		if (keyToRemove != null) {
			document.remove(keyToRemove);
		}

		return document;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	<T> T readValue(ConversionContext context, @Nullable Object value, TypeInformation<?> type) {

		if (value == null) {
			return null;
		}

		Assert.notNull(type, "TypeInformation must not be null");

		Class<?> rawType = type.getType();

		if (conversions.hasCustomReadTarget(value.getClass(), rawType)) {
			return (T) doConvert(value, rawType);
		} else if (value instanceof DBRef) {
			return (T) readDBRef(context, (DBRef) value, type);
		}

		return (T) context.convert(value, type);
	}

	@Nullable
	private Object readDBRef(ConversionContext context, @Nullable DBRef dbref, TypeInformation<?> type) {

		if (type.getType().equals(DBRef.class)) {
			return dbref;
		}

		ObjectPath path = context.getPath();

		Object object = dbref == null ? null : path.getPathItem(dbref.getId(), dbref.getCollectionName(), type.getType());
		if (object != null) {
			return object;
		}

		List<Object> result = bulkReadAndConvertDBRefs(context, Collections.singletonList(dbref), type);
		return CollectionUtils.isEmpty(result) ? null : result.iterator().next();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void bulkReadAndConvertDBRefMapIntoTarget(ConversionContext context, TypeInformation<?> valueType,
			Map<String, Object> sourceMap, Map<Object, Object> targetMap) {

		LinkedHashMap<String, Object> referenceMap = new LinkedHashMap<>(sourceMap);
		List<Object> convertedObjects = bulkReadAndConvertDBRefs(context.withPath(ObjectPath.ROOT),
				(List<DBRef>) new ArrayList(referenceMap.values()), valueType);
		int index = 0;

		for (String key : referenceMap.keySet()) {
			targetMap.put(key, convertedObjects.get(index));
			index++;
		}
	}

	@SuppressWarnings("unchecked")
	private <T> List<T> bulkReadAndConvertDBRefs(ConversionContext context, List<DBRef> dbrefs, TypeInformation<?> type) {

		if (CollectionUtils.isEmpty(dbrefs)) {
			return Collections.emptyList();
		}

		List<Document> referencedRawDocuments = dbrefs.size() == 1
				? Collections.singletonList(readRef(dbrefs.iterator().next()))
				: bulkReadRefs(dbrefs);
		String collectionName = dbrefs.iterator().next().getCollectionName();

		List<T> targetList = new ArrayList<>(dbrefs.size());

		for (Document document : referencedRawDocuments) {

			T target = null;
			if (document != null) {

				maybeEmitEvent(new AfterLoadEvent<>(document, (Class<T>) type.getType(), collectionName));
				target = (T) readDocument(context, document, type);
			}

			if (target != null) {
				maybeEmitEvent(new AfterConvertEvent<>(document, target, collectionName));
				target = maybeCallAfterConvert(target, document, collectionName);
			}

			targetList.add(target);
		}

		return targetList;
	}

	private void maybeEmitEvent(MongoMappingEvent<?> event) {

		if (canPublishEvent()) {
			this.applicationContext.publishEvent(event);
		}
	}

	private boolean canPublishEvent() {
		return this.applicationContext != null;
	}

	protected <T> T maybeCallAfterConvert(T object, Document document, String collection) {

		if (null != entityCallbacks) {
			return entityCallbacks.callback(AfterConvertCallback.class, object, document, collection);
		}

		return object;
	}

	/**
	 * Performs the fetch operation for the given {@link DBRef}.
	 *
	 * @param ref
	 * @return
	 */
	@Nullable
	Document readRef(DBRef ref) {
		return dbRefResolver.fetch(ref);
	}

	/**
	 * Performs a bulk fetch operation for the given {@link DBRef}s.
	 *
	 * @param references must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	List<Document> bulkReadRefs(List<DBRef> references) {
		return dbRefResolver.bulkFetch(references);
	}

	/**
	 * Get the conversion target type if defined or return the {@literal source}.
	 *
	 * @param source must not be {@literal null}.
	 * @return
	 * @since 2.2
	 */
	public Class<?> getWriteTarget(Class<?> source) {
		return conversions.getCustomWriteTarget(source).orElse(source);
	}

	/**
	 * Create a new {@link MappingMongoConverter} using the given {@link MongoDatabaseFactory} when loading {@link DBRef}.
	 *
	 * @return new instance of {@link MappingMongoConverter}. Never {@literal null}.
	 * @since 2.1.6
	 */
	public MappingMongoConverter with(MongoDatabaseFactory dbFactory) {

		MappingMongoConverter target = new MappingMongoConverter(new DefaultDbRefResolver(dbFactory), mappingContext);
		target.applicationContext = applicationContext;
		target.conversions = conversions;
		target.spELContext = spELContext;
		target.setInstantiators(instantiators);
		target.defaultTypeMapper = defaultTypeMapper;
		target.typeMapper = typeMapper;
		target.setCodecRegistryProvider(dbFactory);
		target.afterPropertiesSet();

		return target;
	}

	private <T extends Object> T doConvert(Object value, Class<? extends T> target) {
		return doConvert(value, target, null);
	}

	@SuppressWarnings("ConstantConditions")
	private <T extends Object> T doConvert(Object value, Class<? extends T> target,
			@Nullable Class<? extends T> fallback) {

		if (conversionService.canConvert(value.getClass(), target) || fallback == null) {
			return conversionService.convert(value, target);
		}
		return conversionService.convert(value, fallback);
	}

	/**
	 * Returns whether the given {@link Iterable} contains {@link DBRef} instances all pointing to the same collection.
	 *
	 * @param source must not be {@literal null}.
	 * @return
	 */
	private static boolean isCollectionOfDbRefWhereBulkFetchIsPossible(Iterable<?> source) {

		Assert.notNull(source, "Iterable of DBRefs must not be null!");

		Set<String> collectionsFound = new HashSet<>();

		for (Object dbObjItem : source) {

			if (!(dbObjItem instanceof DBRef)) {
				return false;
			}

			collectionsFound.add(((DBRef) dbObjItem).getCollectionName());

			if (collectionsFound.size() > 1) {
				return false;
			}
		}

		return true;
	}

	static Predicate<MongoPersistentProperty> isIdentifier(PersistentEntity<?, ?> entity) {
		return entity::isIdProperty;
	}

	static Predicate<MongoPersistentProperty> isConstructorArgument(PersistentEntity<?, ?> entity) {
		return entity::isConstructorArgument;
	}

	/**
	 * {@link PropertyValueProvider} to evaluate a SpEL expression if present on the property or simply accesses the field
	 * of the configured source {@link Document}.
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	static class MongoDbPropertyValueProvider implements PropertyValueProvider<MongoPersistentProperty> {

		final ConversionContext context;
		final DocumentAccessor accessor;
		final SpELExpressionEvaluator evaluator;

		/**
		 * Creates a new {@link MongoDbPropertyValueProvider} for the given source, {@link SpELExpressionEvaluator} and
		 * {@link ObjectPath}.
		 *
		 * @param context must not be {@literal null}.
		 * @param source must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 */
		MongoDbPropertyValueProvider(ConversionContext context, Bson source, SpELExpressionEvaluator evaluator) {
			this(context, new DocumentAccessor(source), evaluator);
		}

		/**
		 * Creates a new {@link MongoDbPropertyValueProvider} for the given source, {@link SpELExpressionEvaluator} and
		 * {@link ObjectPath}.
		 *
		 * @param context must not be {@literal null}.
		 * @param accessor must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 */
		MongoDbPropertyValueProvider(ConversionContext context, DocumentAccessor accessor,
				SpELExpressionEvaluator evaluator) {

			Assert.notNull(context, "ConversionContext must no be null!");
			Assert.notNull(accessor, "DocumentAccessor must no be null!");
			Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null!");

			this.context = context;
			this.accessor = accessor;
			this.evaluator = evaluator;
		}

		@Nullable
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(MongoPersistentProperty property) {

			String expression = property.getSpelExpression();
			Object value = expression != null ? evaluator.evaluate(expression) : accessor.get(property);

			if (value == null) {
				return null;
			}

			return (T) context.convert(value, property.getTypeInformation());
		}

		public MongoDbPropertyValueProvider withContext(ConversionContext context) {
			if (context == this.context) {
				return this;
			}

			return new MongoDbPropertyValueProvider(context, accessor, evaluator);

		}
	}

	/**
	 * {@link PropertyValueProvider} that is aware of {@link MongoPersistentProperty#isAssociation()} and that delegates
	 * resolution to {@link DbRefResolver}.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	class AssociationAwareMongoDbPropertyValueProvider extends MongoDbPropertyValueProvider {

		/**
		 * Creates a new {@link AssociationAwareMongoDbPropertyValueProvider} for the given source,
		 * {@link SpELExpressionEvaluator} and {@link ObjectPath}.
		 *
		 * @param source must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 */
		AssociationAwareMongoDbPropertyValueProvider(ConversionContext context, DocumentAccessor source,
				SpELExpressionEvaluator evaluator) {
			super(context, source, evaluator);
		}

		@Nullable
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(MongoPersistentProperty property) {

			if (property.isDbReference() && property.getDBRef().lazy()) {

				Object rawRefValue = accessor.get(property);
				if (rawRefValue == null) {
					return null;
				}

				DbRefResolverCallback callback = new DefaultDbRefResolverCallback(accessor.getDocument(), context.getPath(),
						evaluator, (prop, bson, evaluator, path) -> MappingMongoConverter.this.getValueInternal(context, prop, bson,
								evaluator));

				DBRef dbref = rawRefValue instanceof DBRef ? (DBRef) rawRefValue : null;
				return (T) dbRefResolver.resolveDbRef(property, dbref, callback, dbRefProxyHandler);
			}

			if (property.isDocumentReference()) {
				return (T) dbRefResolver.resolveReference(property, accessor.get(property), referenceLookupDelegate,
						context::convert);
			}

			return super.getPropertyValue(property);
		}
	}

	/**
	 * Extension of {@link SpELExpressionParameterValueProvider} to recursively trigger value conversion on the raw
	 * resolved SpEL value.
	 *
	 * @author Oliver Gierke
	 */
	private static class ConverterAwareSpELExpressionParameterValueProvider
			extends SpELExpressionParameterValueProvider<MongoPersistentProperty> {

		private final ConversionContext context;

		/**
		 * Creates a new {@link ConverterAwareSpELExpressionParameterValueProvider}.
		 *
		 * @param context must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 * @param conversionService must not be {@literal null}.
		 * @param delegate must not be {@literal null}.
		 */
		public ConverterAwareSpELExpressionParameterValueProvider(ConversionContext context,
				SpELExpressionEvaluator evaluator, ConversionService conversionService,
				ParameterValueProvider<MongoPersistentProperty> delegate) {

			super(evaluator, conversionService, delegate);

			Assert.notNull(context, "ConversionContext must no be null!");

			this.context = context;
		}

		@Override
		protected <T> T potentiallyConvertSpelValue(Object object, Parameter<T, MongoPersistentProperty> parameter) {
			return context.convert(object, parameter.getType());
		}
	}

	/**
	 * Marker class used to indicate we have a non root document object here that might be used within an update - so we
	 * need to preserve type hints for potential nested elements but need to remove it on top level.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	static class NestedDocument {

	}

	enum NoOpParameterValueProvider implements ParameterValueProvider<MongoPersistentProperty> {

		INSTANCE;

		@Override
		public <T> T getParameterValue(Parameter<T, MongoPersistentProperty> parameter) {
			return null;
		}
	}

	/**
	 * {@link TypeInformation} considering {@link MongoPersistentProperty#getFieldType()} as type source.
	 *
	 * @param <S>
	 */
	private static class FieldTypeInformation<S> implements TypeInformation<S> {

		private final MongoPersistentProperty persistentProperty;
		private final TypeInformation<S> delegate;

		@SuppressWarnings("unchecked")
		public FieldTypeInformation(MongoPersistentProperty property) {

			this.persistentProperty = property;
			this.delegate = (TypeInformation<S>) property.getTypeInformation();
		}

		@Override
		public List<org.springframework.data.util.TypeInformation<?>> getParameterTypes(Constructor constructor) {
			return persistentProperty.getTypeInformation().getParameterTypes(constructor);
		}

		@Override
		public org.springframework.data.util.TypeInformation<?> getProperty(String property) {
			return delegate.getProperty(property);
		}

		@Override
		public boolean isCollectionLike() {
			return delegate.isCollectionLike();
		}

		@Override
		public org.springframework.data.util.TypeInformation<?> getComponentType() {
			return ClassTypeInformation.from(persistentProperty.getFieldType());
		}

		@Override
		public boolean isMap() {
			return delegate.isMap();
		}

		@Override
		public org.springframework.data.util.TypeInformation<?> getMapValueType() {
			return ClassTypeInformation.from(persistentProperty.getFieldType());
		}

		@Override
		public Class<S> getType() {
			return delegate.getType();
		}

		@Override
		public ClassTypeInformation<?> getRawTypeInformation() {
			return delegate.getRawTypeInformation();
		}

		@Override
		public org.springframework.data.util.TypeInformation<?> getActualType() {
			return delegate.getActualType();
		}

		@Override
		public org.springframework.data.util.TypeInformation<?> getReturnType(Method method) {
			return delegate.getReturnType(method);
		}

		@Override
		public List<org.springframework.data.util.TypeInformation<?>> getParameterTypes(Method method) {
			return delegate.getParameterTypes(method);
		}

		@Override
		public org.springframework.data.util.TypeInformation<?> getSuperTypeInformation(Class superType) {
			return delegate.getSuperTypeInformation(superType);
		}

		@Override
		public boolean isAssignableFrom(org.springframework.data.util.TypeInformation target) {
			return delegate.isAssignableFrom(target);
		}

		@Override
		public List<org.springframework.data.util.TypeInformation<?>> getTypeArguments() {
			return delegate.getTypeArguments();
		}

		@Override
		public org.springframework.data.util.TypeInformation<? extends S> specialize(ClassTypeInformation type) {
			return delegate.specialize(type);
		}
	}

	/**
	 * Conversion context holding references to simple {@link ValueConverter} and {@link ContainerValueConverter}.
	 * Entrypoint for recursive conversion of {@link Document} and other types.
	 *
	 * @since 3.2
	 */
	protected static class ConversionContext {

		final org.springframework.data.convert.CustomConversions conversions;
		final ObjectPath path;
		final ContainerValueConverter<Bson> documentConverter;
		final ContainerValueConverter<Collection<?>> collectionConverter;
		final ContainerValueConverter<Bson> mapConverter;
		final ContainerValueConverter<DBRef> dbRefConverter;
		final ValueConverter<Object> elementConverter;

		ConversionContext(org.springframework.data.convert.CustomConversions customConversions, ObjectPath path,
				ContainerValueConverter<Bson> documentConverter, ContainerValueConverter<Collection<?>> collectionConverter,
				ContainerValueConverter<Bson> mapConverter, ContainerValueConverter<DBRef> dbRefConverter,
				ValueConverter<Object> elementConverter) {

			this.conversions = customConversions;
			this.path = path;
			this.documentConverter = documentConverter;
			this.collectionConverter = collectionConverter;
			this.mapConverter = mapConverter;
			this.dbRefConverter = dbRefConverter;
			this.elementConverter = elementConverter;
		}

		/**
		 * Converts a source object into {@link TypeInformation target}.
		 *
		 * @param source must not be {@literal null}.
		 * @param typeHint must not be {@literal null}.
		 * @return the converted object.
		 */
		@SuppressWarnings("unchecked")
		public <S extends Object> S convert(Object source, TypeInformation<? extends S> typeHint) {

			Assert.notNull(source, "Source must not be null");
			Assert.notNull(typeHint, "TypeInformation must not be null");

			if (conversions.hasCustomReadTarget(source.getClass(), typeHint.getType())) {
				return (S) elementConverter.convert(source, typeHint);
			}

			if (source instanceof Collection) {

				Class<?> rawType = typeHint.getType();
				if (!Object.class.equals(rawType)) {
					if (!rawType.isArray() && !ClassUtils.isAssignable(Iterable.class, rawType)) {
						throw new MappingException(
								String.format(INCOMPATIBLE_TYPES, source, source.getClass(), rawType, getPath()));
					}
				}

				if (typeHint.isCollectionLike() || typeHint.getType().isAssignableFrom(Collection.class)) {
					return (S) collectionConverter.convert(this, (Collection<?>) source, typeHint);
				}
			}

			if (typeHint.isMap()) {

				if (ClassUtils.isAssignable(Document.class, typeHint.getType())) {
					return (S) documentConverter.convert(this, BsonUtils.asBson(source), typeHint);
				}

				if (BsonUtils.supportsBson(source)) {
					return (S) mapConverter.convert(this, BsonUtils.asBson(source), typeHint);
				}

				throw new IllegalArgumentException(
						String.format("Expected map like structure but found %s", source.getClass()));
			}

			if (source instanceof DBRef) {
				return (S) dbRefConverter.convert(this, (DBRef) source, typeHint);
			}

			if (source instanceof Collection) {
				throw new MappingException(
						String.format(INCOMPATIBLE_TYPES, source, BasicDBList.class, typeHint.getType(), getPath()));
			}

			if (BsonUtils.supportsBson(source)) {
				return (S) documentConverter.convert(this, BsonUtils.asBson(source), typeHint);
			}

			return (S) elementConverter.convert(source, typeHint);
		}

		/**
		 * Create a new {@link ConversionContext} with {@link ObjectPath currentPath} applied.
		 *
		 * @param currentPath must not be {@literal null}.
		 * @return a new {@link ConversionContext} with {@link ObjectPath currentPath} applied.
		 */
		public ConversionContext withPath(ObjectPath currentPath) {

			Assert.notNull(currentPath, "ObjectPath must not be null");

			return new ConversionContext(conversions, currentPath, documentConverter, collectionConverter, mapConverter,
					dbRefConverter, elementConverter);
		}

		public ObjectPath getPath() {
			return path;
		}

		public ConversionContext forProperty(String name) {
			return this;
		}

		/**
		 * Converts a simple {@code source} value into {@link TypeInformation the target type}.
		 *
		 * @param <T>
		 */
		interface ValueConverter<T> {

			Object convert(T source, TypeInformation<?> typeHint);

		}

		/**
		 * Converts a container {@code source} value into {@link TypeInformation the target type}. Containers may
		 * recursively apply conversions for entities, collections, maps, etc.
		 *
		 * @param <T>
		 */
		interface ContainerValueConverter<T> {

			Object convert(ConversionContext context, T source, TypeInformation<?> typeHint);

		}

	}

	private static class PropertyTranslatingPropertyAccessor<T> implements PersistentPropertyPathAccessor<T> {

		private final PersistentPropertyAccessor<T> delegate;
		private final PersistentPropertyTranslator propertyTranslator;

		private PropertyTranslatingPropertyAccessor(PersistentPropertyAccessor<T> delegate,
				PersistentPropertyTranslator propertyTranslator) {
			this.delegate = delegate;
			this.propertyTranslator = propertyTranslator;
		}

		static <T> PersistentPropertyAccessor<T> create(PersistentPropertyAccessor<T> delegate,
				PersistentPropertyTranslator propertyTranslator) {
			return new PropertyTranslatingPropertyAccessor<>(delegate, propertyTranslator);
		}

		@Override
		public void setProperty(PersistentProperty property, @Nullable Object value) {
			delegate.setProperty(translate(property), value);
		}

		@Override
		public Object getProperty(PersistentProperty<?> property) {
			return delegate.getProperty(translate(property));
		}

		@Override
		public T getBean() {
			return delegate.getBean();
		}

		@Override
		public void setProperty(PersistentPropertyPath<? extends PersistentProperty<?>> path, Object value,
				AccessOptions.SetOptions options) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object getProperty(PersistentPropertyPath<? extends PersistentProperty<?>> path,
				AccessOptions.GetOptions context) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setProperty(PersistentPropertyPath<? extends PersistentProperty<?>> path, Object value) {
			throw new UnsupportedOperationException();
		}

		private MongoPersistentProperty translate(PersistentProperty<?> property) {
			return propertyTranslator.translate((MongoPersistentProperty) property);
		}
	}

}
