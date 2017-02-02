/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionParameterValueProvider;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverters.ObjectIdToBigIntegerConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.event.AfterConvertEvent;
import org.springframework.data.mongodb.core.mapping.event.AfterLoadEvent;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

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
 */
public class MappingMongoConverter extends AbstractMongoConverter implements ApplicationContextAware, ValueResolver {

	private static final String INCOMPATIBLE_TYPES = "Cannot convert %1$s of type %2$s into an instance of %3$s! Implement a custom Converter<%2$s, %3$s> and register it with the CustomConversions. Parent object was: %4$s";

	protected static final Logger LOGGER = LoggerFactory.getLogger(MappingMongoConverter.class);

	protected final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	protected final SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
	protected final QueryMapper idMapper;
	protected final DbRefResolver dbRefResolver;

	protected ApplicationContext applicationContext;
	protected MongoTypeMapper typeMapper;
	protected String mapKeyDotReplacement = null;

	private SpELContext spELContext;

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
		this.typeMapper = new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, mappingContext);
		this.idMapper = new QueryMapper(this);

		this.spELContext = new SpELContext(DocumentPropertyAccessor.INSTANCE);
	}

	/**
	 * Creates a new {@link MappingMongoConverter} given the new {@link MongoDbFactory} and {@link MappingContext}.
	 * 
	 * @deprecated use the constructor taking a {@link DbRefResolver} instead.
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	@Deprecated
	public MappingMongoConverter(MongoDbFactory mongoDbFactory,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		this(new DefaultDbRefResolver(mongoDbFactory), mappingContext);
	}

	/**
	 * Configures the {@link MongoTypeMapper} to be used to add type information to {@link Document}s created by the
	 * converter and how to lookup type information from {@link Document}s when reading them. Uses a
	 * {@link DefaultMongoTypeMapper} by default. Setting this to {@literal null} will reset the {@link TypeMapper} to the
	 * default one.
	 * 
	 * @param typeMapper the typeMapper to set
	 */
	public void setTypeMapper(MongoTypeMapper typeMapper) {
		this.typeMapper = typeMapper == null
				? new DefaultMongoTypeMapper(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, mappingContext) : typeMapper;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.MongoConverter#getTypeMapper()
	 */
	@Override
	public MongoTypeMapper getTypeMapper() {
		return this.typeMapper;
	}

	/**
	 * Configure the characters dots potentially contained in a {@link Map} shall be replaced with. By default we don't do
	 * any translation but rather reject a {@link Map} with keys containing dots causing the conversion for the entire
	 * object to fail. If further customization of the translation is needed, have a look at
	 * {@link #potentiallyEscapeMapKey(String)} as well as {@link #potentiallyUnescapeMapKey(String)}.
	 * 
	 * @param mapKeyDotReplacement the mapKeyDotReplacement to set
	 */
	public void setMapKeyDotReplacement(String mapKeyDotReplacement) {
		this.mapKeyDotReplacement = mapKeyDotReplacement;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	public MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		this.applicationContext = applicationContext;
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.MongoReader#read(java.lang.Class, com.mongodb.Document)
	 */
	public <S extends Object> S read(Class<S> clazz, final Bson bson) {
		return read(ClassTypeInformation.from(clazz), bson);
	}

	protected <S extends Object> S read(TypeInformation<S> type, Bson bson) {
		return read(type, bson, ObjectPath.ROOT);
	}

	@SuppressWarnings("unchecked")
	private <S extends Object> S read(TypeInformation<S> type, Bson bson, ObjectPath path) {

		if (null == bson) {
			return null;
		}

		TypeInformation<? extends S> typeToUse = typeMapper.readType(bson, type);
		Class<? extends S> rawType = typeToUse.getType();

		if (conversions.hasCustomReadTarget(bson.getClass(), rawType)) {
			return conversionService.convert(bson, rawType);
		}

		if (DBObject.class.isAssignableFrom(rawType)) {
			return (S) bson;
		}

		if (Document.class.isAssignableFrom(rawType)) {
			return (S) bson;
		}

		if (typeToUse.isCollectionLike() && bson instanceof List) {
			return (S) readCollectionOrArray(typeToUse, (List<?>) bson, path);
		}

		if (typeToUse.isMap()) {
			return (S) readMap(typeToUse, bson, path);
		}

		if (bson instanceof Collection) {
			throw new MappingException(String.format(INCOMPATIBLE_TYPES, bson, BasicDBList.class, typeToUse.getType(), path));
		}

		if (typeToUse.equals(ClassTypeInformation.OBJECT)) {
			return (S) bson;
		}
		// Retrieve persistent entity info

		Document target = bson instanceof BasicDBObject ? new Document((BasicDBObject)bson) : (Document) bson;

		return read((MongoPersistentEntity<S>) mappingContext.getRequiredPersistentEntity(typeToUse), target,
				path);
	}

	private ParameterValueProvider<MongoPersistentProperty> getParameterProvider(MongoPersistentEntity<?> entity,
			Bson source, DefaultSpELExpressionEvaluator evaluator, ObjectPath path) {

		MongoDbPropertyValueProvider provider = new MongoDbPropertyValueProvider(source, evaluator, path);
		PersistentEntityParameterValueProvider<MongoPersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<MongoPersistentProperty>(
				entity, provider, path.getCurrentObject());

		return new ConverterAwareSpELExpressionParameterValueProvider(evaluator, conversionService, parameterProvider,
				path);
	}

	private <S extends Object> S read(final MongoPersistentEntity<S> entity, final Document bson, final ObjectPath path) {

		final DefaultSpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(bson, spELContext);

		ParameterValueProvider<MongoPersistentProperty> provider = getParameterProvider(entity, bson, evaluator, path);
		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, provider);

		final PersistentPropertyAccessor accessor = new ConvertingPropertyAccessor(entity.getPropertyAccessor(instance),
				conversionService);

		final Optional<MongoPersistentProperty> idProperty = entity.getIdProperty();
		final S result = instance;
		DocumentAccessor documentAccessor = new DocumentAccessor(bson);

		// make sure id property is set before all other properties
		Optional<Object> idValue = idProperty.filter(it -> documentAccessor.hasValue(it)).map(it -> {

			Optional<Object> value = getValueInternal(it, bson, evaluator, path);
			accessor.setProperty(it, value);

			return value;
		});

		final ObjectPath currentPath = path.push(result, entity,
				idValue.isPresent() ? idProperty.map(it -> bson.get(it.getFieldName())).orElse(null) : null);

		// Set properties not already set in the constructor
		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {
			public void doWithPersistentProperty(MongoPersistentProperty prop) {

				// we skip the id property since it was already set
				if (idProperty != null && idProperty.equals(prop)) {
					return;
				}

				if (entity.isConstructorArgument(prop) || !documentAccessor.hasValue(prop)) {
					return;
				}

				accessor.setProperty(prop, getValueInternal(prop, bson, evaluator, currentPath));
			}
		});

		// Handle associations
		entity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {
			public void doWithAssociation(Association<MongoPersistentProperty> association) {

				final MongoPersistentProperty property = association.getInverse();
				Object value = documentAccessor.get(property);

				if (value == null || entity.isConstructorArgument(property)) {
					return;
				}

				DBRef dbref = value instanceof DBRef ? (DBRef) value : null;

				DbRefProxyHandler handler = new DefaultDbRefProxyHandler(spELContext, mappingContext,
						MappingMongoConverter.this);
				DbRefResolverCallback callback = new DefaultDbRefResolverCallback(bson, currentPath, evaluator,
						MappingMongoConverter.this);

				accessor.setProperty(property, dbRefResolver.resolveDbRef(property, dbref, callback, handler));
			}
		});

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.MongoWriter#toDBRef(java.lang.Object, org.springframework.data.mongodb.core.mapping.MongoPersistentProperty)
	 */
	public DBRef toDBRef(Object object, MongoPersistentProperty referringProperty) {

		org.springframework.data.mongodb.core.mapping.DBRef annotation = null;

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

	/**
	 * Root entry method into write conversion. Adds a type discriminator to the {@link Document}. Shouldn't be called for
	 * nested conversions.
	 * 
	 * @see org.springframework.data.mongodb.core.convert.MongoWriter#write(java.lang.Object, com.mongodb.Document)
	 */
	public void write(final Object obj, final Bson bson) {

		if (null == obj) {
			return;
		}

		Class<?> entityType = ClassUtils.getUserClass(obj.getClass());
		TypeInformation<? extends Object> type = ClassTypeInformation.from(entityType);

		Object target = obj instanceof LazyLoadingProxy ? ((LazyLoadingProxy) obj).getTarget() : obj;

		writeInternal(target, bson, Optional.of(type));
		if (asMap(bson).containsKey("_is") && asMap(bson).get("_id") == null) {
			removeFromMap(bson, "_id");
		}

		boolean handledByCustomConverter = conversions.getCustomWriteTarget(entityType, Document.class) != null;
		if (!handledByCustomConverter && !(bson instanceof Collection)) {
			typeMapper.writeType(type, bson);
		}
	}

	/**
	 * Internal write conversion method which should be used for nested invocations.
	 * 
	 * @param obj
	 * @param bson
	 */
	@SuppressWarnings("unchecked")
	protected void writeInternal(final Object obj, final Bson bson, final Optional<TypeInformation<?>> typeHint) {

		if (null == obj) {
			return;
		}

		Class<?> entityType = obj.getClass();
		Class<?> customTarget = conversions.getCustomWriteTarget(entityType, Document.class);

		if (customTarget != null) {
			Document result = conversionService.convert(obj, Document.class);
			addAllToMap(bson, result);
			return;
		}

		if (Map.class.isAssignableFrom(entityType)) {
			writeMapInternal((Map<Object, Object>) obj, bson, ClassTypeInformation.MAP);
			return;
		}

		if (Collection.class.isAssignableFrom(entityType)) {
			writeCollectionInternal((Collection<?>) obj, Optional.of(ClassTypeInformation.LIST), (BasicDBList) bson);
			return;
		}

		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityType);
		writeInternal(obj, bson, entity);
		addCustomTypeKeyIfNecessary(typeHint, obj, bson);
	}

	protected void writeInternal(Object obj, final Bson bson, MongoPersistentEntity<?> entity) {

		if (obj == null) {
			return;
		}

		if (null == entity) {
			throw new MappingException("No mapping metadata found for entity of type " + obj.getClass().getName());
		}

		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(obj);
		DocumentAccessor dbObjectAccessor = new DocumentAccessor(bson);

		Optional<MongoPersistentProperty> idProperty = entity.getIdProperty();
		idProperty.ifPresent(
				prop -> dbObjectAccessor.computeIfAbsent(prop, () -> idMapper.convertId(accessor.getProperty(prop))));

		// Write the properties
		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {
			public void doWithPersistentProperty(MongoPersistentProperty prop) {

				if (idProperty.map(it -> it.equals(prop)).orElse(false) || !prop.isWritable()) {
					return;
				}

				accessor.getProperty(prop).ifPresent(it -> {
					if (!conversions.isSimpleType(it.getClass())) {

						writePropertyInternal(it, bson, prop);
					} else {
						writeSimpleInternal(it, bson, prop);
					}
				});
			}
		});

		entity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {

			public void doWithAssociation(Association<MongoPersistentProperty> association) {

				MongoPersistentProperty inverseProp = association.getInverse();
				accessor.getProperty(inverseProp).ifPresent(it -> writePropertyInternal(it, bson, inverseProp));
			}
		});
	}

	@SuppressWarnings({ "unchecked" })
	protected void writePropertyInternal(Object obj, Bson bson, MongoPersistentProperty prop) {

		if (obj == null) {
			return;
		}

		DocumentAccessor accessor = new DocumentAccessor(bson);

		TypeInformation<?> valueType = ClassTypeInformation.from(obj.getClass());
		TypeInformation<?> type = prop.getTypeInformation();

		if (valueType.isCollectionLike()) {
			List<Object> collectionInternal = createCollection(asCollection(obj), prop);
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

			if (null != dbRefObj) {
				accessor.put(prop, dbRefObj);
				return;
			}
		}

		/*
		 * If we have a LazyLoadingProxy we make sure it is initialized first.
		 */
		if (obj instanceof LazyLoadingProxy) {
			obj = ((LazyLoadingProxy) obj).getTarget();
		}

		// Lookup potential custom target type
		Class<?> basicTargetType = conversions.getCustomWriteTarget(obj.getClass(), null);

		if (basicTargetType != null) {
			accessor.put(prop, conversionService.convert(obj, basicTargetType));
			return;
		}

		Object existingValue = accessor.get(prop);
		Document document = existingValue instanceof Document ? (Document) existingValue : new Document();

		MongoPersistentEntity<?> entity = isSubtype(prop.getType(), obj.getClass())
				? mappingContext.getRequiredPersistentEntity(obj.getClass()) : mappingContext.getRequiredPersistentEntity(type);

		writeInternal(obj, document, entity);
		addCustomTypeKeyIfNecessary(Optional.of(ClassTypeInformation.from(prop.getRawType())), obj, document);
		accessor.put(prop, document);
	}

	private boolean isSubtype(Class<?> left, Class<?> right) {
		return left.isAssignableFrom(right) && !left.equals(right);
	}

	/**
	 * Returns given object as {@link Collection}. Will return the {@link Collection} as is if the source is a
	 * {@link Collection} already, will convert an array into a {@link Collection} or simply create a single element
	 * collection for everything else.
	 * 
	 * @param source
	 * @return
	 */
	private static Collection<?> asCollection(Object source) {

		if (source instanceof Collection) {
			return (Collection<?>) source;
		}

		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}

	/**
	 * Writes the given {@link Collection} using the given {@link MongoPersistentProperty} information.
	 * 
	 * @param collection must not be {@literal null}.
	 * @param property must not be {@literal null}.
	 * @return
	 */
	protected List<Object> createCollection(Collection<?> collection, MongoPersistentProperty property) {

		if (!property.isDbReference()) {
			return writeCollectionInternal(collection, Optional.of(property.getTypeInformation()), new BasicDBList());
		}

		List<Object> dbList = new ArrayList<Object>();

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
	 * @return
	 */
	protected Bson createMap(Map<Object, Object> map, MongoPersistentProperty property) {

		Assert.notNull(map, "Given map must not be null!");
		Assert.notNull(property, "PersistentProperty must not be null!");

		if (!property.isDbReference()) {
			return writeMapInternal(map, new Document(), property.getTypeInformation());
		}

		Document document = new Document();

		for (Map.Entry<Object, Object> entry : map.entrySet()) {

			Object key = entry.getKey();
			Object value = entry.getValue();

			if (conversions.isSimpleType(key.getClass())) {

				String simpleKey = prepareMapKey(key.toString());
				document.put(simpleKey, value != null ? createDBRef(value, property) : null);

			} else {
				throw new MappingException("Cannot use a complex object as a key value.");
			}
		}

		return document;
	}

	/**
	 * Populates the given {@link BasicDBList} with values from the given {@link Collection}.
	 * 
	 * @param source the collection to create a {@link BasicDBList} for, must not be {@literal null}.
	 * @param type the {@link TypeInformation} to consider or {@literal null} if unknown.
	 * @param sink the {@link BasicDBList} to write to.
	 * @return
	 */
	private BasicDBList writeCollectionInternal(Collection<?> source, Optional<TypeInformation<?>> type,
			BasicDBList sink) {

		Optional<TypeInformation<?>> componentType = type.flatMap(it -> it.getComponentType());

		for (Object element : source) {

			Class<?> elementType = element == null ? null : element.getClass();

			if (elementType == null || conversions.isSimpleType(elementType)) {
				sink.add(getPotentiallyConvertedSimpleWrite(element));
			} else if (element instanceof Collection || elementType.isArray()) {
				sink.add(writeCollectionInternal(asCollection(element), componentType, new BasicDBList()));
			} else {
				Document document = new Document();
				writeInternal(element, document, componentType);
				sink.add(document);
			}
		}

		return sink;
	}

	/**
	 * Writes the given {@link Map} to the given {@link Document} considering the given {@link TypeInformation}.
	 * 
	 * @param obj must not be {@literal null}.
	 * @param bson must not be {@literal null}.
	 * @param propertyType must not be {@literal null}.
	 * @return
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
					addToMap(bson, simpleKey,
							writeCollectionInternal(asCollection(val), propertyType.getMapValueType(), new BasicDBList()));
				} else {
					Document document = new Document();
					Optional<TypeInformation<?>> valueTypeInfo = propertyType.isMap() ? propertyType.getMapValueType()
							: Optional.of(ClassTypeInformation.OBJECT);
					writeInternal(val, document, valueTypeInfo);
					addToMap(bson, simpleKey, document);
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
	 * @return
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
	 * @param source
	 * @return
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

		return source.replaceAll("\\.", mapKeyDotReplacement);
	}

	/**
	 * Returns a {@link String} representation of the given {@link Map} key
	 * 
	 * @param key
	 * @return
	 */
	private String potentiallyConvertMapKey(Object key) {

		if (key instanceof String) {
			return (String) key;
		}

		return conversions.hasCustomWriteTarget(key.getClass(), String.class)
				? (String) getPotentiallyConvertedSimpleWrite(key) : key.toString();
	}

	/**
	 * Translates the map key replacements in the given key just read with a dot in case a map key replacement has been
	 * configured.
	 * 
	 * @param source
	 * @return
	 */
	protected String potentiallyUnescapeMapKey(String source) {
		return mapKeyDotReplacement == null ? source : source.replaceAll(mapKeyDotReplacement, "\\.");
	}

	/**
	 * Adds custom type information to the given {@link Document} if necessary. That is if the value is not the same as
	 * the one given. This is usually the case if you store a subtype of the actual declared type of the property.
	 *
	 * @param type
	 * @param value must not be {@literal null}.
	 * @param bson must not be {@literal null}.
	 */
	protected void addCustomTypeKeyIfNecessary(Optional<TypeInformation<?>> type, Object value, Bson bson) {

		Optional<Class<?>> actualType = type.map(it -> it.getActualType()).map(it -> it.getType());
		Class<?> reference = actualType.orElse(Object.class);
		Class<?> valueType = ClassUtils.getUserClass(value.getClass());

		boolean notTheSameClass = !valueType.equals(reference);
		if (notTheSameClass) {
			typeMapper.writeType(valueType, bson);
		}
	}

	/**
	 * Writes the given simple value to the given {@link Document}. Will store enum names for enum values.
	 * 
	 * @param value
	 * @param bson must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 */
	private void writeSimpleInternal(Object value, Bson bson, String key) {
		addToMap(bson, key, getPotentiallyConvertedSimpleWrite(value));
	}

	private void writeSimpleInternal(Object value, Bson bson, MongoPersistentProperty property) {
		DocumentAccessor accessor = new DocumentAccessor(bson);
		accessor.put(property, getPotentiallyConvertedSimpleWrite(value));
	}

	/**
	 * Checks whether we have a custom conversion registered for the given value into an arbitrary simple Mongo type.
	 * Returns the converted value if so. If not, we perform special enum handling or simply return the value as is.
	 * 
	 * @param value
	 * @return
	 */
	private Object getPotentiallyConvertedSimpleWrite(Object value) {

		if (value == null) {
			return null;
		}

		Class<?> customTarget = conversions.getCustomWriteTarget(value.getClass(), null);

		if (customTarget != null) {
			return conversionService.convert(value, customTarget);
		} else if (ObjectUtils.isArray(value)) {

			if (value instanceof byte[]) {
				return value;
			}
			return asCollection(value);
		}

		else {
			return Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value;
		}
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is.
	 * 
	 * @param value
	 * @param target must not be {@literal null}.
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object getPotentiallyConvertedSimpleRead(Object value, Class<?> target) {

		if (value == null || target == null || target.isAssignableFrom(value.getClass())) {
			return value;
		}

		if (conversions.hasCustomReadTarget(value.getClass(), target)) {
			return conversionService.convert(value, target);
		}

		if (Enum.class.isAssignableFrom(target)) {
			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return conversionService.convert(value, target);
	}

	protected DBRef createDBRef(Object target, MongoPersistentProperty property) {

		Assert.notNull(target, "Target object must not be null!");

		if (target instanceof DBRef) {
			return (DBRef) target;
		}

		Optional<? extends MongoPersistentEntity<?>> targetEntity = mappingContext.getPersistentEntity(target.getClass());
		targetEntity = targetEntity.isPresent() ? targetEntity : mappingContext.getPersistentEntity(property);

		if (null == targetEntity) {
			throw new MappingException("No mapping metadata found for " + target.getClass());
		}

		MongoPersistentEntity<?> entity = targetEntity
				.orElseThrow(() -> new MappingException("No mapping metadata found for " + target.getClass()));

		Optional<MongoPersistentProperty> idProperty = entity.getIdProperty();

		return idProperty.map(it -> {

			Object id = target.getClass().equals(it.getType()) ? target : entity.getPropertyAccessor(target).getProperty(it);

			if (null == id) {
				throw new MappingException("Cannot create a reference to an object with a NULL id.");
			}

			return dbRefResolver.createDbRef(property == null ? null : property.getDBRef(), entity,
					idMapper.convertId(id instanceof Optional ? (Optional)id : Optional.ofNullable(id)).orElse(null));

		}).orElseThrow(() -> new MappingException("No id property found on class " + entity.getType()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.ValueResolver#getValueInternal(org.springframework.data.mongodb.core.mapping.MongoPersistentProperty, com.mongodb.Document, org.springframework.data.mapping.model.SpELExpressionEvaluator, java.lang.Object)
	 */
	@Override
	public Optional<Object> getValueInternal(MongoPersistentProperty prop, Bson bson, SpELExpressionEvaluator evaluator,
			ObjectPath path) {
		return new MongoDbPropertyValueProvider(bson, evaluator, path).getPropertyValue(prop);
	}

	/**
	 * Reads the given {@link BasicDBList} into a collection of the given {@link TypeInformation}.
	 * 
	 * @param targetType must not be {@literal null}.
	 * @param sourceValue must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object readCollectionOrArray(TypeInformation<?> targetType, List sourceValue, ObjectPath path) {

		Assert.notNull(targetType, "Target type must not be null!");
		Assert.notNull(path, "Object path must not be null!");

		Class<?> collectionType = targetType.getType();

		TypeInformation<?> componentType = targetType.getComponentType().orElse(ClassTypeInformation.OBJECT);
		Class<?> rawComponentType = componentType.getType();

		collectionType = Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;
		Collection<Object> items = targetType.getType().isArray() ? new ArrayList<Object>()
				: CollectionFactory.createCollection(collectionType, rawComponentType, sourceValue.size());

		if (sourceValue.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(items, collectionType);
		}

		if (!DBRef.class.equals(rawComponentType) && isCollectionOfDbRefWhereBulkFetchIsPossible(sourceValue)) {
			return bulkReadAndConvertDBRefs((List<DBRef>) (List) (sourceValue), componentType, path, rawComponentType);
		}

		for (Object dbObjItem : sourceValue) {

			if (dbObjItem instanceof DBRef) {
				items.add(DBRef.class.equals(rawComponentType) ? dbObjItem
						: readAndConvertDBRef((DBRef) dbObjItem, componentType, path, rawComponentType));
			} else if (dbObjItem instanceof Document) {
				items.add(read(componentType, (Document) dbObjItem, path));
			} else if (dbObjItem instanceof BasicDBObject) {
				items.add(read(componentType, (BasicDBObject) dbObjItem, path));
			} else {

				if (dbObjItem instanceof Collection) {
					if (!rawComponentType.isArray() && !ClassUtils.isAssignable(Iterable.class, rawComponentType)) {
						throw new MappingException(
								String.format(INCOMPATIBLE_TYPES, dbObjItem, dbObjItem.getClass(), rawComponentType, path));
					}
				}

				if (dbObjItem instanceof List) {
					items.add(readCollectionOrArray(ClassTypeInformation.OBJECT, (List) dbObjItem, path));
				} else {
					items.add(getPotentiallyConvertedSimpleRead(dbObjItem, rawComponentType));
				}
			}
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
	 */
	@SuppressWarnings("unchecked")
	protected Map<Object, Object> readMap(TypeInformation<?> type, Bson bson, ObjectPath path) {

		Assert.notNull(bson, "Document must not be null!");
		Assert.notNull(path, "Object path must not be null!");

		Class<?> mapType = typeMapper.readType(bson, type).getType();

		Optional<TypeInformation<?>> valueType = type.getMapValueType();
		Class<?> rawKeyType = type.getComponentType().map(it -> it.getType()).orElse(null);
		Class<?> rawValueType = type.getMapValueType().map(it -> it.getType()).orElse(null);

		Map<String, Object> sourceMap = asMap(bson);
		Map<Object, Object> map = CollectionFactory.createMap(mapType, rawKeyType, sourceMap.keySet().size());

		if (!DBRef.class.equals(rawValueType) && isCollectionOfDbRefWhereBulkFetchIsPossible(sourceMap.values())) {
			bulkReadAndConvertDBRefMapIntoTarget(valueType.orElse(null), rawValueType, sourceMap, map);
			return map;
		}

		for (Entry<String, Object> entry : sourceMap.entrySet()) {

			if (typeMapper.isTypeKey(entry.getKey())) {
				continue;
			}

			Object key = potentiallyUnescapeMapKey(entry.getKey());

			if (rawKeyType != null) {
				key = conversionService.convert(key, rawKeyType);
			}

			Object value = entry.getValue();
			TypeInformation<?> defaultedValueType = valueType.orElse(ClassTypeInformation.OBJECT);

			if (value instanceof Document) {
				map.put(key, read(defaultedValueType, (Document) value, path));
			} else if (value instanceof BasicDBObject) {
				map.put(key, read(defaultedValueType, (BasicDBObject) value, path));
			} else if (value instanceof DBRef) {
				map.put(key, DBRef.class.equals(rawValueType) ? value
						: readAndConvertDBRef((DBRef) value, defaultedValueType, ObjectPath.ROOT, rawValueType));
			} else if (value instanceof List) {
				map.put(key, readCollectionOrArray(valueType.orElse(ClassTypeInformation.LIST), (List) value, path));
			} else {
				map.put(key, getPotentiallyConvertedSimpleRead(value, rawValueType));
			}
		}

		return map;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> asMap(Bson bson) {

		if (bson instanceof Document) {
			return (Document) bson;
		}

		if (bson instanceof DBObject) {
			return ((DBObject) bson).toMap();
		}

		throw new IllegalArgumentException(
				String.format("Cannot read %s. as map. Given Bson must be a Document or DBObject!", bson.getClass()));
	}

	private void addToMap(Bson bson, String key, Object value) {

		if (bson instanceof Document) {
			((Document) bson).put(key, value);
			return;
		}
		if (bson instanceof DBObject) {
			((DBObject) bson).put(key, value);
			return;
		}
		throw new IllegalArgumentException(String.format(
				"Cannot add key/value pair to %s. as map. Given Bson must be a Document or DBObject!", bson.getClass()));
	}

	@SuppressWarnings("unchecked")
	private void addAllToMap(Bson bson, Map value) {

		if (bson instanceof Document) {
			((Document) bson).putAll(value);
			return;
		}

		if (bson instanceof DBObject) {
			((DBObject) bson).putAll(value);
			return;
		}

		throw new IllegalArgumentException(
				String.format("Cannot add all to %s. Given Bson must be a Document or DBObject.", bson.getClass()));
	}

	private void removeFromMap(Bson bson, String key) {

		if (bson instanceof Document) {
			((Document) bson).remove(key);
			return;
		}

		if (bson instanceof DBObject) {
			((DBObject) bson).removeField(key);
			return;
		}

		throw new IllegalArgumentException(
				String.format("Cannot remove from %s. Given Bson must be a Document or DBObject.", bson.getClass()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.MongoWriter#convertToMongoType(java.lang.Object, org.springframework.data.util.TypeInformation)
	 */
	@SuppressWarnings("unchecked")
	public Object convertToMongoType(Object obj, TypeInformation<?> typeInformation) {

		if (obj == null) {
			return null;
		}

		Class<?> target = conversions.getCustomWriteTarget(obj.getClass());
		if (target != null) {
			return conversionService.convert(obj, target);
		}

		if (conversions.isSimpleType(obj.getClass())) {
			// Doesn't need conversion
			return getPotentiallyConvertedSimpleWrite(obj);
		}

		TypeInformation<?> typeHint = typeInformation;

		if (obj instanceof List) {
			return maybeConvertList((List<Object>) obj, typeHint);
		}

		if (obj instanceof Document) {

			Document newValueDocument = new Document();
			for (String vk : ((Document) obj).keySet()) {
				Object o = ((Document) obj).get(vk);
				newValueDocument.put(vk, convertToMongoType(o, typeHint));
			}
			return newValueDocument;
		}

		if (obj instanceof DBObject) {

			Document newValueDbo = new Document();
			for (String vk : ((DBObject) obj).keySet()) {

				Object o = ((DBObject) obj).get(vk);
				newValueDbo.put(vk, convertToMongoType(o, typeHint));
			}

			return newValueDbo;
		}

		if (obj instanceof Map) {

			Map<Object, Object> converted = new LinkedHashMap<Object, Object>();
			Document result = new Document();

			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) obj).entrySet()) {
				result.put(entry.getKey().toString(), convertToMongoType(entry.getValue(), typeHint));
			}

			return result;
		}

		if (obj.getClass().isArray()) {
			return maybeConvertList(Arrays.asList((Object[]) obj), typeHint);
		}

		if (obj instanceof Collection) {
			return maybeConvertList((Collection<?>) obj, typeHint);
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

	public List<Object> maybeConvertList(Iterable<?> source, TypeInformation<?> typeInformation) {

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
					for (Object element : (List) value) {
						removeTypeInfo(element, recursively);
					}
				} else {
					removeTypeInfo(value, recursively);
				}
			}

			if (typeMapper.isTypeKey(key)) {

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

	/**
	 * {@link PropertyValueProvider} to evaluate a SpEL expression if present on the property or simply accesses the field
	 * of the configured source {@link Document}.
	 *
	 * @author Oliver Gierke
	 */
	private class MongoDbPropertyValueProvider implements PropertyValueProvider<MongoPersistentProperty> {

		private final DocumentAccessor source;
		private final SpELExpressionEvaluator evaluator;
		private final ObjectPath path;

		/**
		 * Creates a new {@link MongoDbPropertyValueProvider} for the given source, {@link SpELExpressionEvaluator} and
		 * {@link ObjectPath}.
		 *
		 * @param source must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 * @param path can be {@literal null}.
		 */
		public MongoDbPropertyValueProvider(Bson source, SpELExpressionEvaluator evaluator, ObjectPath path) {

			Assert.notNull(source, "Source document must no be null!");
			Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null!");
			Assert.notNull(path, "ObjectPath must not be null!");

			this.source = new DocumentAccessor(source);
			this.evaluator = evaluator;
			this.path = path;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.PropertyValueProvider#getPropertyValue(org.springframework.data.mapping.PersistentProperty)
		 */
		public <T> Optional<T> getPropertyValue(MongoPersistentProperty property) {
			return Optional

					.ofNullable(property.getSpelExpression()//
							.map(it -> evaluator.evaluate(it))//
							.orElseGet(() -> source.get(property)))//
					.map(it -> readValue(it, property.getTypeInformation(), path));
		}
	}

	/**
	 * Extension of {@link SpELExpressionParameterValueProvider} to recursively trigger value conversion on the raw
	 * resolved SpEL value.
	 * 
	 * @author Oliver Gierke
	 */
	private class ConverterAwareSpELExpressionParameterValueProvider
			extends SpELExpressionParameterValueProvider<MongoPersistentProperty> {

		private final ObjectPath path;

		/**
		 * Creates a new {@link ConverterAwareSpELExpressionParameterValueProvider}.
		 * 
		 * @param evaluator must not be {@literal null}.
		 * @param conversionService must not be {@literal null}.
		 * @param delegate must not be {@literal null}.
		 */
		public ConverterAwareSpELExpressionParameterValueProvider(SpELExpressionEvaluator evaluator,
				ConversionService conversionService, ParameterValueProvider<MongoPersistentProperty> delegate,
				ObjectPath path) {

			super(evaluator, conversionService, delegate);
			this.path = path;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.SpELExpressionParameterValueProvider#potentiallyConvertSpelValue(java.lang.Object, org.springframework.data.mapping.PreferredConstructor.Parameter)
		 */
		@Override
		protected <T> T potentiallyConvertSpelValue(Object object, Parameter<T, MongoPersistentProperty> parameter) {
			return readValue(object, parameter.getType(), path);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> T readValue(Object value, TypeInformation<?> type, ObjectPath path) {

		Class<?> rawType = type.getType();

		if (conversions.hasCustomReadTarget(value.getClass(), rawType)) {
			return (T) conversionService.convert(value, rawType);
		} else if (value instanceof DBRef) {
			return potentiallyReadOrResolveDbRef((DBRef) value, type, path, rawType);
		} else if (value instanceof List) {
			return (T) readCollectionOrArray(type, (List) value, path);
		} else if (value instanceof Document) {
			return (T) read(type, (Document) value, path);
		} else if (value instanceof DBObject) {
			return (T) read(type, (BasicDBObject) value, path);
		} else {
			return (T) getPotentiallyConvertedSimpleRead(value, rawType);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> T potentiallyReadOrResolveDbRef(DBRef dbref, TypeInformation<?> type, ObjectPath path, Class<?> rawType) {

		if (rawType.equals(DBRef.class)) {
			return (T) dbref;
		}

		Object object = dbref == null ? null : path.getPathItem(dbref.getId(), dbref.getCollectionName());
		return (T) (object != null ? object : readAndConvertDBRef(dbref, type, path, rawType));
	}

	private <T> T readAndConvertDBRef(DBRef dbref, TypeInformation<?> type, ObjectPath path, final Class<?> rawType) {

		List<T> result = bulkReadAndConvertDBRefs(Collections.singletonList(dbref), type, path, rawType);
		return CollectionUtils.isEmpty(result) ? null : result.iterator().next();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void bulkReadAndConvertDBRefMapIntoTarget(TypeInformation<?> valueType, Class<?> rawValueType,
			Map<String, Object> sourceMap, Map<Object, Object> targetMap) {

		LinkedHashMap<String, Object> referenceMap = new LinkedHashMap<String, Object>(sourceMap);
		List<Object> convertedObjects = bulkReadAndConvertDBRefs((List<DBRef>) new ArrayList(referenceMap.values()),
				valueType, ObjectPath.ROOT, rawValueType);
		int index = 0;

		for (String key : referenceMap.keySet()) {
			targetMap.put(key, convertedObjects.get(index));
			index++;
		}
	}

	@SuppressWarnings("unchecked")
	private <T> List<T> bulkReadAndConvertDBRefs(List<DBRef> dbrefs, TypeInformation<?> type, ObjectPath path,
			final Class<?> rawType) {

		if (CollectionUtils.isEmpty(dbrefs)) {
			return Collections.emptyList();
		}

		List<Document> referencedRawDocuments = dbrefs.size() == 1
				? Collections.singletonList(readRef(dbrefs.iterator().next())) : bulkReadRefs(dbrefs);
		String collectionName = dbrefs.iterator().next().getCollectionName();

		List<T> targeList = new ArrayList<T>(dbrefs.size());

		for (Document document : referencedRawDocuments) {

			if (document != null) {
				maybeEmitEvent(new AfterLoadEvent<T>(document, (Class<T>) rawType, collectionName));
			}

			final T target = (T) read(type, document, path);
			targeList.add(target);

			if (target != null) {
				maybeEmitEvent(new AfterConvertEvent<T>(document, target, collectionName));
			}
		}

		return targeList;
	}

	private void maybeEmitEvent(MongoMappingEvent<?> event) {

		if (canPublishEvent()) {
			this.applicationContext.publishEvent(event);
		}
	}

	private boolean canPublishEvent() {
		return this.applicationContext != null;
	}

	/**
	 * Performs the fetch operation for the given {@link DBRef}.
	 * 
	 * @param ref
	 * @return
	 */
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
	 * Returns whether the given {@link Iterable} contains {@link DBRef} instances all pointing to the same collection.
	 *
	 * @param source must not be {@literal null}.
	 * @return
	 */
	private static boolean isCollectionOfDbRefWhereBulkFetchIsPossible(Iterable<Object> source) {

		Assert.notNull(source, "Iterable of DBRefs must not be null!");

		Set<String> collectionsFound = new HashSet<String>();

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

	/**
	 * Marker class used to indicate we have a non root document object here that might be used within an update - so we
	 * need to preserve type hints for potential nested elements but need to remove it on top level.
	 * 
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	static class NestedDocument {

	}
}
