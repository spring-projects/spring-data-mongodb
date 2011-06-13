/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.convert;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionException;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.data.document.mongodb.MongoDbFactory;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntity;
import org.springframework.data.document.mongodb.mapping.MongoPersistentProperty;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.BeanWrapper;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.Association;
import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PreferredConstructor;
import org.springframework.data.mapping.model.SpELAwareParameterValueProvider;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@link MongoConverter} that uses a {@link MappingContext} to do sophisticated mapping of domain objects to
 * {@link DBObject}.
 *
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class MappingMongoConverter extends AbstractMongoConverter implements ApplicationContextAware {

	public static final String CUSTOM_TYPE_KEY = "_class";

	private static final List<Class<?>> VALID_ID_TYPES = Arrays.asList(new Class<?>[]{ObjectId.class, String.class,
			BigInteger.class, byte[].class});
	protected static final Log log = LogFactory.getLog(MappingMongoConverter.class);

	protected final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	protected final SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
	protected final MongoDbFactory mongoDbFactory;
	protected ApplicationContext applicationContext;
	protected boolean useFieldAccessOnly = true;

	/**
	 * Creates a new {@link MappingMongoConverter} given the new {@link MongoDbFactory} and {@link MappingContext}.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingMongoConverter(MongoDbFactory mongoDbFactory, MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		
		super(ConversionServiceFactory.createDefaultConversionService());
		
		Assert.notNull(mongoDbFactory);
		Assert.notNull(mappingContext);
		
		this.mongoDbFactory = mongoDbFactory;
		this.mappingContext = mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.convert.MongoConverter#getMappingContext()
	 */
	public MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	/**
	 * Configures whether to use field access only for entity mapping. Setting this to true will force the
	 * {@link MongoConverter} to not go through getters or setters even if they are present for getting and setting
	 * property values.
	 * 
	 * @param useFieldAccessOnly
	 */
	public void setUseFieldAccessOnly(boolean useFieldAccessOnly) {
		this.useFieldAccessOnly = useFieldAccessOnly;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.convert.MongoConverter#convertObjectId(org.bson.types.ObjectId, java.lang.Class)
	 */
	public <T> T convertObjectId(ObjectId id, Class<T> targetType) {
		return conversionService.convert(id, targetType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.convert.MongoConverter#convertObjectId(java.lang.Object)
	 */
	public ObjectId convertObjectId(Object id) {
		return conversionService.convert(id, ObjectId.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.MongoReader#read(java.lang.Class, com.mongodb.DBObject)
	 */
	public <S extends Object> S read(Class<S> clazz, final DBObject dbo) {
		return read(ClassTypeInformation.from(clazz), dbo);
	}

	@SuppressWarnings("unchecked")
	protected <S extends Object> S read(TypeInformation<S> type, DBObject dbo) {

		if (null == dbo) {
			return null;
		}

		TypeInformation<? extends S> typeToUse = getMoreConcreteTargetType(dbo, type);
		Class<? extends S> rawType = typeToUse.getType();

		if (conversions.hasCustomReadTarget(DBObject.class, rawType)) {
			return conversionService.convert(dbo, rawType);
		}

		if (typeToUse.isCollectionLike() && dbo instanceof BasicDBList) {
			List<Object> l = new ArrayList<Object>();
			BasicDBList dbList = (BasicDBList) dbo;
			for (Object o : dbList) {
				if (o instanceof DBObject) {

					Object newObj = read(typeToUse.getComponentType(), (DBObject) o);
					Class<?> rawComponentType = typeToUse.getComponentType().getType();

					if (newObj.getClass().isAssignableFrom(rawComponentType)) {
						l.add(newObj);
					} else {
						l.add(conversionService.convert(newObj, rawComponentType));
					}
				} else {
					l.add(o);
				}
			}
			return conversionService.convert(l, rawType);
		}

		// Retrieve persistent entity info
		MongoPersistentEntity<S> persistentEntity = (MongoPersistentEntity<S>) mappingContext
				.getPersistentEntity(typeToUse);
		if (persistentEntity == null) {
			throw new MappingException("No mapping metadata found for " + rawType.getName());
		}

		return read(persistentEntity, dbo);
	}

	private <S extends Object> S read(final MongoPersistentEntity<S> entity, final DBObject dbo) {

		final StandardEvaluationContext spelCtx = new StandardEvaluationContext();
		if (null != applicationContext) {
			spelCtx.setBeanResolver(new BeanFactoryResolver(applicationContext));
		}
		if (!(dbo instanceof BasicDBList)) {
			String[] keySet = dbo.keySet().toArray(new String[]{});
			for (String key : keySet) {
				spelCtx.setVariable(key, dbo.get(key));
			}
		}

		final List<String> ctorParamNames = new ArrayList<String>();
		final MongoPersistentProperty idProperty = entity.getIdProperty();

		ParameterValueProvider provider = new SpELAwareParameterValueProvider(spelExpressionParser, spelCtx) {
			@Override
			@SuppressWarnings("unchecked")
			public <T> T getParameterValue(PreferredConstructor.Parameter<T> parameter) {

				if (parameter.getKey() != null) {
					return super.getParameterValue(parameter);
				}

				String name = parameter.getName();
				TypeInformation<T> type = parameter.getType();
				Class<T> rawType = parameter.getRawType();
				String key = idProperty == null ? name : idProperty.getName().equals(name) ? idProperty.getFieldName() : name;
				Object obj = dbo.get(key);

				ctorParamNames.add(name);
				if (obj instanceof DBRef) {
					return read(type, ((DBRef) obj).fetch());
				} else if (obj instanceof BasicDBList) {
					BasicDBList objAsDbList = (BasicDBList) obj;
					List<?> l = unwrapList(objAsDbList, type);
					return conversionService.convert(l, rawType);
				} else if (obj instanceof DBObject) {
					return read(type, ((DBObject) obj));
				} else if (null != obj && obj.getClass().isAssignableFrom(rawType)) {
					return (T) obj;
				} else if (null != obj) {
					return conversionService.convert(obj, rawType);
				}

				return null;
			}
		};

		final BeanWrapper<MongoPersistentEntity<S>, S> wrapper = BeanWrapper.create(entity, provider, conversionService);

		// Set properties not already set in the constructor
		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {
			public void doWithPersistentProperty(MongoPersistentProperty prop) {

				boolean isConstructorProperty = ctorParamNames.contains(prop.getName());
				boolean hasValueForProperty = dbo.containsField(prop.getFieldName());

				if (!hasValueForProperty || isConstructorProperty) {
					return;
				}

				Object obj = getValueInternal(prop, dbo, spelCtx, prop.getSpelExpression());
				try {
					wrapper.setProperty(prop, obj, useFieldAccessOnly);
				} catch (IllegalAccessException e) {
					throw new MappingException(e.getMessage(), e);
				} catch (InvocationTargetException e) {
					throw new MappingException(e.getMessage(), e);
				}
			}
		});

		// Handle associations
		entity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {
			public void doWithAssociation(Association<MongoPersistentProperty> association) {
				MongoPersistentProperty inverseProp = association.getInverse();
				Object obj = getValueInternal(inverseProp, dbo, spelCtx, inverseProp.getSpelExpression());
				try {
					wrapper.setProperty(inverseProp, obj);
				} catch (IllegalAccessException e) {
					throw new MappingException(e.getMessage(), e);
				} catch (InvocationTargetException e) {
					throw new MappingException(e.getMessage(), e);
				}
			}
		});

		return wrapper.getBean();
	}

	/**
	 * Root entry method into write conversion. Adds a type discriminator to the {@link DBObject}. Shouldn't be called for
	 * nested conversions.
	 *
	 * @see org.springframework.data.document.mongodb.MongoWriter#write(java.lang.Object, com.mongodb.DBObject)
	 */
	public void write(final Object obj, final DBObject dbo) {

		if (null == obj) {
			return;
		}

		boolean handledByCustomConverter = conversions.getCustomWriteTarget(obj.getClass(), DBObject.class) != null;

		if (!handledByCustomConverter) {
			dbo.put(CUSTOM_TYPE_KEY, obj.getClass().getName());
		}

		writeInternal(obj, dbo);
	}

	/**
	 * Internal write conversion method which should be used for nested invocations.
	 *
	 * @param obj
	 * @param dbo
	 */
	@SuppressWarnings("unchecked")
	protected void writeInternal(final Object obj, final DBObject dbo) {

		if (null == obj) {
			return;
		}

		Class<?> customTarget = conversions.getCustomWriteTarget(obj.getClass(), DBObject.class);

		if (customTarget != null) {
			DBObject result = conversionService.convert(obj, DBObject.class);
			dbo.putAll(result);
			return;
		}

		if (Map.class.isAssignableFrom(obj.getClass())) {
			writeMapInternal((Map<Object, Object>) obj, dbo, null);
			return;
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(obj.getClass());
		writeInternal(obj, dbo, entity);
	}

	protected void writeInternal(Object obj, final DBObject dbo, MongoPersistentEntity<?> entity) {

		if (obj == null) {
			return;
		}

		if (null == entity) {
			throw new MappingException("No mapping metadata found for entity of type " + obj.getClass().getName());
		}

		final BeanWrapper<MongoPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(obj, conversionService);

		// Write the ID
		final MongoPersistentProperty idProperty = entity.getIdProperty();
		if (!dbo.containsField("_id") && null != idProperty) {
			Object idObj = null;
			Class<?>[] targetClasses = new Class<?>[]{ObjectId.class, Object.class};
			for (Class<?> targetClasse : targetClasses) {
				try {
					idObj = wrapper.getProperty(idProperty, targetClasse, useFieldAccessOnly);
					if (null != idObj) {
						break;
					}
				} catch (ConversionException ignored) {
				} catch (IllegalAccessException e) {
					throw new MappingException(e.getMessage(), e);
				} catch (InvocationTargetException e) {
					throw new MappingException(e.getMessage(), e);
				}
			}

			if (null != idObj) {
				dbo.put("_id", idObj);
			} else {
				if (!VALID_ID_TYPES.contains(idProperty.getType())) {
					throw new MappingException("Invalid data type " + idProperty.getType().getName()
							+ " for Id property. Should be one of " + VALID_ID_TYPES);
				}
			}
		}

		// Write the properties
		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {
			public void doWithPersistentProperty(MongoPersistentProperty prop) {
				if (prop.equals(idProperty)) {
					return;
				}
				Object propertyObj;
				try {
					propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);
				} catch (IllegalAccessException e) {
					throw new MappingException(e.getMessage(), e);
				} catch (InvocationTargetException e) {
					throw new MappingException(e.getMessage(), e);
				}
				if (null != propertyObj) {
					if (!conversions.isSimpleType(propertyObj.getClass())) {
						writePropertyInternal(prop, propertyObj, dbo);
					} else {
						writeSimpleInternal(prop.getFieldName(), propertyObj, dbo);
					}
				}
			}
		});

		entity.doWithAssociations(new AssociationHandler<MongoPersistentProperty>() {
			public void doWithAssociation(Association<MongoPersistentProperty> association) {
				MongoPersistentProperty inverseProp = association.getInverse();
				Class<?> type = inverseProp.getType();
				Object propertyObj;
				try {
					propertyObj = wrapper.getProperty(inverseProp, type, useFieldAccessOnly);
				} catch (IllegalAccessException e) {
					throw new MappingException(e.getMessage(), e);
				} catch (InvocationTargetException e) {
					throw new MappingException(e.getMessage(), e);
				}
				if (null != propertyObj) {
					writePropertyInternal(inverseProp, propertyObj, dbo);
				}
			}
		});
	}

	@SuppressWarnings({"unchecked"})
	protected void writePropertyInternal(MongoPersistentProperty prop, Object obj, DBObject dbo) {

		if (obj == null) {
			return;
		}

		String name = prop.getFieldName();

		if (prop.isCollection()) {
			DBObject collectionInternal = writeCollectionInternal(prop, asCollection(obj));
			dbo.put(name, collectionInternal);
			return;
		}

		if (prop.isMap()) {
			BasicDBObject mapDbObj = new BasicDBObject();
			writeMapInternal((Map<Object, Object>) obj, mapDbObj, prop.getTypeInformation());
			dbo.put(name, mapDbObj);
			return;
		}

		if (prop.isDbReference()) {
			DBRef dbRefObj = createDBRef(obj, prop.getDBRef());
			if (null != dbRefObj) {
				dbo.put(name, dbRefObj);
				return;
			}
		}

		// Lookup potential custom target type
		Class<?> basicTargetType = conversions.getCustomWriteTarget(obj.getClass(), null);

		if (basicTargetType != null) {
			dbo.put(name, conversionService.convert(obj, basicTargetType));
			return;
		}

		BasicDBObject propDbObj = new BasicDBObject();
		addCustomTypeKeyIfNecessary(prop.getTypeInformation(), obj, propDbObj);
		writeInternal(obj, propDbObj, mappingContext.getPersistentEntity(prop.getTypeInformation()));
		dbo.put(name, propDbObj);
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
	 * @param property
	 * @param collection
	 * @return
	 */
	protected DBObject writeCollectionInternal(MongoPersistentProperty property, Collection<?> collection) {
		
		if (!property.isDbReference()) {
			return createCollectionDBObject(property.getTypeInformation(), collection);
		}

		BasicDBList dbList = new BasicDBList();
		
		for (Object element : collection) {

			if (element == null) {
				continue;
			}

			DBRef dbRef = createDBRef(element, property.getDBRef());
			dbList.add(dbRef);
		}
		
		return dbList;
	}
	
	/**
	 * Creates a new {@link BasicDBList} from the given {@link Collection}.
	 * 
	 * @param type
	 * @param source
	 * @return
	 */
	private BasicDBList createCollectionDBObject(TypeInformation<?> type, Collection<?> source) {
		
		BasicDBList dbList = new BasicDBList();
		TypeInformation<?> componentType = type.getComponentType();
		
		for (Object element : source) {

			if (element == null) {
				continue;
			}
			
			Class<?> elementType = element.getClass();

			if (conversions.isSimpleType(elementType)) {
				dbList.add(element);
			} else if (element instanceof Collection || elementType.isArray()) {
				dbList.add(createCollectionDBObject(componentType, asCollection(element)));
			} else {
				BasicDBObject propDbObj = new BasicDBObject();
				writeInternal(element, propDbObj, mappingContext.getPersistentEntity(ClassTypeInformation.from(element.getClass())));
				addCustomTypeKeyIfNecessary(componentType, element, propDbObj);
				dbList.add(propDbObj);
			}
		}

		return dbList;
	}

	protected void writeMapInternal(Map<Object, Object> obj, DBObject dbo, TypeInformation<?> propertyType) {
		for (Map.Entry<Object, Object> entry : obj.entrySet()) {
			Object key = entry.getKey();
			Object val = entry.getValue();
			if (conversions.isSimpleType(key.getClass())) {
				// Don't use conversion service here as removal of ObjectToString converter results in some primitive types not
				// being convertable
				String simpleKey = key.toString();
				if (conversions.isSimpleType(val.getClass())) {
					writeSimpleInternal(simpleKey, val, dbo);
				} else {
					DBObject newDbo = new BasicDBObject();
					writeInternal(val, newDbo);
					addCustomTypeKeyIfNecessary(propertyType, val, newDbo);
					dbo.put(simpleKey, newDbo);
				}
			} else {
				throw new MappingException("Cannot use a complex object as a key value.");
			}
		}
	}

	/**
	 * Adds custom type information to the given {@link DBObject} if necessary. That is if the value is not the same as
	 * the one given. This is usually the case if you store a subtype of the actual declared type of the property.
	 *
	 * @param type
	 * @param value
	 * @param dbObject
	 */
	public void addCustomTypeKeyIfNecessary(TypeInformation<?> type, Object value, DBObject dbObject) {

		if (type == null) {
			return;
		}

		Class<?> reference = getValueType(type).getType();

		boolean notTheSameClass = !value.getClass().equals(reference);
		if (notTheSameClass) {
			dbObject.put(CUSTOM_TYPE_KEY, value.getClass().getName());
		}
	}

	/**
	 * Returns the type type information of the actual value to be stored. That is, for maps it will return the map value
	 * type, for collections it will return the component type as well as the given type if it is a non-collection or
	 * non-map one.
	 *
	 * @param type
	 * @return
	 */
	public TypeInformation<?> getValueType(TypeInformation<?> type) {
		if (type.isMap()) {
			return type.getMapValueType();
		} else if (type.isCollectionLike()) {
			return type.getComponentType();
		} else {
			return type;
		}
	}

	/**
	 * Writes the given simple value to the given {@link DBObject}. Will store enum names for enum values.
	 *
	 * @param key
	 * @param value
	 * @param dbObject
	 */
	private void writeSimpleInternal(String key, Object value, DBObject dbObject) {

		Class<?> customTarget = conversions.getCustomWriteTarget(value.getClass(), null);

		Object valueToSet = null;
		if (customTarget != null) {
			valueToSet = conversionService.convert(value, customTarget);
		} else {
			valueToSet = value.getClass().isEnum() ? ((Enum<?>) value).name() : value;
		}
		dbObject.put(key, valueToSet);
	}

	protected DBRef createDBRef(Object target, org.springframework.data.document.mongodb.mapping.DBRef dbref) {
		MongoPersistentEntity<?> targetEntity = mappingContext.getPersistentEntity(target.getClass());
		if (null == targetEntity || null == targetEntity.getIdProperty()) {
			return null;
		}

		MongoPersistentProperty idProperty = targetEntity.getIdProperty();
		ObjectId id = null;
		BeanWrapper<MongoPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(target, conversionService);
		try {
			id = wrapper.getProperty(idProperty, ObjectId.class, useFieldAccessOnly);
			if (null == id) {
				throw new MappingException("Cannot create a reference to an object with a NULL id.");
			}
		} catch (IllegalAccessException e) {
			throw new MappingException(e.getMessage(), e);
		} catch (InvocationTargetException e) {
			throw new MappingException(e.getMessage(), e);
		}

		String collection = dbref.collection();
		if ("".equals(collection)) {
			collection = targetEntity.getCollection();
		}

		String dbname = dbref.db();
		DB db = StringUtils.hasText(dbname) ? mongoDbFactory.getDb(dbname) : mongoDbFactory.getDb();
		return new DBRef(db, collection, id);
	}


	protected Object getValueInternal(MongoPersistentProperty prop, DBObject dbo, StandardEvaluationContext ctx, String spelExpr) {

		Object o;
		if (null != spelExpr) {
			Expression x = spelExpressionParser.parseExpression(spelExpr);
			o = x.getValue(ctx);
		} else {

			Object sourceValue = dbo.get(prop.getFieldName());

			if (sourceValue == null) {
				return null;
			}

			Class<?> propertyType = prop.getType();

			if (conversions.hasCustomReadTarget(sourceValue.getClass(), propertyType)) {
				return conversionService.convert(sourceValue, propertyType);
			}

			if (sourceValue instanceof DBRef) {
				sourceValue = ((DBRef) sourceValue).fetch();
			}
			if (sourceValue instanceof DBObject) {
				if (prop.isMap()) {
					return readMap(prop.getTypeInformation(), (DBObject) sourceValue);
				} else if (prop.isArray() && sourceValue instanceof BasicDBObject && ((DBObject) sourceValue).keySet().size() == 0) {
					// It's empty
					return Array.newInstance(prop.getComponentType(), 0);
				} else if (prop.isCollection() && sourceValue instanceof BasicDBList) {
					BasicDBList dbObjList = (BasicDBList) sourceValue;
					List<Object> items = new ArrayList<Object>();
					for (int i = 0; i < dbObjList.size(); i++) {
						Object dbObjItem = dbObjList.get(i);
						if (dbObjItem instanceof DBRef) {
							items.add(read(prop.getComponentType(), ((DBRef) dbObjItem).fetch()));
						} else if (dbObjItem instanceof DBObject) {
							items.add(read(prop.getComponentType(), (DBObject) dbObjItem));
						} else {
							items.add(dbObjItem);
						}
					}
					List<Object> itemsToReturn = new ArrayList<Object>();
					for (Object obj : items) {
						itemsToReturn.add(obj);
					}
					return itemsToReturn;
				}

				Class<?> toType = findTypeToBeUsed((DBObject) sourceValue);

				// It's a complex object, have to read it in
				if (toType != null) {
					dbo.removeField(CUSTOM_TYPE_KEY);
					o = read(toType, (DBObject) sourceValue);
				} else {
					o = read(mappingContext.getPersistentEntity(prop.getTypeInformation()), (DBObject) sourceValue);
				}
			} else {
				o = sourceValue;
			}
		}
		return o;
	}

	/**
	 * Reads the given {@link DBObject} into a {@link Map}. will recursively resolve nested {@link Map}s as well.
	 * 
	 * @param type the {@link Map} {@link TypeInformation} to be used to unmarshall this {@link DBObject}.
	 * @param dbObject
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<Object, Object> readMap(TypeInformation<?> type, DBObject dbObject) {
		
		Assert.notNull(type);
		Assert.isTrue(type.isMap());
		Assert.notNull(dbObject);
		
		Class<?> customMapType = findTypeToBeUsed(dbObject);
		Class<?> mapType = customMapType == null ? Map.class : customMapType;
		
		Map<Object, Object> map = CollectionFactory.createMap(mapType, dbObject.keySet().size());
		Map<String, Object> sourceMap = dbObject.toMap();
		
		for (Entry<String, Object> entry : sourceMap.entrySet()) {
			if (entry.getKey().equals(CUSTOM_TYPE_KEY)) {
				continue;
			}

			Class<?> keyType = type.getComponentType().getType();
			Object key = conversionService.convert(entry.getKey(), keyType);

			if (null != entry.getValue() && entry.getValue() instanceof DBObject) {
				
				DBObject valueSource = (DBObject) entry.getValue();
				TypeInformation<?> valueType = type.getMapValueType();
				
				Object value = valueType.isMap() ? readMap(valueType, valueSource) : read(valueType, valueSource);
				
				map.put(key, value);
			} else {
				map.put(key, entry.getValue());
			}
		}
		
		return map;
	}

	/**
	 * Returns the type to be used to convert the DBObject given to. Will return {@literal null} if there's not type hint
	 * found in the {@link DBObject} or the type hint found can't be converted into a {@link Class} as the type might not
	 * be available.
	 *
	 * @param dbObject
	 * @return the type to be used for converting the given {@link DBObject} into or {@literal null} if there's no type
	 *         found.
	 */
	protected Class<?> findTypeToBeUsed(DBObject dbObject) {
		Object classToBeUsed = dbObject.get(CUSTOM_TYPE_KEY);

		if (classToBeUsed == null) {
			return null;
		}

		try {
			return Class.forName(classToBeUsed.toString());
		} catch (ClassNotFoundException e) {
			return null;
		}
	}

	/**
	 * Inspects the a custom class definition stored inside the given {@link DBObject} and returns that in case it's a
	 * subtype of the given basic one.
	 *
	 * @param dbObject
	 * @param basicType
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <S> TypeInformation<? extends S> getMoreConcreteTargetType(DBObject dbObject, TypeInformation<S> basicType) {
		Class<?> documentsTargetType = findTypeToBeUsed(dbObject);
		Class<S> rawType = basicType.getType();
		boolean isMoreConcreteCustomType = documentsTargetType != null && rawType.isAssignableFrom(documentsTargetType);
		return isMoreConcreteCustomType ? (TypeInformation<? extends S>) ClassTypeInformation.from(documentsTargetType)
				: basicType;
	}

	protected <T> List<?> unwrapList(BasicDBList dbList, TypeInformation<T> targetType) {
		List<Object> rootList = new ArrayList<Object>();
		for (int i = 0; i < dbList.size(); i++) {
			Object obj = dbList.get(i);
			if (obj instanceof BasicDBList) {
				rootList.add(unwrapList((BasicDBList) obj, targetType.getComponentType()));
			} else if (obj instanceof DBObject) {
				rootList.add(read(targetType, (DBObject) obj));
			} else {
				rootList.add(obj);
			}
		}
		return rootList;
	}
}
