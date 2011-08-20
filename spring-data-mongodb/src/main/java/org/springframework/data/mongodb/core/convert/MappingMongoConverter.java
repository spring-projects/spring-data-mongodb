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

package org.springframework.data.mongodb.core.convert;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.SpELAwareParameterValueProvider;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
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

	@SuppressWarnings("rawtypes")
	private static final TypeInformation<Map> MAP_TYPE_INFORMATION = ClassTypeInformation.from(Map.class);
	private static final List<Class<?>> VALID_ID_TYPES = Arrays.asList(new Class<?>[] { ObjectId.class, String.class,
			BigInteger.class, byte[].class });
	
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
	public MappingMongoConverter(MongoDbFactory mongoDbFactory,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		super(ConversionServiceFactory.createDefaultConversionService());

		Assert.notNull(mongoDbFactory);
		Assert.notNull(mappingContext);

		this.mongoDbFactory = mongoDbFactory;
		this.mappingContext = mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.convert.MongoConverter#getMappingContext()
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
	 * @see org.springframework.data.mongodb.core.core.MongoReader#read(java.lang.Class, com.mongodb.DBObject)
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

		if (conversions.hasCustomReadTarget(dbo.getClass(), rawType)) {
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
		
		if (typeToUse.isMap()) {
			return (S) readMap(typeToUse, dbo);
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
			String[] keySet = dbo.keySet().toArray(new String[] {});
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
	 * @see org.springframework.data.mongodb.core.core.convert.MongoWriter#write(java.lang.Object, com.mongodb.DBObject)
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
			Class<?>[] targetClasses = new Class<?>[] { ObjectId.class, String.class, Object.class };
			for (Class<?> targetClass : targetClasses) {
				try {
					idObj = wrapper.getProperty(idProperty, targetClass, useFieldAccessOnly);
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
						writePropertyInternal(propertyObj, dbo, prop);
					} else {
						writeSimpleInternal(propertyObj, dbo, prop.getFieldName());
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
					writePropertyInternal(propertyObj, dbo, inverseProp);
				}
			}
		});
	}

	@SuppressWarnings({ "unchecked" })
	protected void writePropertyInternal(Object obj, DBObject dbo, MongoPersistentProperty prop) {

		if (obj == null) {
			return;
		}

		String name = prop.getFieldName();

		if (prop.isCollection()) {
			DBObject collectionInternal = createCollection(asCollection(obj), prop);
			dbo.put(name, collectionInternal);
			return;
		}

		TypeInformation<?> type = prop.getTypeInformation();
		
		if (prop.isMap()) {
			BasicDBObject mapDbObj = new BasicDBObject();
			writeMapInternal((Map<Object, Object>) obj, mapDbObj, type);
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
		addCustomTypeKeyIfNecessary(type, obj, propDbObj);
		
		MongoPersistentEntity<?> entity = isSubtype(prop.getType(), obj.getClass()) ? mappingContext
				.getPersistentEntity(obj.getClass()) : mappingContext.getPersistentEntity(type);
		
		writeInternal(obj, propDbObj, entity);
		dbo.put(name, propDbObj);
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
	 * 
	 * @return
	 */
	protected DBObject createCollection(Collection<?> collection, MongoPersistentProperty property) {

		if (!property.isDbReference()) {
			return createCollectionDBObject(collection, property.getTypeInformation());
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
	 * @param source the collection to create a {@link BasicDBList} for, must not be {@literal null}.
	 * @param type the {@link TypeInformation} to consider or {@literal null} if unknown.
	 * @return
	 */
	private BasicDBList createCollectionDBObject(Collection<?> source, TypeInformation<?> type) {

		BasicDBList dbList = new BasicDBList();
		TypeInformation<?> componentType = type == null ? null : type.getComponentType();

		for (Object element : source) {

			if (element == null) {
				continue;
			}

			Class<?> elementType = element.getClass();

			if (conversions.isSimpleType(elementType)) {
				dbList.add(getPotentiallyConvertedSimpleWrite(element));
			} else if (element instanceof Collection || elementType.isArray()) {
				dbList.add(createCollectionDBObject(asCollection(element), componentType));
			} else {
				BasicDBObject propDbObj = new BasicDBObject();
				writeInternal(element, propDbObj,
						mappingContext.getPersistentEntity(ClassTypeInformation.from(element.getClass())));
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
				if (val == null || conversions.isSimpleType(val.getClass())) {
					writeSimpleInternal(val, dbo, simpleKey);
				} else if (val instanceof Collection) {
					dbo.put(simpleKey, createCollectionDBObject((Collection<?>) val, propertyType.getMapValueType()));
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
	protected void addCustomTypeKeyIfNecessary(TypeInformation<?> type, Object value, DBObject dbObject) {

		if (type == null) {
			return;
		}

		Class<?> reference = type.getActualType().getType();

		boolean notTheSameClass = !value.getClass().equals(reference);
		if (notTheSameClass) {
			dbObject.put(CUSTOM_TYPE_KEY, value.getClass().getName());
		}
	}

	/**
	 * Writes the given simple value to the given {@link DBObject}. Will store enum names for enum values.
	 * 
	 * @param value
	 * @param dbObject must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 */
	private void writeSimpleInternal(Object value, DBObject dbObject, String key) {
		dbObject.put(key, getPotentiallyConvertedSimpleWrite(value));
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
		} else {
			return value.getClass().isEnum() ? ((Enum<?>) value).name() : value;
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
		
		Assert.notNull(target);
		
		if (value == null) {
			return null;
		}
		
		if (conversions.hasCustomReadTarget(value.getClass(), target)) {
			return conversionService.convert(value, target);
		}
		
		if (target.isEnum()) {
			return Enum.valueOf((Class<Enum>) target, value.toString());
		}
		
		return value;
	}

	protected DBRef createDBRef(Object target, org.springframework.data.mongodb.core.mapping.DBRef dbref) {
		MongoPersistentEntity<?> targetEntity = mappingContext.getPersistentEntity(target.getClass());
		if (null == targetEntity || null == targetEntity.getIdProperty()) {
			return null;
		}

		MongoPersistentProperty idProperty = targetEntity.getIdProperty();
		Object id = null;
		BeanWrapper<MongoPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(target, conversionService);
		try {
			id = wrapper.getProperty(idProperty, Object.class, useFieldAccessOnly);
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

	@SuppressWarnings("unchecked")
	protected Object getValueInternal(MongoPersistentProperty prop, DBObject dbo, StandardEvaluationContext ctx,
			String spelExpr) {

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
				} else if (prop.isArray() && sourceValue instanceof BasicDBObject
						&& ((DBObject) sourceValue).keySet().size() == 0) {
					// It's empty
					return Array.newInstance(prop.getComponentType(), 0);
				} else if (prop.isCollection() && sourceValue instanceof BasicDBList) {

					BasicDBList dbObjList = (BasicDBList) sourceValue;
					Collection<Object> items = prop.isArray() ? new ArrayList<Object>() : CollectionFactory.createCollection(
							propertyType, dbObjList.size());

					for (int i = 0; i < dbObjList.size(); i++) {
						Object dbObjItem = dbObjList.get(i);
						if (dbObjItem instanceof DBRef) {
							items.add(read(prop.getComponentType(), ((DBRef) dbObjItem).fetch()));
						} else if (dbObjItem instanceof DBObject) {
							items.add(read(prop.getComponentType(), (DBObject) dbObjItem));
						} else {
							items.add(getPotentiallyConvertedSimpleRead(dbObjItem, prop.getComponentType()));
						}
					}

					return items;
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
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Map<Object, Object> readMap(TypeInformation<?> type, DBObject dbObject) {

		Assert.notNull(dbObject);
		
		type = type == null ? MAP_TYPE_INFORMATION : type;
		Class<?> customMapType = findTypeToBeUsed(dbObject);
		Class<?> mapType = customMapType == null ? type.getType() : customMapType;

		Map<Object, Object> map = CollectionFactory.createMap(mapType, dbObject.keySet().size());
		Map<String, Object> sourceMap = dbObject.toMap();

		for (Entry<String, Object> entry : sourceMap.entrySet()) {
			if (entry.getKey().equals(CUSTOM_TYPE_KEY)) {
				continue;
			}

			Object key = entry.getKey();
			
			TypeInformation<?> keyTypeInformation = type.getComponentType();
			if (keyTypeInformation != null) {
				Class<?> keyType = keyTypeInformation.getType();
				key = conversionService.convert(entry.getKey(), keyType);
			}
			
			Object value = entry.getValue();
			TypeInformation<?> valueType = type.getMapValueType();
			valueType = valueType == null ? MAP_TYPE_INFORMATION : valueType;

			if (value instanceof BasicDBList) {
				BasicDBList list =(BasicDBList) value;
				map.put(key, read(valueType, list));
			} else if (value instanceof DBObject) {
				DBObject valueSource = (DBObject) value;
				map.put(key, valueType.isMap() ? readMap(valueType, valueSource) : read(valueType, valueSource));
			} else {
				map.put(key, getPotentiallyConvertedSimpleRead(value, valueType.getType()));
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
		
		if (dbObject instanceof BasicDBList) {
			return List.class;
		}
		
		Object classToBeUsed = dbObject.get(CUSTOM_TYPE_KEY);

		if (classToBeUsed == null) {
			return null;
		}

		try {
			return ClassUtils.forName(classToBeUsed.toString(), null);
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
		
		if (documentsTargetType == null && Object.class.equals(rawType)) {
			return (TypeInformation<? extends S>) MAP_TYPE_INFORMATION;
		}
		
		boolean isMoreConcreteCustomType = documentsTargetType != null && rawType.isAssignableFrom(documentsTargetType);
		return isMoreConcreteCustomType ? (TypeInformation<? extends S>) ClassTypeInformation.from(documentsTargetType) : basicType;
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

	@SuppressWarnings("unchecked")
	public Object convertToMongoType(Object obj) {
		
		if (obj == null) {
			return null;
		}
		
		Class<?> target = conversions.getCustomWriteTarget(getClass());
		if (target != null) {
			return conversionService.convert(obj, target);
		}

		if (null != obj && conversions.isSimpleType(obj.getClass())) {
			// Doesn't need conversion
			return getPotentiallyConvertedSimpleWrite(obj);
		}

		if (obj instanceof BasicDBList) {
			return maybeConvertList((BasicDBList) obj);
		}

		if (obj instanceof DBObject) {
			DBObject newValueDbo = new BasicDBObject();
			for (String vk : ((DBObject) obj).keySet()) {
				Object o = ((DBObject) obj).get(vk);
				newValueDbo.put(vk, convertToMongoType(o));
			}
			return newValueDbo;
		}

		if (obj instanceof Map) {
			Map<Object, Object> m = new HashMap<Object, Object>();
			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) obj).entrySet()) {
				m.put(entry.getKey(), convertToMongoType(entry.getValue()));
			}
			return m;
		}

		if (obj instanceof List) {
			List<?> l = (List<?>) obj;
			List<Object> newList = new ArrayList<Object>();
			for (Object o : l) {
				newList.add(convertToMongoType(o));
			}
			return newList;
		}

		if (obj.getClass().isArray()) {
			return maybeConvertArray((Object[]) obj);
		}

		DBObject newDbo = new BasicDBObject();
		this.write(obj, newDbo);
		return newDbo;
	}

	public Object[] maybeConvertArray(Object[] src) {
		Object[] newArr = new Object[src.length];
		for (int i = 0; i < src.length; i++) {
			newArr[i] = convertToMongoType(src[i]);
		}
		return newArr;
	}

	public BasicDBList maybeConvertList(BasicDBList dbl) {
		BasicDBList newDbl = new BasicDBList();
		Iterator<?> iter = dbl.iterator();
		while (iter.hasNext()) {
			Object o = iter.next();
			newDbl.add(convertToMongoType(o));
		}
		return newDbl;
	}
}
