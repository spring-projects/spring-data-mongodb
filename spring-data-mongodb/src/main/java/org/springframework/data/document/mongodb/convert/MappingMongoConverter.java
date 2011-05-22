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

import static org.springframework.data.document.mongodb.convert.ObjectIdConverters.*;
import static org.springframework.data.mapping.MappingBeanHelper.*;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.document.mongodb.MongoDbFactory;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntity;
import org.springframework.data.document.mongodb.mapping.MongoPersistentProperty;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingBeanHelper;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.Association;
import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PreferredConstructor;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@link MongoConverter} that uses a {@link MappingContext} to do sophisticated mapping of domain objects to
 * {@link DBObject}.
 *
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class MappingMongoConverter extends AbstractMongoConverter implements ApplicationContextAware, InitializingBean {

	public static final String CUSTOM_TYPE_KEY = "_class";
	@SuppressWarnings({"unchecked"})
	private static final List<Class<?>> MONGO_TYPES = Arrays.asList(Number.class, Date.class, String.class,
			DBObject.class);
	private static final List<Class<?>> VALID_ID_TYPES = Arrays.asList(new Class<?>[]{ObjectId.class, String.class,
			BigInteger.class, byte[].class});
	protected static final Log log = LogFactory.getLog(MappingMongoConverter.class);

	protected final GenericConversionService conversionService = ConversionServiceFactory
			.createDefaultConversionService();
	protected final Set<ConvertiblePair> customTypeMapping = new HashSet<ConvertiblePair>();
	protected final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	protected SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
	protected ApplicationContext applicationContext;
	protected boolean useFieldAccessOnly = true;
	protected MongoDbFactory mongoDbFactory;

	public MappingMongoConverter(MongoDbFactory mongoDbFactory, MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		this.mongoDbFactory = mongoDbFactory;
		this.mappingContext = mappingContext;
	}

	/**
	 * Creates a new {@link MappingMongoConverter} with the given {@link MappingContext}.
	 *
	 * @param mappingContext
	 */
	public MappingMongoConverter(
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		this.mappingContext = mappingContext;
		this.conversionService.removeConvertible(Object.class, String.class);
	}

	/**
	 * Add custom {@link Converter} or {@link ConverterFactory} instances to be used that will take presidence over
	 * metadata driven conversion between of objects to/from DBObject
	 *
	 * @param converters
	 */
	@Override
	public void setCustomConverters(Set<?> converters) {
		if (null != converters) {
			for (Object c : converters) {
				registerConverter(c);
			}
		}
	}

	/**
	 * Inspects the given {@link Converter} for the types it can convert and registers the pair for custom type conversion
	 * in case the target type is a Mongo basic type.
	 *
	 * @param converter
	 */
	private void registerConverter(Object converter) {
		Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), Converter.class);
		if (MONGO_TYPES.contains(arguments[1]) || MONGO_TYPES.contains(arguments[0])) {
			customTypeMapping.add(new ConvertiblePair(arguments[0], arguments[1]));
		}
		boolean added = false;
		if (converter instanceof Converter) {
			this.conversionService.addConverter((Converter<?, ?>) converter);
			added = true;
		}
		if (converter instanceof ConverterFactory) {
			this.conversionService.addConverterFactory((ConverterFactory<?, ?>) converter);
			added = true;
		}
		if (!added) {
			throw new IllegalArgumentException("Given set contains element that is neither Converter nor ConverterFactory!");
		}
	}

	private Class<?> getCustomTarget(Class<?> source, Class<?> expectedTargetType) {
		for (ConvertiblePair typePair : customTypeMapping) {
			if (typePair.getSourceType().isAssignableFrom(source)) {

				Class<?> targetType = typePair.getTargetType();

				if (targetType.equals(expectedTargetType) || expectedTargetType == null) {
					return targetType;
				}
			}
		}

		return null;
	}

	public MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	public void setMongoDbFactory(MongoDbFactory mongoDbFactory) {
		this.mongoDbFactory = mongoDbFactory;
	}

	public boolean isUseFieldAccessOnly() {
		return useFieldAccessOnly;
	}

	public void setUseFieldAccessOnly(boolean useFieldAccessOnly) {
		this.useFieldAccessOnly = useFieldAccessOnly;
	}

	public <T> T convertObjectId(ObjectId id, Class<T> targetType) {
		return conversionService.convert(id, targetType);
	}

	public ObjectId convertObjectId(Object id) {
		return conversionService.convert(id, ObjectId.class);
	}

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
		Class<?> customTarget = getCustomTarget(rawType, DBObject.class);

		if (customTarget != null) {
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
		final S instance = constructInstance(entity, new PreferredConstructor.ParameterValueProvider() {
					@SuppressWarnings("unchecked")
					public <T> T getParameterValue(PreferredConstructor.Parameter<T> parameter) {
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
				}, spelCtx);

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
					setProperty(instance, prop, obj, useFieldAccessOnly);
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
					setProperty(instance, inverseProp, obj);
				} catch (IllegalAccessException e) {
					throw new MappingException(e.getMessage(), e);
				} catch (InvocationTargetException e) {
					throw new MappingException(e.getMessage(), e);
				}
			}
		});

		return instance;
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

		boolean handledByCustomConverter = getCustomTarget(obj.getClass(), DBObject.class) != null;

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

		Class<?> customTarget = getCustomTarget(obj.getClass(), DBObject.class);

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

	protected void writeInternal(final Object obj, final DBObject dbo, MongoPersistentEntity<?> entity) {

		if (obj == null) {
			return;
		}

		if (null == entity) {
			throw new MappingException("No mapping metadata found for entity of type " + obj.getClass().getName());
		}

		// Write the ID
		final MongoPersistentProperty idProperty = entity.getIdProperty();
		if (!dbo.containsField("_id") && null != idProperty) {
			Object idObj;
			try {
				idObj = getProperty(obj, idProperty, Object.class, useFieldAccessOnly);
			} catch (IllegalAccessException e) {
				throw new MappingException(e.getMessage(), e);
			} catch (InvocationTargetException e) {
				throw new MappingException(e.getMessage(), e);
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
				Object propertyObj;
				try {
					propertyObj = getProperty(obj, prop, prop.getType(), useFieldAccessOnly);
				} catch (IllegalAccessException e) {
					throw new MappingException(e.getMessage(), e);
				} catch (InvocationTargetException e) {
					throw new MappingException(e.getMessage(), e);
				}
				if (null != propertyObj) {
					if (!isSimpleType(propertyObj.getClass())) {
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
					propertyObj = getProperty(obj, inverseProp, type, useFieldAccessOnly);
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

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * Registers converters for {@link ObjectId} handling, removes plain {@link #toString()} converter and promotes the
	 * configured {@link ConversionService} to {@link MappingBeanHelper}.
	 */
	private void initializeConverters() {

		if (!conversionService.canConvert(ObjectId.class, String.class)) {
			conversionService.addConverter(ObjectIdToStringConverter.INSTANCE);
		}
		if (!conversionService.canConvert(String.class, ObjectId.class)) {
			conversionService.addConverter(StringToObjectIdConverter.INSTANCE);
		}
		if (!conversionService.canConvert(ObjectId.class, BigInteger.class)) {
			conversionService.addConverter(ObjectIdToBigIntegerConverter.INSTANCE);
		}
		if (!conversionService.canConvert(BigInteger.class, ObjectId.class)) {
			conversionService.addConverter(BigIntegerToObjectIdConverter.INSTANCE);
		}

		setConversionService(conversionService);
	}

	@SuppressWarnings({"unchecked"})
	protected void writePropertyInternal(MongoPersistentProperty prop, Object obj, DBObject dbo) {
		
		if (obj == null) {
			return;
		}

		String name = prop.getFieldName();
		
		if (prop.isCollection()) {
			DBObject collectionInternal = writeCollectionInternal(prop, obj);
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
		Class<?> basicTargetType = getCustomTarget(obj.getClass(), null);

		if (basicTargetType != null) {
			dbo.put(name, conversionService.convert(obj, basicTargetType));
			return;
		}

		BasicDBObject propDbObj = new BasicDBObject();
		addCustomTypeKeyIfNecessary(prop.getTypeInformation(), obj, propDbObj);
		writeInternal(obj, propDbObj, mappingContext.getPersistentEntity(prop.getTypeInformation()));
		dbo.put(name, propDbObj);
	}
	
	@SuppressWarnings("unchecked")
	protected DBObject writeCollectionInternal(MongoPersistentProperty property, Object obj) {
		
		BasicDBList dbList = new BasicDBList();
		Class<?> type = property.getType();
		Collection<Object> coll = type.isArray() ? CollectionUtils.arrayToList(obj) : (Collection<Object>) obj;
		TypeInformation<?> componentType = property.getTypeInformation().getComponentType();
		
		for (Object element : coll) {
			
			if (element == null) {
				continue;
			}
			
			TypeInformation<?> valueType = ClassTypeInformation.from(element.getClass());
			
			if (property.isDbReference()) {
				DBRef dbRef = createDBRef(element, property.getDBRef());
				dbList.add(dbRef);
			} else if (type.isArray() && isSimpleType(property.getComponentType())) {
				dbList.add(element);
			} else if (element instanceof List) {
				List<?> propObjColl = (List<?>) element;
				while (valueType.isCollectionLike()) {
					valueType = valueType.getComponentType();
				}
				if (isSimpleType(valueType.getType())) {
					dbList.add(propObjColl);
				} else {
					BasicDBList propNestedDbList = new BasicDBList();
					for (Object propNestedObjItem : propObjColl) {
						BasicDBObject propDbObj = new BasicDBObject();
						writeInternal(propNestedObjItem, propDbObj);
						propNestedDbList.add(propDbObj);
					}
					dbList.add(propNestedDbList);
				}
			} else if (isSimpleType(element.getClass())) {
				dbList.add(element);
			} else {
				BasicDBObject propDbObj = new BasicDBObject();
				writeInternal(element, propDbObj, mappingContext.getPersistentEntity(valueType));
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
			if (isSimpleType(key.getClass())) {
				// Don't use conversion service here as removal of ObjectToString converter results in some primitive types not
				// being convertable
				String simpleKey = key.toString();
				if (isSimpleType(val.getClass())) {
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

		Object valueToSet = value.getClass().isEnum() ? ((Enum<?>) value).name() : value;
		dbObject.put(key, valueToSet);
	}

	protected DBRef createDBRef(Object target, org.springframework.data.document.mongodb.mapping.DBRef dbref) {
		MongoPersistentEntity<?> targetEntity = mappingContext.getPersistentEntity(target.getClass());
		if (null == targetEntity || null == targetEntity.getIdProperty()) {
			return null;
		}

		MongoPersistentProperty idProperty = targetEntity.getIdProperty();
		ObjectId id = null;
		try {
			id = getProperty(target, idProperty, ObjectId.class, useFieldAccessOnly);
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
		DB db = StringUtils.hasText(dbname) ? mongoDbFactory.getMongo().getDB(dbname) : mongoDbFactory.getDb();
		return new DBRef(db, collection, id);
	}

	@SuppressWarnings({"unchecked"})
	protected Object getValueInternal(MongoPersistentProperty prop, DBObject dbo, StandardEvaluationContext ctx,
																		String spelExpr) {

		Object o;
		if (null != spelExpr) {
			Expression x = spelExpressionParser.parseExpression(spelExpr);
			o = x.getValue(ctx);
		} else {

			Object dbObj = dbo.get(prop.getFieldName());

			if (dbObj == null) {
				return null;
			}

			Class<?> propertyType = prop.getType();
			Class<?> customTarget = getCustomTarget(dbObj.getClass(), propertyType);

			if (customTarget != null) {
				return conversionService.convert(dbObj, propertyType);
			}

			if (dbObj instanceof DBRef) {
				dbObj = ((DBRef) dbObj).fetch();
			}
			if (dbObj instanceof DBObject) {
				if (prop.isMap() && dbObj instanceof DBObject) {

					// We have to find a potentially stored class to be used first.
					Class<?> toType = findTypeToBeUsed((DBObject) dbObj);
					Map<Object, Object> m = new LinkedHashMap<Object, Object>();

					for (Map.Entry<String, Object> entry : ((Map<String, Object>) ((DBObject) dbObj).toMap()).entrySet()) {
						if (entry.getKey().equals(CUSTOM_TYPE_KEY)) {
							continue;
						}

						Class<?> keyType = prop.getComponentType();
						Object key = conversionService.convert(entry.getKey(), keyType);

						if (null != entry.getValue() && entry.getValue() instanceof DBObject) {
							m.put(key, read((null != toType ? toType : prop.getMapValueType()), (DBObject) entry.getValue()));
						} else {
							m.put(key, entry.getValue());
						}
					}
					return m;
				} else if (prop.isArray() && dbObj instanceof BasicDBObject && ((DBObject) dbObj).keySet().size() == 0) {
					// It's empty
					return Array.newInstance(prop.getComponentType(), 0);
				} else if (prop.isCollection() && dbObj instanceof BasicDBList) {
					BasicDBList dbObjList = (BasicDBList) dbObj;
					List<Object> items = new LinkedList<Object>();
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
					List<Object> itemsToReturn = new LinkedList<Object>();
					for (Object obj : items) {
						itemsToReturn.add(obj);
					}
					return itemsToReturn;
				}

				Class<?> toType = findTypeToBeUsed((DBObject) dbObj);

				// It's a complex object, have to read it in
				if (toType != null) {
					dbo.removeField(CUSTOM_TYPE_KEY);
					o = read(toType, (DBObject) dbObj);
				} else {
					o = read(mappingContext.getPersistentEntity(prop.getTypeInformation()), (DBObject) dbObj);
				}
			} else {
				o = dbObj;
			}
		}
		return o;
	}

	/**
	 * Returns the type to be used to convert the DBObject given to.
	 *
	 * @param dbObject
	 * @return
	 */
	protected Class<?> findTypeToBeUsed(DBObject dbObject) {
		Object classToBeUsed = dbObject.get(CUSTOM_TYPE_KEY);

		if (classToBeUsed == null) {
			return null;
		}

		try {
			return Class.forName(classToBeUsed.toString());
		} catch (ClassNotFoundException e) {
			throw new MappingException(e.getMessage(), e);
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
		List<Object> rootList = new LinkedList<Object>();
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

	public void afterPropertiesSet() {
		initializeConverters();
	}

}
