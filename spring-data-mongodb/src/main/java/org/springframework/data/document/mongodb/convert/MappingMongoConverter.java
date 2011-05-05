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
import com.mongodb.Mongo;
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
import org.springframework.data.document.mongodb.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingBeanHelper;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.Association;
import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PersistentEntity;
import org.springframework.data.mapping.model.PersistentProperty;
import org.springframework.data.mapping.model.PreferredConstructor;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * {@link MongoConverter} that uses a {@link MappingContext} to do sophisticated mapping of domain objects to
 * {@link DBObject}.
 *
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke
 */
public class MappingMongoConverter extends AbstractMongoConverter implements ApplicationContextAware, InitializingBean {

	private static final String CUSTOM_TYPE_KEY = "_class";
	@SuppressWarnings({"unchecked"})
	private static final List<Class<?>> MONGO_TYPES = Arrays.asList(Number.class, Date.class, String.class, DBObject.class);
	private static final List<Class<?>> VALID_ID_TYPES = Arrays.asList(new Class<?>[]{ObjectId.class, String.class, BigInteger.class, byte[].class});
	protected static final Log log = LogFactory.getLog(MappingMongoConverter.class);

	protected final GenericConversionService conversionService = ConversionServiceFactory.createDefaultConversionService();
	protected final Set<ConvertiblePair> customTypeMapping = new HashSet<ConvertiblePair>();
	protected final MappingContext<BasicMongoPersistentEntity<?>> mappingContext;
	protected SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
	protected ApplicationContext applicationContext;
	protected boolean useFieldAccessOnly = true;
	protected Mongo mongo;
	protected String defaultDatabase;

	/**
	 * Creates a new {@link MappingMongoConverter} with the given {@link MappingContext}.
	 *
	 * @param mappingContext
	 */
	public MappingMongoConverter(MappingContext<BasicMongoPersistentEntity<?>> mappingContext) {
		this.mappingContext = mappingContext;
		this.conversionService.removeConvertible(Object.class, String.class);
	}

	/**
	 * Add custom {@link Converter} or {@link ConverterFactory} instances to be used that will take presidence over
	 * metadata driven conversion between of objects to/from DBObject
	 *
	 * @param converters
	 */
	public void setConverters(List<Converter<?, ?>> converters) {
		if (null != converters) {
			for (Converter<?, ?> c : converters) {
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
	private void registerConverter(Converter<?, ?> converter) {
		Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), Converter.class);
		if (MONGO_TYPES.contains(arguments[1]) || MONGO_TYPES.contains(arguments[0])) {
			customTypeMapping.add(new ConvertiblePair(arguments[0], arguments[1]));
		}
		conversionService.addConverter(converter);
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

	public MappingContext<BasicMongoPersistentEntity<?>> getMappingContext() {
		return mappingContext;
	}

	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}

	public void setDefaultDatabase(String defaultDatabase) {
		this.defaultDatabase = defaultDatabase;
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


	@SuppressWarnings({"unchecked", "rawtypes"})
	public <S extends Object> S read(Class<S> clazz, final DBObject dbo) {
		if (null == dbo) {
			return null;
		}

		Class<?> customTarget = getCustomTarget(clazz, DBObject.class);

		if (customTarget != null) {
			return conversionService.convert(dbo, clazz);
		}

		if ((clazz.isArray()
				|| (clazz.isAssignableFrom(Collection.class)
				|| clazz.isAssignableFrom(List.class)))
				&& dbo instanceof BasicDBList) {
			List l = new ArrayList<S>();
			BasicDBList dbList = (BasicDBList) dbo;
			for (Object o : dbList) {
				if (o instanceof DBObject) {
					Object newObj = read(clazz.getComponentType(), (DBObject) o);
					if (newObj.getClass().isAssignableFrom(clazz.getComponentType())) {
						l.add(newObj);
					} else {
						l.add(conversionService.convert(newObj, clazz.getComponentType()));
					}
				} else {
					l.add(o);
				}
			}
			return conversionService.convert(l, clazz);
		}

		// Retrieve persistent entity info
		PersistentEntity<S> persistentEntity = (PersistentEntity<S>) mappingContext.getPersistentEntity(clazz);
		if (persistentEntity == null) {
			throw new MappingException("No mapping metadata found for " + clazz.getName());
		}

		return read(persistentEntity, dbo);
	}

	private <S extends Object> S read(PersistentEntity<S> entity, final DBObject dbo) {

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
		final S instance = constructInstance(entity, new PreferredConstructor.ParameterValueProvider() {
			public Object getParameterValue(PreferredConstructor.Parameter parameter) {
				String name = parameter.getName();
				Class<?> type = parameter.getType();
				Object obj = dbo.get(name);

				ctorParamNames.add(name);
				if (obj instanceof DBRef) {
					return read(type, ((DBRef) obj).fetch());
				} else if (obj instanceof BasicDBList) {
					BasicDBList objAsDbList = (BasicDBList) obj;
					List<?> l = unwrapList(objAsDbList, type);
					return conversionService.convert(l, parameter.getRawType());
				} else if (obj instanceof DBObject) {
					return read(type, ((DBObject) obj));
				} else if (null != obj && obj.getClass().isAssignableFrom(type)) {
					return obj;
				} else if (null != obj) {
					return conversionService.convert(obj, type);
				}

				return null;
			}
		}, spelCtx);

		// Set the ID
		PersistentProperty idProperty = entity.getIdProperty();
		if (dbo.containsField("_id") && null != idProperty) {
			Object idObj = dbo.get("_id");
			try {
				setProperty(instance, idProperty, idObj, useFieldAccessOnly);
			} catch (IllegalAccessException e) {
				throw new MappingException(e.getMessage(), e);
			} catch (InvocationTargetException e) {
				throw new MappingException(e.getMessage(), e);
			}
		}

		// Set properties not already set in the constructor
		entity.doWithProperties(new PropertyHandler() {
			public void doWithPersistentProperty(PersistentProperty prop) {
				if (ctorParamNames.contains(prop.getName())) {
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
		entity.doWithAssociations(new AssociationHandler() {
			public void doWithAssociation(Association association) {
				PersistentProperty inverseProp = association.getInverse();
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

	public void write(final Object obj, final DBObject dbo) {
		if (null == obj) {
			return;
		}

		Class<?> customTarget = getCustomTarget(obj.getClass(), DBObject.class);

		if (customTarget != null) {
			DBObject result = conversionService.convert(obj, DBObject.class);
			dbo.putAll(result);
			return;
		}

		PersistentEntity<?> entity = mappingContext.getPersistentEntity(obj.getClass());
		write(obj, dbo, entity);
	}

	protected void write(final Object obj, final DBObject dbo, PersistentEntity<?> entity) {

		if (obj == null) {
			return;
		}

		if (null == entity) {
			throw new MappingException("No mapping metadata found for entity of type " + obj.getClass().getName());
		}

		// Write the ID
		final PersistentProperty idProperty = entity.getIdProperty();
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
					throw new MappingException("Invalid data type " + idProperty.getType().getName() + " for Id property. Should be one of " + VALID_ID_TYPES);
				}
			}
		}

		// Write the properties
		entity.doWithProperties(new PropertyHandler() {
			public void doWithPersistentProperty(PersistentProperty prop) {
				String name = prop.getName();
				Class<?> type = prop.getType();
				Object propertyObj;
				try {
					propertyObj = getProperty(obj, prop, type, useFieldAccessOnly);
				} catch (IllegalAccessException e) {
					throw new MappingException(e.getMessage(), e);
				} catch (InvocationTargetException e) {
					throw new MappingException(e.getMessage(), e);
				}
				if (null != propertyObj) {
					if (!isSimpleType(propertyObj.getClass())) {
						writePropertyInternal(prop, propertyObj, dbo);
					} else {
						dbo.put(name, propertyObj);
					}
				}
			}
		});

		entity.doWithAssociations(new AssociationHandler() {
			public void doWithAssociation(Association association) {
				PersistentProperty inverseProp = association.getInverse();
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
	protected void writePropertyInternal(PersistentProperty prop, Object obj, DBObject dbo) {
		org.springframework.data.document.mongodb.mapping.DBRef dbref = prop.getField()
				.getAnnotation(org.springframework.data.document.mongodb.mapping.DBRef.class);

		String name = prop.getName();
		Class<?> type = prop.getType();
		if (prop.isCollection()) {
			BasicDBList dbList = new BasicDBList();
			Collection<?> coll;
			if (type.isArray()) {
				coll = new ArrayList<Object>();
				for (Object o : (Object[]) obj) {
					((List) coll).add(o);
				}
			} else {
				coll = (Collection<?>) obj;
			}
			for (Object propObjItem : coll) {
				if (null != dbref) {
					DBRef dbRef = createDBRef(propObjItem, dbref);
					dbList.add(dbRef);
				} else if (type.isArray() && isSimpleType(prop.getComponentType())) {
					dbList.add(propObjItem);
				} else if (propObjItem instanceof List) {
					List<?> propObjColl = (List<?>) propObjItem;
					ClassTypeInformation typeInfo = new ClassTypeInformation(propObjItem.getClass());
					while (typeInfo.isCollectionLike()) {
						typeInfo = new ClassTypeInformation(typeInfo.getComponentType().getType());
					}
					if (isSimpleType(typeInfo.getType())) {
						dbList.add(propObjColl);
					} else {
						BasicDBList propNestedDbList = new BasicDBList();
						for (Object propNestedObjItem : propObjColl) {
							BasicDBObject propDbObj = new BasicDBObject();
							write(propNestedObjItem, propDbObj);
							propNestedDbList.add(propDbObj);
						}
						dbList.add(propNestedDbList);
					}
				} else if (isSimpleType(propObjItem.getClass())) {
					dbList.add(propObjItem);
				} else {
					BasicDBObject propDbObj = new BasicDBObject();
					write(propObjItem, propDbObj, mappingContext.getPersistentEntity(prop.getComponentType()));
					dbList.add(propDbObj);
				}
			}
			dbo.put(name, dbList);
			return;
		}

		if (null != obj && obj instanceof Map) {
			BasicDBObject mapDbObj = new BasicDBObject();
			writeMapInternal((Map<Object, Object>) obj, mapDbObj);
			dbo.put(name, mapDbObj);
			return;
		}

		if (null != dbref) {
			DBRef dbRefObj = createDBRef(obj, dbref);
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
		write(obj, propDbObj, mappingContext.getPersistentEntity(prop.getTypeInformation()));
		dbo.put(name, propDbObj);
	}

	protected void writeMapInternal(Map<Object, Object> obj, DBObject dbo) {
		for (Map.Entry<Object, Object> entry : obj.entrySet()) {
			Object key = entry.getKey();
			Object val = entry.getValue();
			if (isSimpleType(key.getClass())) {
				String simpleKey = conversionService.convert(key, String.class);
				if (isSimpleType(val.getClass())) {
					dbo.put(simpleKey, val);
				} else {
					DBObject newDbo = new BasicDBObject();
					Class<?> componentType = val.getClass();
					if (componentType.isArray()
							|| componentType.isAssignableFrom(Collection.class)
							|| componentType.isAssignableFrom(List.class)) {
						Class<?> ctype = val.getClass().getComponentType();
						dbo.put("_class", (null != ctype ? ctype.getName() : componentType.getName()));
					} else {
						dbo.put("_class", componentType.getName());
					}
					write(val, newDbo);
					dbo.put(simpleKey, newDbo);
				}
			} else {
				throw new MappingException("Cannot use a complex object as a key value.");
			}
		}
	}

	protected DBRef createDBRef(Object target, org.springframework.data.document.mongodb.mapping.DBRef dbref) {
		PersistentEntity<?> targetEntity = mappingContext.getPersistentEntity(target.getClass());
		if (null == targetEntity || null == targetEntity.getIdProperty()) {
			return null;
		}

		PersistentProperty idProperty = targetEntity.getIdProperty();
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
			collection = targetEntity.getType().getSimpleName().toLowerCase();
		}

		String dbname = dbref.db();
		if ("".equals(dbname)) {
			dbname = defaultDatabase;
		}

		DB db = mongo.getDB(dbname);
		return new DBRef(db, collection, id);
	}

	@SuppressWarnings({"unchecked"})
	protected Object getValueInternal(PersistentProperty prop, DBObject dbo, StandardEvaluationContext ctx, String spelExpr) {

		String name = prop.getName();
		Class<?> propertyType = prop.getType();

		Object o;
		if (null != spelExpr) {
			Expression x = spelExpressionParser.parseExpression(spelExpr);
			o = x.getValue(ctx);
		} else {
			Object dbObj = dbo.get(name);

			if (dbObj == null) {
				return null;
			}

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
					Map<String, Object> m = new LinkedHashMap<String, Object>();

					for (Map.Entry<String, Object> entry : ((Map<String, Object>) ((DBObject) dbObj).toMap()).entrySet()) {
						if (entry.getKey().equals(CUSTOM_TYPE_KEY)) {
							continue;
						}
						if (null != entry.getValue() && entry.getValue() instanceof DBObject) {
							m.put(entry.getKey(), read((null != toType ? toType : prop.getMapValueType()), (DBObject) entry.getValue()));
						} else {
							m.put(entry.getKey(), entry.getValue());
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

	protected <T> List<?> unwrapList(BasicDBList dbList, Class<T> targetType) {
		List<Object> rootList = new LinkedList<Object>();
		for (int i = 0; i < dbList.size(); i++) {
			Object obj = dbList.get(i);
			if (obj instanceof BasicDBList) {
				rootList.add(unwrapList((BasicDBList) obj, targetType));
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
