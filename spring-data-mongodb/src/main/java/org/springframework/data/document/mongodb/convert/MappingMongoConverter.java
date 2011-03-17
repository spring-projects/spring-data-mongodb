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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingBeanHelper;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.Association;
import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PersistentEntity;
import org.springframework.data.mapping.model.PersistentProperty;
import org.springframework.data.mapping.model.PreferredConstructor;
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
public class MappingMongoConverter implements MongoConverter, ApplicationContextAware {

  private static final String CUSTOM_TYPE_KEY = "_class";
  private static final List<Class<?>> MONGO_TYPES = Arrays.asList(Number.class, Date.class, String.class, DBObject.class);
  protected static final Log log = LogFactory.getLog(MappingMongoConverter.class);

  protected final GenericConversionService conversionService = ConversionServiceFactory.createDefaultConversionService();
  protected final Map<Class<?>, Class<?>> customTypeMapping = new HashMap<Class<?>, Class<?>>();
  protected SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
  protected MappingContext mappingContext;
  protected ApplicationContext applicationContext;
  protected boolean autowirePersistentBeans = false;
  protected boolean useFieldAccessOnly = true;
  protected Mongo mongo;
  protected String defaultDatabase;

  public MappingMongoConverter() {
    initializeConverters();
  }

  public MappingMongoConverter(MappingContext mappingContext) {
    this.mappingContext = mappingContext;
    initializeConverters();
  }

  public MappingMongoConverter(MappingContext mappingContext, List<Converter<?, ?>> converters) {
    this.mappingContext = mappingContext;
    if (null != converters) {
      for (Converter<?, ?> c : converters) {
        registerConverter(c);
        conversionService.addConverter(c);
      }
    }
    initializeConverters();
  }

  /**
   * Inspects the given {@link Converter} for the types it can convert and registers the pair for custom type conversion
   * in case the target type is a Mongo basic type.
   * 
   * @param converter
   */
  private void registerConverter(Converter<?, ?> converter) {
    Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), Converter.class);
    if (MONGO_TYPES.contains(arguments[1])) {
      customTypeMapping.put(arguments[0], arguments[1]);
    }
  }

  public MappingContext getMappingContext() {
    return mappingContext;
  }

  public void setMappingContext(MappingContext mappingContext) {
    this.mappingContext = mappingContext;
  }

  public Mongo getMongo() {
    return mongo;
  }

  public void setMongo(Mongo mongo) {
    this.mongo = mongo;
  }

  public String getDefaultDatabase() {
    return defaultDatabase;
  }

  public void setDefaultDatabase(String defaultDatabase) {
    this.defaultDatabase = defaultDatabase;
  }

  public boolean isAutowirePersistentBeans() {
    return autowirePersistentBeans;
  }

  public void setAutowirePersistentBeans(boolean autowirePersistentBeans) {
    this.autowirePersistentBeans = autowirePersistentBeans;
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
    PersistentEntity<S> persistentEntity = mappingContext.getPersistentEntity(clazz);
    if (persistentEntity == null) {
      persistentEntity = mappingContext.addPersistentEntity(clazz);
    }

    return read(persistentEntity, dbo);
  }

  private <S extends Object> S read(PersistentEntity<S> entity, final DBObject dbo) {

    final StandardEvaluationContext spelCtx = new StandardEvaluationContext();
    if (null != applicationContext) {
      spelCtx.setBeanResolver(new BeanFactoryResolver(applicationContext));
    }
    String[] keySet = dbo.keySet().toArray(new String[]{});
    for (String key : keySet) {
      spelCtx.setVariable(key, dbo.get(key));
    }

    final List<String> ctorParamNames = new ArrayList<String>();
    final S instance = MappingBeanHelper.constructInstance(entity, new PreferredConstructor.ParameterValueProvider() {
      public Object getParameterValue(PreferredConstructor.Parameter parameter) {
        String name = parameter.getName();
        Class<?> type = parameter.getType();
        Object obj = dbo.get(name);
        if (obj instanceof DBRef) {
          ctorParamNames.add(name);
          return read(type, ((DBRef) obj).fetch());
        } else if (obj instanceof DBObject) {
          ctorParamNames.add(name);
          return read(type, ((DBObject) obj));
        } else if (null != obj && obj.getClass().isAssignableFrom(type)) {
          ctorParamNames.add(name);
          return obj;
        } else if (null != obj) {
          ctorParamNames.add(name);
          return conversionService.convert(obj, type);
        }

        return null;
      }
    }, spelCtx);

    // Set the ID
    PersistentProperty idProperty = entity.getIdProperty();
    if (dbo.containsField("_id") || null != idProperty) {
      Object idObj = dbo.get("_id");
      try {
        MappingBeanHelper.setProperty(instance, idProperty, idObj, useFieldAccessOnly);
      } catch (IllegalAccessException e) {
        throw new MappingException(e.getMessage(), e);
      } catch (InvocationTargetException e) {
        throw new MappingException(e.getMessage(), e);
      }
    }

    // Set properties not already set in the constructor
    entity.doWithProperties(new PropertyHandler() {
      public void doWithPersistentProperty(PersistentProperty prop) {
        if (!ctorParamNames.contains(prop.getName())) {
          Object obj = getValueInternal(prop, dbo, spelCtx, prop.getValueAnnotation());
          try {
            MappingBeanHelper.setProperty(instance, prop, obj, useFieldAccessOnly);
          } catch (IllegalAccessException e) {
            throw new MappingException(e.getMessage(), e);
          } catch (InvocationTargetException e) {
            throw new MappingException(e.getMessage(), e);
          }
        }

        if (ctorParamNames.contains(prop.getName())) {
          return;
        }

        Object obj = getValueInternal(prop, dbo, spelCtx, prop.getValueAnnotation());
        try {
          MappingBeanHelper.setProperty(instance, prop, obj, useFieldAccessOnly);
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
        Object obj = getValueInternal(inverseProp, dbo, spelCtx, inverseProp.getValueAnnotation());
        try {
          MappingBeanHelper.setProperty(instance, inverseProp, obj);
        } catch (IllegalAccessException e) {
          throw new MappingException(e.getMessage(), e);
        } catch (InvocationTargetException e) {
          throw new MappingException(e.getMessage(), e);
        }
      }
    });

    if (null != applicationContext && autowirePersistentBeans) {
      applicationContext.getAutowireCapableBeanFactory().autowireBean(instance);
    }
    if (instance instanceof InitializingBean) {
      try {
        ((InitializingBean) instance).afterPropertiesSet();
      } catch (Exception e) {
        throw new MappingException(e.getMessage(), e);
      }
    }

    return instance;
  }

  public void write(final Object obj, final DBObject dbo) {
    if (null == obj) {
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
      // Must not have explictly added this entity yet
      entity = mappingContext.addPersistentEntity(obj.getClass());
      if (null == entity) {
        // We can't map this entity for some reason
        throw new MappingException("Unable to map entity " + obj);
      }
    }

    // Write the ID
    final PersistentProperty idProperty = entity.getIdProperty();
    if (!dbo.containsField("_id") && null != idProperty) {
      Object idObj = null;
      try {
        idObj = MappingBeanHelper.getProperty(obj, idProperty, ObjectId.class, useFieldAccessOnly);
      } catch (IllegalAccessException e) {
        throw new MappingException(e.getMessage(), e);
      } catch (InvocationTargetException e) {
        throw new MappingException(e.getMessage(), e);
      }

      if (null != idObj) {
        dbo.put("_id", idObj);
      }
    }

    // Write the properties
    entity.doWithProperties(new PropertyHandler() {
      public void doWithPersistentProperty(PersistentProperty prop) {
        String name = prop.getName();
        if (null != idProperty && name.equals(idProperty.getName())) {
          return;
        }
        if (prop.isAssociation()) {
          return;
        }

        Class<?> type = prop.getType();
        Object propertyObj = null;
        try {
          propertyObj = MappingBeanHelper.getProperty(obj, prop, type, useFieldAccessOnly);
        } catch (IllegalAccessException e) {
          throw new MappingException(e.getMessage(), e);
        } catch (InvocationTargetException e) {
          throw new MappingException(e.getMessage(), e);
        }
        if (null != propertyObj) {
          if (prop.isComplexType()) {
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
        Object propertyObj = null;
        try {
          propertyObj = MappingBeanHelper.getProperty(obj, inverseProp, type, useFieldAccessOnly);
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
  protected void initializeConverters() {
    
    this.conversionService.removeConvertible(Object.class, String.class);
    
    if (!conversionService.canConvert(ObjectId.class, String.class)) {
      conversionService.addConverter(ObjectIdToStringConverter.INSTANCE);
      conversionService.addConverter(StringToObjectIdConverter.INSTANCE);
    }
    if (!conversionService.canConvert(ObjectId.class, BigInteger.class)) {
      conversionService.addConverter(ObjectIdToBigIntegerConverter.INSTANCE);
      conversionService.addConverter(BigIntegerToIdConverter.INSTANCE);
    }
    
    MappingBeanHelper.setConversionService(conversionService);
  }

  @SuppressWarnings({"unchecked"})
  protected void writePropertyInternal(PersistentProperty prop, Object obj, DBObject dbo) {
    org.springframework.data.document.mongodb.mapping.DBRef dbref = prop.getField()
        .getAnnotation(org.springframework.data.document.mongodb.mapping.DBRef.class);
    
    String name = prop.getName();
    
    if (prop.isCollection()) {
      Class<?> type = prop.getType();
      BasicDBList dbList = new BasicDBList();
      Collection<?> coll = (type.isArray() ? Arrays.asList((Object[]) obj) : (Collection<?>) obj);
      for (Object propObjItem : coll) {
        if (null != dbref) {
          DBRef dbRef = createDBRef(propObjItem, dbref);
          dbList.add(dbRef);
        } else {
          BasicDBObject propDbObj = new BasicDBObject();
          write(propObjItem, propDbObj, mappingContext.getPersistentEntity(prop.getTypeInformation()));
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
    Class<?> basicTargetType = customTypeMapping.get(obj.getClass());

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
      if (MappingBeanHelper.isSimpleType(key.getClass())) {
        String simpleKey = conversionService.convert(key, String.class);
        if (MappingBeanHelper.isSimpleType(val.getClass())) {
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
      id = MappingBeanHelper.getProperty(target, idProperty, ObjectId.class, useFieldAccessOnly);
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
  protected Object getValueInternal(PersistentProperty prop, DBObject dbo, StandardEvaluationContext ctx, Value spelExpr) {
    String name = prop.getName();
    Object o;
    if (null != spelExpr) {
      Expression x = spelExpressionParser.parseExpression(spelExpr.value());
      o = x.getValue(ctx);
    } else {
      DBObject from = dbo;
      if (dbo instanceof DBRef) {
        from = ((DBRef) dbo).fetch();
      }
      Object dbObj = from.get(name);
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
          Object[] items = (Object[]) Array.newInstance(prop.getComponentType(), dbObjList.size());
          for (int i = 0; i < dbObjList.size(); i++) {
            Object dbObjItem = dbObjList.get(i);
            if (dbObjItem instanceof DBRef) {
              items[i] = read(prop.getComponentType(), ((DBRef) dbObjItem).fetch());
            } else if (dbObjItem instanceof DBObject) {
              items[i] = read(prop.getComponentType(), (DBObject) dbObjItem);
            } else {
              items[i] = dbObjItem;
            }
          }
          return Arrays.asList(items);
        }

        // It's a complex object, have to read it in
        if (dbo.containsField("_class")) {
          Class<?> toType = findTypeToBeUsed((DBObject) dbObj);
          dbo.removeField("_class");
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
  private Class<?> findTypeToBeUsed(DBObject dbObject) {
    Object classToBeUsed = dbObject.get(CUSTOM_TYPE_KEY);

    try {
      return Class.forName(classToBeUsed.toString());
    } catch (ClassNotFoundException e) {
      throw new MappingException(e.getMessage(), e);
    }
  }

  /**
   * Simple singleton to convert {@link ObjectId}s to their {@link String} representation.
   *
   * @author Oliver Gierke
   */
  public static enum ObjectIdToStringConverter implements Converter<ObjectId, String> {
    INSTANCE;

    public String convert(ObjectId id) {
      return id.toString();
    }
  }

  /**
   * Simple singleton to convert {@link String}s to their {@link ObjectId} representation.
   *
   * @author Oliver Gierke
   */
  public static enum StringToObjectIdConverter implements Converter<String, ObjectId> {
    INSTANCE;

    public ObjectId convert(String source) {
      return new ObjectId(source);
    }
  }

  /**
   * Simple singleton to convert {@link ObjectId}s to their {@link java.math.BigInteger} representation.
   *
   * @author Oliver Gierke
   */
  public static enum ObjectIdToBigIntegerConverter implements Converter<ObjectId, BigInteger> {
    INSTANCE;

    public BigInteger convert(ObjectId source) {
      return new BigInteger(source.toString(), 16);
    }
  }

  /**
   * Simple singleton to convert {@link BigInteger}s to their {@link ObjectId} representation.
   *
   * @author Oliver Gierke
   */
  public static enum BigIntegerToIdConverter implements Converter<BigInteger, ObjectId> {
    INSTANCE;

    public ObjectId convert(BigInteger source) {
      return new ObjectId(source.toString(16));
    }
  }

  protected class PersistentPropertyWrapper {
    private final PersistentProperty property;
    private final DBObject target;

    public PersistentPropertyWrapper(PersistentProperty property, DBObject target) {
      this.property = property;
      this.target = target;
    }

    public PersistentProperty getProperty() {
      return property;
    }

    public DBObject getTarget() {
      return target;
    }
  }
}
