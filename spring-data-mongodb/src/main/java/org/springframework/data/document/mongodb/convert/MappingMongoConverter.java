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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.LocalVariableTableParameterNameDiscoverer;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.document.mongodb.mapping.MappingException;
import org.springframework.data.document.mongodb.mapping.MappingIntrospector;
import org.springframework.data.document.mongodb.mapping.MongoMappingContext;
import org.springframework.data.mapping.annotation.Id;
import org.springframework.data.mapping.annotation.Persistent;
import org.springframework.data.mapping.annotation.Transient;
import org.springframework.data.mapping.annotation.Value;
import org.springframework.data.mapping.model.PersistentEntity;
import org.springframework.data.mapping.model.PersistentProperty;
import org.springframework.data.mapping.model.types.Association;
import org.springframework.data.mapping.model.types.Simple;
import org.springframework.data.mapping.reflect.ClassPropertyFetcher;
import org.springframework.data.mapping.reflect.ReflectionUtils;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
@SuppressWarnings({"unchecked"})
public class MappingMongoConverter implements MongoConverter, ApplicationContextAware {

  protected static final Log log = LogFactory.getLog(MappingMongoConverter.class);
  protected static final Set<String> SIMPLE_TYPES;
  protected static final ConcurrentMap<Class<?>, Map<String, Field>> fieldsByName = new ConcurrentHashMap<Class<?>, Map<String, Field>>();

  static {
    Set<String> basics = new HashSet<String>();
    basics.add(boolean.class.getName());
    basics.add(long.class.getName());
    basics.add(short.class.getName());
    basics.add(int.class.getName());
    basics.add(byte.class.getName());
    basics.add(float.class.getName());
    basics.add(double.class.getName());
    basics.add(char.class.getName());
    basics.add(Boolean.class.getName());
    basics.add(Long.class.getName());
    basics.add(Short.class.getName());
    basics.add(Integer.class.getName());
    basics.add(Byte.class.getName());
    basics.add(Float.class.getName());
    basics.add(Double.class.getName());
    basics.add(Character.class.getName());
    basics.add(String.class.getName());
    basics.add(java.util.Date.class.getName());
    basics.add(Locale.class.getName());
    basics.add(Class.class.getName());
    basics.add(DBRef.class.getName());
    basics.add(Pattern.class.getName());
    basics.add(CodeWScope.class.getName());
    basics.add(ObjectId.class.getName());
    // TODO check on enums..
    basics.add(Enum.class.getName());
    SIMPLE_TYPES = Collections.unmodifiableSet(basics);
  }

  protected GenericConversionService conversionService = ConversionServiceFactory.createDefaultConversionService();
  protected MongoMappingContext mappingContext;
  protected ApplicationContext applicationContext;
  protected boolean autowirePersistentBeans = false;

  public MappingMongoConverter() {
    initializeConverters();
  }

  public MappingMongoConverter(MongoMappingContext mappingContext) {
    this.mappingContext = mappingContext;
    initializeConverters();
  }

  public MappingMongoConverter(MongoMappingContext mappingContext, List<Converter<?, ?>> converters) {
    this.mappingContext = mappingContext;
    if (null != converters) {
      for (Converter<?, ?> c : converters) {
        conversionService.addConverter(c);
      }
    }
    initializeConverters();
  }

  public MongoMappingContext getMappingContext() {
    return mappingContext;
  }

  public void setMappingContext(MongoMappingContext mappingContext) {
    this.mappingContext = mappingContext;
  }

  public boolean isAutowirePersistentBeans() {
    return autowirePersistentBeans;
  }

  public void setAutowirePersistentBeans(boolean autowirePersistentBeans) {
    this.autowirePersistentBeans = autowirePersistentBeans;
  }

  public <T> T convertObjectId(ObjectId id, Class<T> targetType) {
    return conversionService.convert(id, targetType);
  }

  public ObjectId convertObjectId(Object id) {
    return conversionService.convert(id, ObjectId.class);
  }

  public <S extends Object> S read(Class<S> clazz, final DBObject dbo) {
    final StandardEvaluationContext ctx = new StandardEvaluationContext();
    if (null != applicationContext) {
      ctx.setBeanResolver(new BeanFactoryResolver(applicationContext));
    }
    String[] keySet = dbo.keySet().toArray(new String[]{});
    for (String key : keySet) {
      ctx.setVariable(key, dbo.get(key));
    }

    try {
      if ((clazz.isArray() || clazz.isAssignableFrom(Collection.class)) && dbo instanceof BasicDBList) {
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

      final MappingIntrospector<S> introspector = MappingIntrospector.getInstance(clazz);
      final List<String> cparamNames = new ArrayList<String>();
      final S instance = introspector.createInstance(new MappingIntrospector.ParameterValueProvider() {
        public <T> T getParameterValue(String name, Class<T> type, Expression spelExpr) {
          Object o = getValueInternal(name, type, dbo, ctx, spelExpr);
          if (null != o && o.getClass().isAssignableFrom(type)) {
            cparamNames.add(name);
            return (T) o;
          } else {
            cparamNames.add(name);
            return conversionService.convert(o, type);
          }
        }
      });
      introspector.doWithProperties(new MappingIntrospector.PropertyHandler() {
        public void doWithProperty(PropertyDescriptor descriptor, Field field, Expression spelExpr) {
          String name = descriptor.getName();
          if (!cparamNames.contains(name)) {
            Object o = getValueInternal(name, descriptor.getPropertyType(), dbo, ctx, spelExpr);
            try {
              introspector.setValue(name, instance, o);
            } catch (MappingException e) {
              log.error(e.getMessage(), e);
            }
          }
        }
      });

      introspector.maybeAutowire(instance, applicationContext, autowirePersistentBeans);

      return instance;
    } catch (MappingException e) {
      throw new RuntimeException(e);
    }
  }

  public void write(final Object o, final DBObject dbo) {
    try {
      final MappingIntrospector<?> introspector = MappingIntrospector.getInstance(o.getClass());

      Field idFld = introspector.getIdField();
      if (null != idFld) {
        Object idObj = introspector.getFieldValue(idFld.getName(), o);
        if (null != idObj) {
          dbo.put("_id", conversionService.convert(idObj, ObjectId.class));
        }
      }

      introspector.doWithProperties(new MappingIntrospector.PropertyHandler() {
        public void doWithProperty(PropertyDescriptor descriptor, Field field, Expression spelExpr) {
          String name = descriptor.getName();
          try {
            Object newObj = introspector.getFieldValue(name, o);
            if (null != newObj) {
              if (MappingIntrospector.isSimpleType(newObj.getClass())) {
                dbo.put(name, newObj);
              } else {
                if (newObj.getClass().isAssignableFrom(Collection.class)) {
                  BasicDBList dbList = new BasicDBList();
                  write(newObj, dbList);
                  dbo.put(name, dbList);
                } else {
                  DBObject newDbObj = new BasicDBObject();
                  write(newObj, newDbObj);
                  dbo.put(name, newDbObj);
                }
              }
            }
          } catch (MappingException e) {
            log.error(e.getMessage(), e);
          }
        }
      });

      // Handle mapping-specific stuff
      if (introspector.isMappable()) {
        introspector.doWithAssociations(new MappingIntrospector.AssociationHandler() {
          public void doWithAssociation(MappingIntrospector.Association association) {
            log.info("HANDLE ASSOCIATION: " + association);
          }
        });
      }

    } catch (MappingException e) {
      log.error(e.getMessage(), e);
    }
  }

  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  protected boolean isTransientField(Field f) {
    return (Modifier.isTransient(f.getModifiers()) || null != f.getAnnotation(Transient.class) || null != f.getAnnotation(Autowired.class));
  }

  protected boolean isPersistentProperty(Object obj, PropertyDescriptor descriptor) {
    try {
      Field f = obj.getClass().getDeclaredField(descriptor.getName());

      return false;
    } catch (NoSuchFieldException e) {
      return false;
    }
  }

  protected void initializeConverters() {
    if (!conversionService.canConvert(ObjectId.class, String.class)) {
      conversionService.addConverter(ObjectIdToStringConverter.INSTANCE);
      conversionService.addConverter(StringToObjectIdConverter.INSTANCE);
    }
    if (!conversionService.canConvert(ObjectId.class, BigInteger.class)) {
      conversionService.addConverter(ObjectIdToBigIntegerConverter.INSTANCE);
      conversionService.addConverter(BigIntegerToIdConverter.INSTANCE);
    }
    if (!conversionService.canConvert(ObjectId.class, Integer.class)) {
      conversionService.addConverter(IntegerToIdConverter.INSTANCE);
      conversionService.addConverter(IdToIntegerConverter.INSTANCE);
    }
    if (!conversionService.canConvert(Object.class, DBObject.class)) {
      conversionService.addConverter(new ObjectToDBObjectConverter());
    }
  }

  protected Object getValueInternal(String name, Class<?> type, DBObject dbo, StandardEvaluationContext ctx, Expression spelExpr) {
    Object o;
    if (null != spelExpr) {
      o = spelExpr.getValue(ctx);
    } else {
      Object dbObj = dbo.get(name);
      if (dbObj instanceof DBObject) {
        // It's a complex object, have to read it in
        o = read(type, (DBObject) dbObj);
      } else {
        o = dbObj;
      }
    }
    return o;
  }

  protected static boolean isSimpleType(Class<?> propertyType) {
    if (propertyType == null) {
      return false;
    }
    if (propertyType.isArray()) {
      return isSimpleType(propertyType.getComponentType());
    }
    return SIMPLE_TYPES.contains(propertyType.getName());
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

  public static enum IntegerToIdConverter implements Converter<Integer, ObjectId> {
    INSTANCE;

    public ObjectId convert(Integer source) {
      String s = String.valueOf(source);
      StringBuffer buff = new StringBuffer();
      for (int i = 0; i < (16 - s.length()); i++) {
        buff.append("0");
      }
      buff.append(s);
      return new ObjectId(buff.toString());
    }
  }

  public static enum IdToIntegerConverter implements Converter<ObjectId, Integer> {
    INSTANCE;

    public Integer convert(ObjectId source) {
      return Integer.parseInt(source.toString());
    }
  }

  protected class ObjectToDBObjectConverter implements Converter<Object, DBObject> {

    private final DBObject dbo;

    public ObjectToDBObjectConverter() {
      this.dbo = null;
    }

    public ObjectToDBObjectConverter(DBObject dbo) {
      this.dbo = dbo;
    }

    public DBObject convert(Object source) {
      DBObject dbo;
      if (source instanceof Collection) {
        Collection c = (Collection) source;
        dbo = (null == this.dbo ? new BasicDBList() : this.dbo);
        for (Object o : c) {
          if (isSimpleType(o.getClass())) {
            ((BasicDBList) dbo).add(o);
          } else {
            ((BasicDBList) dbo).add(convert(o));
          }
        }
      } else if (source instanceof Map) {
        Map<Object, Object> m = (Map) source;
        dbo = (null == this.dbo ? new BasicDBObject() : this.dbo);
        for (Map.Entry<Object, Object> entry : m.entrySet()) {
          String key = (entry.getKey() instanceof String ? entry.getKey().toString() : conversionService.convert(entry.getKey(), String.class));
          if (isSimpleType(entry.getValue().getClass())) {
            dbo.put(key, entry.getValue());
          } else {
            dbo.put(key, convert(entry.getValue()));
          }
        }
      } else {
        dbo = (null == this.dbo ? new BasicDBObject() : this.dbo);
        if (!fieldsByName.containsKey(source.getClass())) {
          Map<String, Field> fields = new HashMap<String, Field>();
          for (Field f : source.getClass().getDeclaredFields()) {
            if (!"class".equals(f.getName())) {
              ReflectionUtils.makeAccessible(f);
              fields.put(f.getName(), f);
            }
          }
          fieldsByName.put(source.getClass(), fields);
        }
        try {
          for (PropertyDescriptor descriptor : Introspector.getBeanInfo(source.getClass()).getPropertyDescriptors()) {
            Field f = fieldsByName.get(source.getClass()).get(descriptor.getName());
            if (null != f && !isTransientField(f)) {
              try {
                Object o;
                if (null != descriptor.getReadMethod()) {
                  o = descriptor.getReadMethod().invoke(source);
                } else {
                  o = f.get(source);
                }
                if (null != o && isSimpleType(o.getClass())) {
                  dbo.put(descriptor.getName(), o);
                } else if (null != o) {
                  dbo.put(descriptor.getName(), convert(o));
                } else {
                  // Value was NULL, skip it
                }
              } catch (InvocationTargetException e) {
                throw new RuntimeException("Error converting " + source + " to DBObject");
              } catch (IllegalAccessException e) {
                throw new RuntimeException("Error converting " + source + " to DBObject");
              }
            }
          }
        } catch (IntrospectionException e) {
          throw new RuntimeException("Error converting " + source + " to DBObject");
        }
      }
      return dbo;
    }
  }

}
