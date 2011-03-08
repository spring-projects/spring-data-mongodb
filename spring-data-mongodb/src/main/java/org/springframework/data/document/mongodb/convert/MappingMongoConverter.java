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
import org.bson.types.ObjectId;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.document.mongodb.mapping.MappingException;
import org.springframework.data.document.mongodb.mapping.MappingIntrospector;
import org.springframework.data.document.mongodb.mapping.MongoMappingContext;
import org.springframework.data.mapping.model.MappingInstantiationException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.springframework.data.document.mongodb.mapping.MappingIntrospector.isSimpleType;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
@SuppressWarnings({"unchecked"})
public class MappingMongoConverter implements MongoConverter, ApplicationContextAware {

  protected static final Log log = LogFactory.getLog(MappingMongoConverter.class);
  protected static final ConcurrentMap<Class<?>, Map<String, Field>> fieldsByName = new ConcurrentHashMap<Class<?>, Map<String, Field>>();

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
    if (null == dbo) {
      return null;
    }

    final StandardEvaluationContext ctx = new StandardEvaluationContext();
    if (null != applicationContext) {
      ctx.setBeanResolver(new BeanFactoryResolver(applicationContext));
    }
    String[] keySet = dbo.keySet().toArray(new String[]{});
    for (String key : keySet) {
      ctx.setVariable(key, dbo.get(key));
    }

    try {
      if ((clazz.isArray() || (clazz.isAssignableFrom(Collection.class) || clazz.isAssignableFrom(List.class))) && dbo instanceof BasicDBList) {
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
          if (o instanceof DBRef) {
            cparamNames.add(name);
            return read(type, ((DBRef) o).fetch());
          } else if (null != o && o.getClass().isAssignableFrom(type)) {
            if (type == Object.class && dbo.containsField("_class")) {
              try {
                cparamNames.add(name);
                return (T) conversionService.convert(o, Class.forName(dbo.get("_class").toString()));
              } catch (ClassNotFoundException e) {
                throw new MappingInstantiationException(e.getMessage(), e);
              }
            } else {
              cparamNames.add(name);
              return (T) o;
            }
          } else {
            cparamNames.add(name);
            return conversionService.convert(o, type);
          }
        }
      });

      // The id is important. Set it first.
      if (dbo.containsField("_id")) {
        introspector.setValue(introspector.getIdField().getName(), instance, convertObjectId((ObjectId) dbo.get("_id"), introspector.getIdField().getType()));
      }

      introspector.doWithProperties(new MappingIntrospector.PropertyHandler() {
        public void doWithProperty(PropertyDescriptor descriptor, Field field, Expression spelExpr) {
          String name = descriptor.getName();
          if (!cparamNames.contains(name)) {
            Object o = getValueInternal(name, descriptor.getPropertyType(), dbo, ctx, spelExpr);
            try {
              if (null != descriptor.getWriteMethod()) {
                Class<?> requiredType = descriptor.getWriteMethod().getParameterTypes()[0];
                if (null != o && o.getClass().isAssignableFrom(requiredType)) {
                  introspector.setValue(name, instance, o);
                } else {
                  introspector.setValue(name, instance, conversionService.convert(o, requiredType));
                }
              } else {
                introspector.setValue(name, instance, o);
              }
            } catch (MappingException e) {
              log.error(e.getMessage(), e);
            }
          }
        }
      });

      // Handle mapping-specific stuff
      if (introspector.isMappable()) {
        introspector.doWithAssociations(new MappingIntrospector.AssociationHandler() {
          public void doWithAssociation(MappingIntrospector.Association association) {
            String name = association.getDescriptor().getName();
            Object targetObj = dbo.get(name);
            if (null != targetObj) {
              if (targetObj instanceof DBRef) {
                targetObj = read(association.getTargetClass(), ((DBRef) targetObj).fetch());
              } else if (targetObj instanceof BasicDBList) {
                BasicDBList dbList = (BasicDBList) targetObj;
                Collection<Object> targets = CollectionFactory.createCollection(association.getField().getType(), dbList.size());
                for (Object tgtDbObj : dbList) {
                  if (tgtDbObj instanceof DBRef) {
                    targets.add(read(association.getTargetClass(), ((DBRef) tgtDbObj).fetch()));
                  } else if (tgtDbObj instanceof DBObject) {
                    targets.add(read(association.getTargetClass(), (DBObject) tgtDbObj));
                  } else if (tgtDbObj.getClass().isAssignableFrom(association.getTargetClass())) {
                    targets.add(tgtDbObj);
                  } else {
                    targets.add(conversionService.convert(tgtDbObj, association.getTargetClass()));
                  }
                }
                targetObj = targets;
              }
              introspector.setValue(name, instance, targetObj);
            }
          }
        });
      }

      introspector.maybeAutowire(instance, applicationContext, autowirePersistentBeans);

      if (log.isDebugEnabled()) {
        log.debug("Handing back configured object: " + instance);
      }
      return instance;
    } catch (MappingException e) {
      throw new RuntimeException(e);
    }
  }

  public void write(final Object o, final DBObject dbo) {
    try {
      final MappingIntrospector<?> introspector = MappingIntrospector.getInstance(o.getClass());
      //final PersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(o.getClass().getName());

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
              if (isSimpleType(newObj.getClass())) {
                dbo.put(name, newObj);
              } else {
                if (newObj.getClass().isArray() || newObj.getClass().isAssignableFrom(Collection.class)) {
                  BasicDBList dbList = new BasicDBList();
                  for (Object collObj : (Object[]) (newObj.getClass().isArray() ? newObj : ((Collection) newObj).toArray())) {
                    BasicDBObject newDbObj = new BasicDBObject();
                    write(collObj, newDbObj);
                    dbList.add(newDbObj);
                  }
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
            String name = association.getDescriptor().getName();
            Object targetObj = introspector.getFieldValue(name, o);
            if (null != targetObj) {
              if (isSimpleType(targetObj.getClass())) {
                dbo.put(name, targetObj);
              } else if (targetObj instanceof Collection || targetObj instanceof List) {
                // We're doing a ToMany
                BasicDBList dbRefList = new BasicDBList();
                for (Object depObj : (targetObj instanceof Collection ? (Collection) targetObj : (List) targetObj)) {
                  DBObject depDbRef = createDBRef(depObj, depObj.getClass());
                  if (null != depDbRef) {
                    dbo.put(name, depDbRef);
                  }
                  dbRefList.add(depDbRef);
                }
                dbo.put(name, dbRefList);
              } else {
                DBObject depDbRef = createDBRef(targetObj, association.getTargetClass());
                if (null != depDbRef) {
                  dbo.put(name, depDbRef);
                }
              }
            }
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
  }

  protected Object getValueInternal(String name, Class<?> type, DBObject dbo, StandardEvaluationContext ctx, Expression spelExpr) {
    Object o;
    if (null != spelExpr) {
      o = spelExpr.getValue(ctx);
    } else {
      DBObject from = dbo;
      if (dbo instanceof DBRef) {
        from = ((DBRef) dbo).fetch();
      }
      Object dbObj = from.get(name);
      if (dbObj instanceof DBObject) {
        // It's a complex object, have to read it in
        o = read(type, (DBObject) dbObj);
      } else {
        o = dbObj;
      }
    }
    return o;
  }

  protected DBObject createDBRef(Object obj, Class<?> targetClass) {
    if (null != obj) {
      MappingIntrospector<?> introspector = MappingIntrospector.getInstance(obj.getClass());
      if (introspector.isMappable()) {
        String idName = introspector.getIdField().getName();
        Object idVal = introspector.getFieldValue(idName, obj);
        BasicDBObject dbRefDbo = new BasicDBObject();
        dbRefDbo.put("$ref", targetClass.getSimpleName().toLowerCase());
        dbRefDbo.put("$id", convertObjectId(idVal));
        return dbRefDbo;
      }
    }
    return null;
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

}
