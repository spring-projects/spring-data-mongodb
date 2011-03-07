package org.springframework.data.document.mongodb.mapping;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import org.springframework.dao.DataAccessException;
import org.springframework.data.document.mongodb.index.CompoundIndex;
import org.springframework.data.document.mongodb.index.CompoundIndexes;
import org.springframework.data.document.mongodb.index.IndexDirection;
import org.springframework.data.document.mongodb.index.Indexed;
import org.springframework.data.document.mongodb.CollectionCallback;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.mapping.annotation.*;
import org.springframework.data.mapping.model.*;
import org.springframework.data.mapping.model.types.Association;
import org.springframework.data.mapping.reflect.ClassPropertyFetcher;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author J. Brisbin <jbrisbin@vmware.com>
 */
public class MongoMappingConfigurationStrategy implements MappingConfigurationStrategy {

  protected MongoTemplate mongo;
  protected MongoMappingFactory mappingFactory;
  protected SpelExpressionParser expressionParser = new SpelExpressionParser();
  protected TemplateParserContext templateParserContext = new TemplateParserContext();
  protected Map<Class<?>, PersistenceDescriptor> descriptors = new ConcurrentHashMap<Class<?>, PersistenceDescriptor>();
  protected Map<Class<?>, Set<PersistentEntity>> owners = new LinkedHashMap<Class<?>, Set<PersistentEntity>>();

  public MongoMappingConfigurationStrategy(MongoTemplate mongo) {
    this.mongo = mongo;
  }

  public MongoMappingConfigurationStrategy(MongoTemplate mongo, MongoMappingFactory mappingFactory) {
    this.mongo = mongo;
    this.mappingFactory = mappingFactory;
  }

  public SpelExpressionParser getExpressionParser() {
    return expressionParser;
  }

  public void setExpressionParser(SpelExpressionParser expressionParser) {
    this.expressionParser = expressionParser;
  }

  public TemplateParserContext getTemplateParserContext() {
    return templateParserContext;
  }

  public void setTemplateParserContext(TemplateParserContext templateParserContext) {
    this.templateParserContext = templateParserContext;
  }

  @SuppressWarnings({"unchecked"})
  public boolean isPersistentEntity(Class aClass) {
    if (null != aClass) {
      return (null != aClass.getAnnotation(Persistent.class));
    }
    return false;
  }

  public List<PersistentProperty> getPersistentProperties(Class aClass, MappingContext mappingContext) {
    return getPersistentProperties(aClass, mappingContext, null);
  }

  public List<PersistentProperty> getPersistentProperties(Class aClass,
                                                          MappingContext mappingContext,
                                                          ClassMapping classMapping) {
    PersistenceDescriptor pd = getPersistenceDescriptor(aClass, mappingContext, classMapping);
    return (null != pd ? pd.getProperties() : null);
  }

  public PersistentProperty getIdentity(Class aClass, MappingContext mappingContext) {
    PersistenceDescriptor idPd = getPersistenceDescriptor(aClass, mappingContext, null);
    return (null != idPd ? idPd.getIdProperty() : null);
  }

  public IdentityMapping getDefaultIdentityMapping(final ClassMapping classMapping) {
    final PersistentProperty<?> prop = getPersistenceDescriptor(classMapping.getEntity().getJavaClass(),
        classMapping.getEntity().getMappingContext(),
        classMapping).getIdProperty();

    return new IdentityMapping() {
      public String[] getIdentifierName() {
        return new String[]{prop.getName()};
      }

      public ClassMapping getClassMapping() {
        return classMapping;
      }

      public Object getMappedForm() {
        return classMapping.getMappedForm();
      }
    };
  }

  public Set getOwningEntities(Class aClass, MappingContext mappingContext) {
    return owners.get(aClass);
  }

  protected PersistenceDescriptor getPersistenceDescriptor(Class<?> javaClass,
                                                           MappingContext context,
                                                           ClassMapping mapping) {
    PersistenceDescriptor descriptor = descriptors.get(javaClass);
    if (null == descriptor) {
      ClassPropertyFetcher fetcher = ClassPropertyFetcher.forClass(javaClass);
      PersistentEntity entity = getPersistentEntity(javaClass, context, mapping);

      String collection = javaClass.getSimpleName().toLowerCase();
      for (Annotation anno : javaClass.getAnnotations()) {
        if (anno instanceof Persistent) {

        } else if (anno instanceof CompoundIndexes) {
          CompoundIndexes idxs = (CompoundIndexes) anno;
          for (CompoundIndex idx : idxs.value()) {
            String idxColl = collection;
            if (!"".equals(idx.collection())) {
              idxColl = idx.collection();
            }
            String name = null;
            if (!"".equals(idx.name())) {
              name = idx.name();
            }
            ensureIndex(idxColl, name, idx.def(), null, idx.unique(), idx.dropDups(), idx.sparse());
          }
        }
      }

      EvaluationContext elContext = createElContext();
      elContext.setVariable("class", javaClass);

      PersistentProperty<?> id = extractIdProperty(entity, context, elContext);

      List<PersistentProperty> properties = new LinkedList<PersistentProperty>();
      for (PropertyDescriptor propertyDescriptor : fetcher.getPropertyDescriptors()) {
        if (null == id || !propertyDescriptor.getName().equals(id.getName())) {
          PersistentProperty<?> p = createPersistentProperty(entity, context, propertyDescriptor, mapping);
          if (null != p) {
            properties.add(p);
            for (Annotation anno : fetcher.getDeclaredField(p.getName()).getDeclaredAnnotations()) {
              if (anno instanceof Indexed) {
                Indexed idx = (Indexed) anno;
                String idxColl = collection;
                if (!"".equals(idx.collection())) {
                  idxColl = idx.collection();
                }
                String name = p.getName();
                if (!"".equals(idx.name())) {
                  name = idx.name();
                }
                ensureIndex(idxColl, name, null, idx.direction(), idx.unique(), idx.dropDups(), idx.sparse());
              }
            }
          }
        }
      }
      descriptor = new PersistenceDescriptor(entity, id, properties);
      descriptors.put(javaClass, descriptor);
    }
    return descriptor;
  }

  protected PersistentEntity getPersistentEntity(Class<?> javaClass, MappingContext context, ClassMapping mapping) {
    Assert.notNull(javaClass);
    if (null != mapping) {
      return mapping.getEntity();
    } else {
      return context.getPersistentEntity(javaClass.getName());
    }
  }

  protected PersistentProperty<?> createPersistentProperty(PersistentEntity entity,
                                                           MappingContext mappingContext,
                                                           PropertyDescriptor descriptor,
                                                           ClassMapping mapping) {
    ClassPropertyFetcher fetcher = ClassPropertyFetcher.forClass(entity.getJavaClass());
    Field f = fetcher.getDeclaredField(descriptor.getName());

    if (null != f) {
      // Handle associations and persistent types
      PersistentProperty<?> prop = extractChildType(entity, mappingContext, descriptor, mapping);
      if (null == prop) {
        // Must be a simple type
        prop = mappingFactory.createSimple(entity, mappingContext, descriptor);
      }
      return prop;
    }

    return null;
  }

  protected PersistentProperty<?> extractChildType(PersistentEntity entity,
                                                   MappingContext mappingContext,
                                                   PropertyDescriptor descriptor,
                                                   ClassMapping mapping) {

    ClassPropertyFetcher fetcher = ClassPropertyFetcher.forClass(entity.getJavaClass());
    Field f = fetcher.getDeclaredField(descriptor.getName());

    Class<?> childClass = null;
    Association<?> assoc = null;
    if (f.getType().isAssignableFrom(List.class)) {
      if (null != f.getAnnotation(org.springframework.data.mapping.annotation.OneToMany.class)) {
        org.springframework.data.mapping.annotation.OneToMany otm = f.getAnnotation(
            org.springframework.data.mapping.annotation.OneToMany.class);
        if (Object.class != otm.targetClass()) {
          childClass = otm.targetClass();
        } else {
          childClass = extractType(f);
        }
        assoc = mappingFactory.createOneToMany(entity, mappingContext, descriptor);
      } else if (null != f.getAnnotation(org.springframework.data.mapping.annotation.ManyToMany.class)) {
        org.springframework.data.mapping.annotation.ManyToMany mtm = f.getAnnotation(
            org.springframework.data.mapping.annotation.ManyToMany.class);
        if (Object.class != mtm.targetClass()) {
          childClass = mtm.targetClass();
        } else {
          childClass = extractType(f);
        }
        assoc = mappingFactory.createManyToMany(entity, mappingContext, descriptor);
      }
    } else {
      if (null != f.getAnnotation(OneToOne.class) || null != f.getType().getAnnotation(Persistent.class)) {
        childClass = f.getType();
        assoc = mappingFactory.createOneToOne(entity, mappingContext, descriptor);
      }
    }
    if (null != childClass && null != assoc) {
      if (childClass != entity.getJavaClass()) {
        PersistentEntity childEntity = getPersistentEntity(childClass, mappingContext, mapping);
        addOwner(entity.getJavaClass(), childEntity);
        assoc.setAssociatedEntity(childEntity);
      } else {
        addOwner(entity.getJavaClass(), entity);
        assoc.setAssociatedEntity(entity);
      }
    }

    return assoc;
  }

  protected Class<?> extractType(Field f) {
    Type t = f.getGenericType();
    if (null != t && t instanceof ParameterizedType) {
      Type[] types = ((ParameterizedType) t).getActualTypeArguments();
      return ((Class) types[0]);
    }
    return f.getType();
  }

  protected IdentifiedBy extractIdentifiedBy(Class<?> clazz, Field field) {
    Persistent cAnno = clazz.getAnnotation(Persistent.class);
    Persistent fldAnno = (null != field ? field.getAnnotation(Persistent.class) : null);
    return (null != fldAnno ? fldAnno.identifiedBy() : cAnno.identifiedBy());
  }

  protected PersistenceStrategy extractPersistenceStrategy(Class<?> clazz, Field field) {
    Persistent cAnno = clazz.getAnnotation(Persistent.class);
    Persistent fldAnno = (null != field ? field.getAnnotation(Persistent.class) : null);
    return (null != fldAnno ? fldAnno.strategy() : cAnno.strategy());
  }

  protected String extractId(Class<?> clazz, Field field) {
    Persistent cAnno = clazz.getAnnotation(Persistent.class);
    Persistent fldAnno = (null != field ? field.getAnnotation(Persistent.class) : null);
    return (null != fldAnno ? fldAnno.id() : cAnno.id());
  }

  protected void addOwner(Class<?> javaClass, PersistentEntity parent) {
    if (!owners.containsKey(javaClass)) {
      Set<PersistentEntity> owningEntities = new HashSet<PersistentEntity>();
      owningEntities.add(parent);
      owners.put(javaClass, owningEntities);
    }
  }

  protected PersistentProperty<?> extractIdProperty(PersistentEntity entity,
                                                    MappingContext mappingContext,
                                                    EvaluationContext elContext) {
    ClassPropertyFetcher fetcher = ClassPropertyFetcher.forClass(entity.getJavaClass());

    // Let field annotation override that on the class
    IdentifiedBy idBy = extractIdentifiedBy(fetcher.getJavaClass(), null);
    String id = extractId(fetcher.getJavaClass(), null);
    Assert.notNull(id);
    if (id.indexOf("#") > -1) {
      id = expressionParser.parseExpression(id).getValue(elContext, String.class);
    }

    PropertyDescriptor idPropDesc = null;
    switch (idBy) {
      case DEFAULT:
        idPropDesc = findIdByAnnotation(fetcher);
        if (null == idPropDesc) {
          idPropDesc = fetcher.getPropertyDescriptor(id);
        }
        break;
      case ANNOTATION:
        idPropDesc = findIdByAnnotation(fetcher);
        break;
      case PROPERTY:
        idPropDesc = fetcher.getPropertyDescriptor(id);
        break;
      case VALUE:
        try {
          idPropDesc = new ValuePropertyDescriptor("id", entity.getJavaClass(), id);
        } catch (IntrospectionException e) {
          throw new IllegalStateException(e.getMessage(), e);
        }
        break;
    }
    //Assert.notNull(idPropDesc, String.format("No ID property could be found on the entity %s", entity));

    return (null != idPropDesc ? mappingFactory.createIdentity(entity, mappingContext, idPropDesc) : null);
  }

  protected PropertyDescriptor findIdByAnnotation(ClassPropertyFetcher fetcher) {
    for (PropertyDescriptor descriptor : fetcher.getPropertyDescriptors()) {
      Field f = fetcher.getDeclaredField(descriptor.getName());
      if (null != f && null != f.getAnnotation(Id.class)) {
        return descriptor;
      }
    }
    return null;
  }

  protected EvaluationContext createElContext() {
    StandardEvaluationContext elContext = new StandardEvaluationContext(System.getProperties());
    for (String prop : System.getProperties().stringPropertyNames()) {
      elContext.setVariable(prop, System.getProperty(prop));
    }
    return elContext;
  }

  protected void ensureIndex(String collection,
                             final String name,
                             final String def,
                             final IndexDirection direction,
                             final boolean unique,
                             final boolean dropDups,
                             final boolean sparse) {
    mongo.execute(collection, new CollectionCallback<Object>() {
      public Object doInCollection(DBCollection collection) throws MongoException, DataAccessException {
        DBObject defObj;
        if (null != def) {
          defObj = (DBObject) JSON.parse(def);
        } else {
          defObj = new BasicDBObject();
          defObj.put(name, (direction == IndexDirection.ASCENDING ? 1 : -1));
        }
        DBObject opts = new BasicDBObject();
        if (!"".equals(name)) {
          opts.put("name", name);
        }
        opts.put("dropDups", dropDups);
        opts.put("sparse", sparse);
        opts.put("unique", unique);
        collection.ensureIndex(defObj, opts);
        return null;
      }
    });
  }

  protected class PersistenceDescriptor {

    PersistentEntity entity;
    PersistentProperty idProperty = null;
    List<PersistentProperty> properties = null;

    PersistenceDescriptor(PersistentEntity entity, PersistentProperty idProperty, List<PersistentProperty> properties) {
      this.entity = entity;
      this.idProperty = idProperty;
      this.properties = properties;
    }

    public PersistentEntity getEntity() {
      return entity;
    }

    public PersistentProperty getIdProperty() {
      return idProperty;
    }

    public List<PersistentProperty> getProperties() {
      return properties;
    }

  }

  protected class ValuePropertyDescriptor extends PropertyDescriptor {

    Object value;

    ValuePropertyDescriptor(String s, Class<?> aClass, Object value) throws IntrospectionException {
      super(s, aClass);
      this.value = value;
    }

    public Object getValue() {
      return value;
    }

    public void setValue(Object value) {
      this.value = value;
    }

    @Override
    public Method getReadMethod() {
      try {
        return getClass().getMethod("getValue", null);
      } catch (NoSuchMethodException e) {
        // IGNORED
      }
      return super.getReadMethod();
    }

    @Override
    public void setReadMethod(Method method) throws IntrospectionException {
      // IGNORED
    }

    @Override
    public Method getWriteMethod() {
      try {
        return getClass().getMethod("setValue", Object.class);
      } catch (NoSuchMethodException e) {
        // IGNORED
      }
      return super.getWriteMethod();
    }

    @Override
    public void setWriteMethod(Method method) throws IntrospectionException {
      // IGNORED
    }
  }

}
