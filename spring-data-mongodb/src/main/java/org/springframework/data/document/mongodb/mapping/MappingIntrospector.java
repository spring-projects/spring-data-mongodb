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

package org.springframework.data.document.mongodb.mapping;

import com.mongodb.DBRef;
import com.sun.org.apache.xalan.internal.extensions.ExpressionContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.LocalVariableTableParameterNameDiscoverer;
import org.springframework.data.mapping.annotation.*;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import java.beans.BeanInfo;
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
public class MappingIntrospector<T> {

  private static final Log log = LogFactory.getLog(MappingIntrospector.class);
  private static final ConcurrentMap<Class<?>, MappingIntrospector<?>> introspectors = new ConcurrentHashMap<Class<?>, MappingIntrospector<?>>();
  private static final Set<String> SIMPLE_TYPES;

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

  private final Class<T> clazz;
  private final BeanInfo beanInfo;
  private final Map<String, PropertyDescriptor> properties;
  private final Map<String, Field> fields;
  private final Set<Association> associations;
  private final Map<String, Method> setters;
  private final Map<String, Method> getters;
  private Field idField = null;
  private List<String> ignoredProperties = new ArrayList<String>() {{
    add("class");
  }};
  private SpelExpressionParser parser = new SpelExpressionParser();
  private Map<String, Expression> expressions = new HashMap<String, Expression>();
  private PreferredConstructor preferredConstructor = null;

  @SuppressWarnings({"unchecked"})
  private MappingIntrospector(Class<T> clazz) throws MappingException {
    this.clazz = clazz;
    try {
      this.beanInfo = Introspector.getBeanInfo(clazz);
    } catch (IntrospectionException e) {
      throw new MappingException(e, clazz);
    }
    Map<String, PropertyDescriptor> properties = new HashMap<String, PropertyDescriptor>();
    Map<String, Field> fields = new HashMap<String, Field>();
    Set<Association> associations = new HashSet<Association>();
    Map<String, Method> setters = new HashMap<String, Method>();
    Map<String, Method> getters = new HashMap<String, Method>();
    for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {
      String name = descriptor.getName();
      if (!ignoredProperties.contains(name)) {
        try {
          Field fld = clazz.getDeclaredField(name);
          Class<?> fldType = fld.getType();
          fld.setAccessible(true);
          if (!isTransientField(fld)) {
            if (fld.isAnnotationPresent(Id.class)) {
              if (null != idField) {
                throw new IllegalStateException("You cannot have two fields in a domain object annotated with Id! " + clazz);
              }
              idField = fld;
              continue;
            } else if (null == idField && fldType.equals(ObjectId.class)) {
              // Respect fields of the MongoDB ObjectId type
              idField = fld;
              continue;
            } else if (null == idField
                && (fldType.equals(String.class) || fldType.equals(BigInteger.class))
                && ("id".equals(name) || "_id".equals(name))) {
              // Strings and BigIntegers named "id"|"_id" are also valid ID fields
              idField = fld;
              continue;
            } else if (fld.isAnnotationPresent(OneToMany.class)
                || fld.isAnnotationPresent(OneToOne.class)
                || fld.isAnnotationPresent(ManyToMany.class)) {
              associations.add(new Association(descriptor, fld));
              continue;
            } else if (fld.isAnnotationPresent(Value.class)) {
              // @Value fields are evaluated at runtime and are the same transient fields
              continue;
            }
            fields.put(name, fld);
            properties.put(name, descriptor);
            setters.put(name, descriptor.getWriteMethod());
            getters.put(name, descriptor.getReadMethod());
            if (fld.isAnnotationPresent(Value.class)) {
              expressions.put(name, parser.parseExpression(fld.getAnnotation(Value.class).value()));
            }
          }
        } catch (NoSuchFieldException e) {
          log.warn(e.getMessage());
        }
      }
    }
    this.properties = Collections.unmodifiableMap(properties);
    this.fields = Collections.unmodifiableMap(fields);
    this.associations = Collections.unmodifiableSet(associations);
    this.setters = Collections.unmodifiableMap(setters);
    this.getters = Collections.unmodifiableMap(getters);

    // Find the right constructor
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterTypes().length != 0) {
        // Non-no-arg constructor
        if (null == preferredConstructor) {
          String[] paramNames = new LocalVariableTableParameterNameDiscoverer().getParameterNames(constructor);
          Type[] paramTypes = constructor.getGenericParameterTypes();
          Class<?>[] paramClassTypes = new Class[paramTypes.length];
          for (int i = 0; i < paramTypes.length; i++) {
            Class<?> targetType;
            if (paramTypes[i] instanceof ParameterizedType) {
              ParameterizedType ptype = (ParameterizedType) paramTypes[i];
              Type[] types = ptype.getActualTypeArguments();
              if (types.length == 1) {
                targetType = (Class<?>) types[0];
              } else {
                targetType = (Class<?>) ptype.getRawType();
              }
            } else {
              targetType = (Class<?>) paramTypes[i];
            }
            paramClassTypes[i] = targetType;
          }
          preferredConstructor = new PreferredConstructor((Constructor<T>) constructor, paramNames, paramClassTypes);
        }
      }
    }
  }


  @SuppressWarnings({"unchecked"})
  public static <C extends Object> MappingIntrospector<C> getInstance(Class<C> clazz) throws MappingException {
    MappingIntrospector<C> introspector = (MappingIntrospector<C>) introspectors.get(clazz);
    if (null == introspector) {
      introspector = new MappingIntrospector<C>(clazz);
      introspectors.put(clazz, introspector);
    }
    return introspector;
  }

  public Class<T> getTargetClass() {
    return clazz;
  }

  public Field getField(String name) {
    return fields.get(name);
  }

  public PropertyDescriptor getPropertyDescriptor(String name) {
    return properties.get(name);
  }

  public boolean isMappable() {
    return (null != clazz.getAnnotation(Persistent.class));
  }

  public Field getIdField() {
    return idField;
  }

  public T createInstance() throws MappingException {
    return createInstance(null);
  }

  public T createInstance(ParameterValueProvider provider) throws MappingException {
    try {
      if (null == preferredConstructor || null == provider) {
        return clazz.newInstance();
      } else {
        List<Object> params = new LinkedList<Object>();
        for (ConstructorParameter<?> p : preferredConstructor.parameters) {
          Expression x = null;
          Value v = p.getValueAnnotation();
          if (null != v) {
            x = parser.parseExpression(v.value());
          }
          params.add(provider.getParameterValue(p.name, p.type, x));
        }
        return preferredConstructor.constructor.newInstance(params.toArray());
      }
    } catch (InvocationTargetException e) {
      throw new MappingException(e, clazz);
    } catch (InstantiationException e) {
      throw new MappingException(e, clazz);
    } catch (IllegalAccessException e) {
      throw new MappingException(e, clazz);
    }
  }

  public Object getFieldValue(PropertyDescriptor descriptor, Object from) throws MappingException {
    try {
      Method getter = descriptor.getReadMethod();
      if (null != getter) {
        return getter.invoke(from);
      } else {
        Field f = fields.get(descriptor.getName());
        return f.get(from);
      }
    } catch (IllegalAccessException e) {
      throw new MappingException(e, from);
    } catch (InvocationTargetException e) {
      throw new MappingException(e, from);
    }
  }

  public Object getFieldValue(String name, Object from) throws MappingException {
    PropertyDescriptor descriptor = properties.get(name);
    if (null != descriptor) {
      return getFieldValue(descriptor, from);
    }
    return null;
  }

  public void setValue(String name, Object on, Object value) throws MappingException {
    Field f = fields.get(name);
    if (null != f) {
      Method setter = setters.get(name);
      try {
        if (null != setter) {
          setter.invoke(on, value);
        } else {
          f.set(on, value);
        }
      } catch (IllegalAccessException e) {
        throw new MappingException(e, value);
      } catch (InvocationTargetException e) {
        throw new MappingException(e, value);
      }
    }
  }

  public void maybeAutowire(Object obj, ApplicationContext applicationContext, boolean autowire) throws MappingException {
    StandardEvaluationContext evaluationContext = new StandardEvaluationContext(obj);
    evaluationContext.setBeanResolver(new BeanFactoryResolver(applicationContext));

    if (autowire) {
      applicationContext.getAutowireCapableBeanFactory().autowireBean(obj);
      if (obj instanceof InitializingBean) {
        try {
          ((InitializingBean) obj).afterPropertiesSet();
        } catch (Exception e) {
          throw new MappingException(e, obj);
        }
      }
    }

    for (Map.Entry<String, Expression> entry : expressions.entrySet()) {
      setValue(entry.getKey(), obj, entry.getValue().getValue(evaluationContext));
    }
  }

  public void doWithProperties(PropertyHandler handler) {
    for (Map.Entry<String, PropertyDescriptor> entry : properties.entrySet()) {
      String name = entry.getKey();
      handler.doWithProperty(entry.getValue(), fields.get(name), expressions.get(name));
    }
  }

  public void doWithAssociations(AssociationHandler handler) {
    for (Association association : associations) {
      handler.doWithAssociation(association);
    }
  }

  public static boolean isSimpleType(Class<?> propertyType) {
    if (propertyType == null) {
      return false;
    }
    if (propertyType.isArray()) {
      return isSimpleType(propertyType.getComponentType());
    }
    return SIMPLE_TYPES.contains(propertyType.getName());
  }

  public static boolean isTransientField(Field f) {
    return (Modifier.isTransient(f.getModifiers()) || null != f.getAnnotation(Transient.class) || null != f.getAnnotation(Autowired.class));
  }

  public static class Association {

    private final PropertyDescriptor descriptor;
    private final Field field;

    public Association(PropertyDescriptor descriptor, Field field) {
      this.descriptor = descriptor;
      this.field = field;
    }

    public PropertyDescriptor getDescriptor() {
      return descriptor;
    }

    public Field getField() {
      return field;
    }

  }

  private class PreferredConstructor {

    Constructor<T> constructor;
    ConstructorParameter<?>[] parameters;

    @SuppressWarnings({"unchecked"})
    public PreferredConstructor(Constructor<T> constructor, String[] names, Class<?>[] types) {
      this.constructor = constructor;
      Annotation[][] annos = constructor.getParameterAnnotations();
      parameters = new ConstructorParameter[names.length];
      for (int i = 0; i < names.length; i++) {
        parameters[i] = new ConstructorParameter(names[i], types[i], annos[i]);
      }
    }
  }

  private class ConstructorParameter<V> {
    String name;
    Class<V> type;
    Annotation[] annotations;

    private ConstructorParameter(String name, Class<V> type, Annotation[] annotations) {
      this.name = name;
      this.type = type;
      this.annotations = annotations;
    }

    private Value getValueAnnotation() {
      for (Annotation anno : annotations) {
        if (anno instanceof Value) {
          return (Value) anno;
        }
      }
      return null;
    }
  }

  public interface AssociationHandler {
    void doWithAssociation(Association association);
  }

  public interface PropertyHandler {
    void doWithProperty(PropertyDescriptor descriptor, Field field, Expression spelExpr);
  }

  public interface ParameterValueProvider {
    <T> T getParameterValue(String name, Class<T> type, Expression spelExpr);
  }

}
