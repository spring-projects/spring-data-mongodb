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
import org.springframework.data.mapping.model.MappingInstantiationException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

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
  private PropertyDescriptor idPropertyDescriptor = null;
  private List<String> ignoredProperties = new ArrayList<String>() {{
    add("class");
  }};
  private List<Class<?>> validIdTypes = new ArrayList<Class<?>>() {{
    add(ObjectId.class);
    add(String.class);
    add(BigInteger.class);
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
      throw new MappingException(e.getMessage(), e, clazz);
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
              if (null == idField) {
                idField = fld;
                idPropertyDescriptor = descriptor;
              } else {
                log.warn("Only the first field found with the @Id annotation will be considered the ID. Ignoring " + idField);
              }
              continue;
            } else if (null == idField
                && validIdTypes.contains(fldType)
                && ("id".equals(name) || "_id".equals(name))) {
              idField = fld;
              idPropertyDescriptor = descriptor;
              continue;
            } else if (fld.isAnnotationPresent(Reference.class)
                || fld.isAnnotationPresent(OneToMany.class)
                || fld.isAnnotationPresent(OneToOne.class)
                || fld.isAnnotationPresent(ManyToMany.class)) {
              Class<?> targetClass = fld.getType();
              if (fld.isAnnotationPresent(Reference.class)) {
                Reference ref = fld.getAnnotation(Reference.class);
                if (ref.targetClass() != Object.class) {
                  targetClass = ref.targetClass();
                }
              }
              if (fldType.isAssignableFrom(Collection.class) || fldType.isAssignableFrom(List.class)) {
                Type t = fld.getGenericType();
                if (t instanceof ParameterizedType) {
                  ParameterizedType ptype = (ParameterizedType) t;
                  Type[] paramTypes = ptype.getActualTypeArguments();
                  if (paramTypes.length > 0) {
                    if (paramTypes[0] instanceof TypeVariable) {
                      targetClass = Object.class;
                    } else {
                      targetClass = (Class<?>) paramTypes[0];
                    }
                  }
                }
              }
              associations.add(new Association(descriptor, fld, targetClass));
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

    if (null == this.idField) {
      // ID might be in a private field
      for (Field f : fields.values()) {
        Class<?> type = f.getType();
        if (f.isAnnotationPresent(Id.class)) {
          this.idField = f;
          break;
        } else if (validIdTypes.contains(type) && (f.getName().equals("id") || f.getName().equals("_id"))) {
          this.idField = f;
          break;
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
        if (null == preferredConstructor || constructor.isAnnotationPresent(PersistenceConstructor.class)) {
          String[] paramNames = new LocalVariableTableParameterNameDiscoverer().getParameterNames(constructor);
          Type[] paramTypes = constructor.getGenericParameterTypes();
          Class<?>[] paramClassTypes = new Class[paramTypes.length];
          for (int i = 0; i < paramTypes.length; i++) {
            Class<?> targetType;
            if (paramTypes[i] instanceof ParameterizedType) {
              ParameterizedType ptype = (ParameterizedType) paramTypes[i];
              Type[] types = ptype.getActualTypeArguments();
              if (types.length == 1) {
                if (types[0] instanceof TypeVariable) {
                  // Placeholder type
                  targetType = Object.class;
                } else {
                  targetType = (Class<?>) types[0];
                }
              } else {
                targetType = (Class<?>) ptype.getRawType();
              }
            } else {
              targetType = (Class<?>) paramTypes[i];
            }
            paramClassTypes[i] = targetType;
          }
          preferredConstructor = new PreferredConstructor((Constructor<T>) constructor, paramNames, paramClassTypes);
          if (constructor.isAnnotationPresent(PersistenceConstructor.class)) {
            // We're done
            break;
          }
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

  public PropertyDescriptor getIdPropertyDescriptor() {
    return idPropertyDescriptor;
  }

  public T createInstance() {
    return createInstance(null);
  }

  public T createInstance(ParameterValueProvider provider) {
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
      throw new MappingInstantiationException(e.getMessage(), e);
    } catch (InstantiationException e) {
      throw new MappingInstantiationException(e.getMessage(), e);
    } catch (IllegalAccessException e) {
      throw new MappingInstantiationException(e.getMessage(), e);
    }
  }

  public Object getFieldValue(String name, Object from) throws MappingException {
    return getFieldValue(name, from, null);
  }

  public Object getFieldValue(String name, Object from, Object defaultObj) throws MappingException {
    try {
      if (properties.containsKey(name) && null != getters.get(name)) {
        return getters.get(name).invoke(from);
      } else {
        if (fields.containsKey(name)) {
          Field f = fields.get(name);
          return f.get(from);
        } else {
          for (Association assoc : associations) {
            if (assoc.getField().getName().equals(name)) {
              return assoc.getField().get(from);
            }
          }
          for (Field f : clazz.getDeclaredFields()) {
            // Lastly, check for any private fields
            if (f.getName().equals(name)) {
              f.setAccessible(true);
              return f.get(from);
            }
          }
        }
      }
    } catch (IllegalAccessException e) {
      throw new MappingException(e.getMessage(), e, from);
    } catch (InvocationTargetException e) {
      throw new MappingException(e.getMessage(), e, from);
    }
    return defaultObj;
  }

  public void setValue(String name, Object on, Object value) throws MappingException {
    Field f = fields.get(name);
    try {
      if (null != f) {
        Method setter = setters.get(name);
        if (null != setter) {
          setter.invoke(on, value);
        } else {
          f.set(on, value);
        }
      } else {
        for (Association assoc : associations) {
          if (assoc.getField().getName().equals(name)) {
            assoc.getField().set(on, value);
            return;
          }
        }
        for (Field privFld : clazz.getDeclaredFields()) {
          if (privFld.getName().equals(name)) {
            privFld.setAccessible(true);
            privFld.set(on, value);
            return;
          }
        }
      }
    } catch (IllegalAccessException e) {
      throw new MappingException(e.getMessage(), e, value);
    } catch (InvocationTargetException e) {
      throw new MappingException(e.getMessage(), e, value);
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
          throw new MappingException(e.getMessage(), e, obj);
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
    private final Class<?> targetClass;

    public Association(PropertyDescriptor descriptor, Field field, Class<?> targetClass) {
      this.descriptor = descriptor;
      this.field = field;
      this.targetClass = targetClass;
    }

    public PropertyDescriptor getDescriptor() {
      return descriptor;
    }

    public Field getField() {
      return field;
    }

    public Class<?> getTargetClass() {
      return targetClass;
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
