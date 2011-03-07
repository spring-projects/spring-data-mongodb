/* Copyright 2004-2005 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mapping.reflect;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.mapping.annotation.PersistenceConstructor;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.*;
import java.util.*;

/**
 * Reads the properties of a class in an optimized manner avoiding
 * exceptions
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public class ClassPropertyFetcher {

  private static final Log log = LogFactory.getLog(ClassPropertyFetcher.class);

  private final Class clazz;
  final Map<String, PropertyFetcher> staticFetchers = new HashMap<String, PropertyFetcher>();
  final Map<String, PropertyFetcher> instanceFetchers = new HashMap<String, PropertyFetcher>();
  private final ReferenceInstanceCallback callback;
  private PropertyDescriptor[] propertyDescriptors;
  private Constructor preferredConstructor;
  private Map<String, PropertyDescriptor> propertyDescriptorsByName = new HashMap<String, PropertyDescriptor>();
  private Map<String, Field> fieldsByName = new HashMap<String, Field>();
  private Map<Class, List<PropertyDescriptor>> typeToPropertyMap = new HashMap<Class, List<PropertyDescriptor>>();

  private static Map<Class, ClassPropertyFetcher> cachedClassPropertyFetchers = new WeakHashMap<Class, ClassPropertyFetcher>();

  public static ClassPropertyFetcher forClass(final Class c) {
    ClassPropertyFetcher cpf = cachedClassPropertyFetchers.get(c);
    if (cpf == null) {
      cpf = new ClassPropertyFetcher(c);
      cachedClassPropertyFetchers.put(c, cpf);
    }
    return cpf;
  }

  ClassPropertyFetcher(final Class clazz) {
    this.clazz = clazz;
    this.callback = new ReferenceInstanceCallback() {
      public Object getReferenceInstance() {
        try {
          return ReflectionUtils.instantiate(clazz);
        } catch (InstantiationException e) {
          throw new IllegalStateException(e);
        }
      }
    };
    init();
  }

  /**
   * @return The Java that this ClassPropertyFetcher was constructor for
   */
  public Class getJavaClass() {
    return this.clazz;
  }

  public Object getReference() {
    if (callback != null)
      return this.callback.getReferenceInstance();
    return null;
  }

  public PropertyDescriptor[] getPropertyDescriptors() {
    return propertyDescriptors;
  }

  public boolean isReadableProperty(String name) {
    return staticFetchers.containsKey(name) || instanceFetchers.containsKey(name);
  }

  private void init() {

    List<Class> allClasses = resolveAllClasses(clazz);
    for (Class c : allClasses) {
      Field[] fields = c.getDeclaredFields();
      for (Field field : fields) {
        processField(field);
      }
      Method[] methods = c.getDeclaredMethods();
      for (Method method : methods) {
        processMethod(method);
      }
    }

    try {
      this.propertyDescriptors = Introspector.getBeanInfo(clazz).getPropertyDescriptors();
    } catch (IntrospectionException e) {
      // ignore
    }
    if (propertyDescriptors != null) {
      for (PropertyDescriptor desc : propertyDescriptors) {
        propertyDescriptorsByName.put(desc.getName(), desc);
        final Class<?> propertyType = desc.getPropertyType();
        List<PropertyDescriptor> pds = typeToPropertyMap.get(propertyType);
        if (pds == null) {
          pds = new ArrayList<PropertyDescriptor>();
          typeToPropertyMap.put(propertyType, pds);
        }
        pds.add(desc);

        Method readMethod = desc.getReadMethod();
        if (readMethod != null) {
          boolean staticReadMethod = Modifier.isStatic(readMethod.getModifiers());
          if (staticReadMethod) {
            staticFetchers.put(desc.getName(), new GetterPropertyFetcher(readMethod, staticReadMethod));
          } else {
            instanceFetchers.put(desc.getName(), new GetterPropertyFetcher(readMethod, staticReadMethod));
          }
        }
      }
    }

    Constructor[] constructors = clazz.getConstructors();
    if (constructors.length == 1 && constructors[0].getGenericParameterTypes().length > 0) {
      preferredConstructor = constructors[0];
    } else {
      for (Constructor<?> c : constructors) {
        if (c.isAnnotationPresent(PersistenceConstructor.class)) {
          preferredConstructor = c;
        }
      }
    }

  }

  public <T> Constructor<T> getPreferredConstructor() {
    return preferredConstructor;
  }

  private void processMethod(Method method) {
    if (method.isSynthetic())
      return;
    if (!Modifier.isPublic(method.getModifiers()))
      return;
    if (Modifier.isStatic(method.getModifiers())
        && method.getReturnType() != Void.class) {
      if (method.getParameterTypes().length == 0) {
        String name = method.getName();
        if (name.indexOf('$') == -1) {
          if (name.length() > 3 && name.startsWith("get") && Character.isUpperCase(name.charAt(3))) {
            name = name.substring(3);
          } else if (name.length() > 2
              && name.startsWith("is")
              && Character.isUpperCase(name.charAt(2))
              && (method.getReturnType() == Boolean.class || method.getReturnType() == boolean.class)) {
            name = name.substring(2);
          }
          PropertyFetcher fetcher = new GetterPropertyFetcher(method, true);
          staticFetchers.put(name, fetcher);
          staticFetchers.put(Introspector.decapitalize(name), fetcher);
        }
      }
    }
  }

  private void processField(Field field) {
    if (field.isSynthetic())
      return;
    final int modifiers = field.getModifiers();
    final String name = field.getName();
    if (!Modifier.isPublic(modifiers)) {
      if (name.indexOf('$') == -1) {
        fieldsByName.put(name, field);
      }
    } else {
      if (name.indexOf('$') == -1) {
        boolean staticField = Modifier.isStatic(modifiers);
        if (staticField) {
          staticFetchers.put(name, new FieldReaderFetcher(field, staticField));
        } else {
          instanceFetchers.put(name, new FieldReaderFetcher(field, staticField));
        }
      }
    }

  }

  private List<Class> resolveAllClasses(Class c) {
    List<Class> list = new ArrayList<Class>();
    Class currentClass = c;
    while (currentClass != null) {
      list.add(currentClass);
      currentClass = currentClass.getSuperclass();
    }
    Collections.reverse(list);
    return list;
  }

  public Object getPropertyValue(String name) {
    return getPropertyValue(name, false);
  }

  public Object getPropertyValue(String name, boolean onlyInstanceProperties) {
    PropertyFetcher fetcher = resolveFetcher(name, onlyInstanceProperties);
    return getPropertyValueWithFetcher(name, fetcher);
  }

  private Object getPropertyValueWithFetcher(String name, PropertyFetcher fetcher) {
    if (fetcher != null) {
      try {
        return fetcher.get(callback);
      } catch (Exception e) {
        log.warn("Error fetching property's " + name + " value from class " + clazz.getName(), e);
      }
    }
    return null;
  }

  public <T> T getStaticPropertyValue(String name, Class<T> c) {
    PropertyFetcher fetcher = staticFetchers.get(name);
    if (fetcher != null) {
      Object v = getPropertyValueWithFetcher(name, fetcher);
      return returnOnlyIfInstanceOf(v, c);
    }
    return null;
  }

  public <T> T getPropertyValue(String name, Class<T> c) {
    return returnOnlyIfInstanceOf(getPropertyValue(name, false), c);
  }

  private <T> T returnOnlyIfInstanceOf(Object value, Class<T> type) {
    if ((value != null) && (type == Object.class || ReflectionUtils.isAssignableFrom(type, value.getClass()))) {
      return (T) value;
    } else {
      return null;
    }
  }


  private PropertyFetcher resolveFetcher(String name,
                                         boolean onlyInstanceProperties) {
    PropertyFetcher fetcher = null;
    if (!onlyInstanceProperties) {
      fetcher = staticFetchers.get(name);
    }
    if (fetcher == null) {
      fetcher = instanceFetchers.get(name);
    }
    return fetcher;
  }

  public Class getPropertyType(String name) {
    return getPropertyType(name, false);
  }

  public Class getPropertyType(String name, boolean onlyInstanceProperties) {
    PropertyFetcher fetcher = resolveFetcher(name, onlyInstanceProperties);
    if (fetcher != null) {
      return fetcher.getPropertyType(name);
    }
    return null;
  }

  public PropertyDescriptor getPropertyDescriptor(String name) {
    return propertyDescriptorsByName.get(name);
  }

  public List<PropertyDescriptor> getPropertiesOfType(Class javaClass) {
    final List<PropertyDescriptor> propertyDescriptorList = typeToPropertyMap.get(javaClass);
    if (propertyDescriptorList == null) return Collections.emptyList();
    return propertyDescriptorList;
  }

  public List<PropertyDescriptor> getPropertiesAssignableToType(Class assignableType) {
    List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
    for (Class type : typeToPropertyMap.keySet()) {
      if (assignableType.isAssignableFrom(type)) {
        properties.addAll(typeToPropertyMap.get(type));
      }
    }
    return properties;
  }

  public static interface ReferenceInstanceCallback {
    public Object getReferenceInstance();
  }

  static interface PropertyFetcher {
    public Object get(ReferenceInstanceCallback callback)
        throws IllegalArgumentException, IllegalAccessException,
        InvocationTargetException;

    public Class getPropertyType(String name);
  }

  static class GetterPropertyFetcher implements PropertyFetcher {
    private final Method readMethod;
    private final boolean staticMethod;

    GetterPropertyFetcher(Method readMethod, boolean staticMethod) {
      this.readMethod = readMethod;
      this.staticMethod = staticMethod;
      ReflectionUtils.makeAccessible(readMethod);
    }

    public Object get(ReferenceInstanceCallback callback)
        throws IllegalArgumentException, IllegalAccessException,
        InvocationTargetException {
      if (staticMethod) {
        return readMethod.invoke(null, (Object[]) null);
      } else {
        if (callback != null) {
          return readMethod.invoke(callback.getReferenceInstance(),
              (Object[]) null);
        } else {
          return null;
        }
      }
    }

    public Class getPropertyType(String name) {
      return readMethod.getReturnType();
    }
  }

  static class FieldReaderFetcher implements PropertyFetcher {
    private final Field field;
    private final boolean staticField;

    public FieldReaderFetcher(Field field, boolean staticField) {
      this.field = field;
      this.staticField = staticField;
      ReflectionUtils.makeAccessible(field);
    }

    public Object get(ReferenceInstanceCallback callback)
        throws IllegalArgumentException, IllegalAccessException,
        InvocationTargetException {
      if (staticField) {
        return field.get(null);
      } else {
        if (callback != null) {
          return field.get(callback.getReferenceInstance());
        } else {
          return null;
        }
      }
    }

    public Class getPropertyType(String name) {
      return field.getType();
    }
  }

  public Field getDeclaredField(String name) {
    return fieldsByName.get(name);
  }
}