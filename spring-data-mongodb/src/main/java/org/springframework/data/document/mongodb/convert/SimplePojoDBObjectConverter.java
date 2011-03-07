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
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.mapping.annotation.Transient;
import org.springframework.data.mapping.reflect.ReflectionUtils;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class SimplePojoDBObjectConverter {

  protected static final Set<String> SIMPLE_TYPES;

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

  protected static final ConcurrentMap<Class<?>, Map<String, Field>> fieldsByName = new ConcurrentHashMap<Class<?>, Map<String, Field>>();

  protected GenericConversionService conversionService = ConversionServiceFactory.createDefaultConversionService();

  public SimplePojoDBObjectConverter() {
  }

  public GenericConversionService getConversionService() {
    return conversionService;
  }

  public void setConversionService(GenericConversionService conversionService) {
    this.conversionService = conversionService;
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

  public DBObject convert(Object source, DBObject existing) throws Exception {
    DBObject dbo;
    if (source instanceof Collection) {
      Collection c = (Collection) source;
      dbo = (null == existing ? new BasicDBList() : existing);
      for (Object o : c) {
        if (isSimpleType(o.getClass())) {
          ((BasicDBList) dbo).add(o);
        } else {
          ((BasicDBList) dbo).add(convert(o, null));
        }
      }
    } else if (source instanceof Map) {
      Map<Object, Object> m = (Map) source;
      dbo = (null == existing ? new BasicDBObject() : existing);
      for (Map.Entry<Object, Object> entry : m.entrySet()) {
        String key = (entry.getKey() instanceof String ? entry.getKey().toString() : conversionService.convert(entry.getKey(), String.class));
        if (isSimpleType(entry.getValue().getClass())) {
          dbo.put(key, entry.getValue());
        } else {
          dbo.put(key, convert(entry.getValue(), null));
        }
      }
    } else {
      dbo = (null == existing ? new BasicDBObject() : existing);
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
                dbo.put(descriptor.getName(), convert(o, null));
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
