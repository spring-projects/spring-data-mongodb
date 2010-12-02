/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.data.document.mongodb;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

public class SimpleMongoConverter implements MongoConverter {


	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	public static final Set<String> SIMPLE_TYPES;

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
		// basics.add(Time.class.getName());
		// basics.add(Timestamp.class.getName());
		// basics.add(java.sql.Date.class.getName());
		// basics.add(BigDecimal.class.getName());
		// basics.add(BigInteger.class.getName());
		basics.add(Locale.class.getName());
		// basics.add(Calendar.class.getName());
		// basics.add(GregorianCalendar.class.getName());
		// basics.add(java.util.Currency.class.getName());
		// basics.add(TimeZone.class.getName());
		// basics.add(Object.class.getName());
		basics.add(Class.class.getName());
		// basics.add(byte[].class.getName());
		// basics.add(Byte[].class.getName());
		// basics.add(char[].class.getName());
		// basics.add(Character[].class.getName());
		// basics.add(Blob.class.getName());
		// basics.add(Clob.class.getName());
		// basics.add(Serializable.class.getName());
		// basics.add(URI.class.getName());
		// basics.add(URL.class.getName());
		basics.add(DBRef.class.getName());
		basics.add(Pattern.class.getName());
		basics.add(CodeWScope.class.getName());
		basics.add(ObjectId.class.getName());
		// TODO check on enums.. 
		basics.add(Enum.class.getName());
		SIMPLE_TYPES = Collections.unmodifiableSet(basics);
	}

	protected GenericConversionService conversionService = new GenericConversionService();

	public SimpleMongoConverter() {
		initializeConverters();
	}

	public SimpleMongoConverter(GenericConversionService conversionService) {
		super();
		this.conversionService = conversionService;
	}

	protected void initializeConverters() {
		conversionService.addConverter(new Converter<ObjectId, String>() {
			public String convert(ObjectId id) {
				return id.toString();
			}
		});
	}

	/*
	public ConversionContext getConversionContext() {
		return conversionContext;
	}

	public void setConversionContext(ConversionContext conversionContext) {
		this.conversionContext = conversionContext;
	}

	public void writeNew(Object obj, DBObject dbo) {
		conversionContext.convertToDBObject(dbo, null, obj);
	}*/

	@SuppressWarnings("rawtypes")
	public void write(Object obj, DBObject dbo) {

		BeanWrapper bw = PropertyAccessorFactory.forBeanPropertyAccess(obj);
		// This will leverage the conversion service.
		initBeanWrapper(bw);

		PropertyDescriptor[] propertyDescriptors = BeanUtils
				.getPropertyDescriptors(obj.getClass());
		for (PropertyDescriptor pd : propertyDescriptors) {
			// if (isSimpleType(pd.getPropertyType())) {
			Object value = bw.getPropertyValue(pd.getName());
			String keyToUse = ("id".equals(pd.getName()) ? "_id" : pd.getName());

			if (isValidProperty(pd)) {
				// TODO validate Enums...
				if (value != null && Enum.class.isAssignableFrom(pd.getPropertyType())) {
					writeValue(dbo, keyToUse, ((Enum)value).name());
				}
				else {
					writeValue(dbo, keyToUse, value);
				}
				// dbo.put(keyToUse, value);
			} else {
				//TODO exclude Class properties from consideration
				logger.warn("Unable to map property " + pd.getName()
						+ ".  Skipping.");
			}
			// }

		}

	}

	private void writeValue(DBObject dbo, String keyToUse, Object value) {

		// is not asimple type.
		if (value != null) {
			if (!isSimpleType(value.getClass())) {
				writeCompoundValue(dbo, keyToUse, value);
			} else {
				dbo.put(keyToUse, value);
			}
		}

	}

	private void writeCompoundValue(DBObject dbo, String keyToUse, Object value) {
		if (value instanceof Map) {
			writeMap(dbo, keyToUse, (Map<String, Object>)value);
			return;
		}
		if (value instanceof Collection) {
			// Should write a collection!
			return;
		}
		DBObject nestedDbo = new BasicDBObject();
		write(value, nestedDbo);
		dbo.put(keyToUse, nestedDbo);
		
	}

	protected void writeMap(DBObject dbo, String keyToUse, Map<String, Object> map) {
		//TODO support non-string based keys as long as there is a Spring Converter obj->string and (optionally) string->obj
		DBObject dboToPopulate = null;
		if (keyToUse != null) {
			dboToPopulate = new BasicDBObject();
		} else {
			dboToPopulate = dbo;
		}
		if (map != null) {
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				Object entryValue = entry.getValue();
				if (!isSimpleType(entryValue.getClass())) {			
					writeCompoundValue(dboToPopulate, entry.getKey(), entryValue);
				} else {
					dboToPopulate.put(entry.getKey(), entryValue);
				}
			}					
			dbo.put(keyToUse, dboToPopulate);			
		}
	}

	/*
	public Object readNew(Class<? extends Object> clazz, DBObject dbo) {
		return conversionContext.convertToObject(clazz, dbo);
	}*/

	public Object read(Class<? extends Object> clazz, DBObject dbo) {
		Assert.state(clazz != null, "Mapped class was not specified");
		Object mappedObject = BeanUtils.instantiate(clazz);
		BeanWrapper bw = PropertyAccessorFactory
				.forBeanPropertyAccess(mappedObject);
		initBeanWrapper(bw);

		// Iterate over properties of the object.b
		// TODO iterate over the properties of DBObject and support nested property names with SpEL
		// e.g. { "parameters.p1" : "1" , "count" : 5.0}
		PropertyDescriptor[] propertyDescriptors = BeanUtils
				.getPropertyDescriptors(clazz);
		for (PropertyDescriptor pd : propertyDescriptors) {
			
			String keyToUse = ("id".equals(pd.getName()) ? "_id" : pd.getName());
			System.out.println("??? " + pd.getName() + " " + keyToUse + " = " + dbo.get(keyToUse));
			if (dbo.containsField(keyToUse)) {
				Object value = dbo.get(keyToUse);
				if (value instanceof ObjectId) {
					setObjectIdOnObject(bw, pd, (ObjectId) value);
				} else {
					if (isValidProperty(pd)) {
						// This will leverage the conversion service.
						if (!isSimpleType(value.getClass())) {
							if (value instanceof DBObject) {
								bw.setPropertyValue(pd.getName(), readCompoundValue(pd, (DBObject) value));
							}
							else {
								logger.warn("Unable to map compound DBObject field "
										+ keyToUse + " to property " + pd.getName()
										+ ".  The field value should have been a 'DBObject.class' but was " 
										+ value.getClass().getName());
							}
						}
						else {
							bw.setPropertyValue(pd.getName(), value);
						}
					} else {
						logger.warn("Unable to map DBObject field "
								+ keyToUse + " to property " + pd.getName()
								+ ".  Skipping.");
					}
				}
			}
		}

		return mappedObject;
	}
	
	private Object readCompoundValue(PropertyDescriptor pd, DBObject dbo) {
		Class propertyClazz = pd.getPropertyType();
		if (Map.class.isAssignableFrom(propertyClazz)) {
			//TODO assure is assignable to BasicDBObject
			return readMap(pd, (BasicDBObject)dbo, getGenericParameterClass(pd.getWriteMethod()).get(1) );			
		}
		if (Collection.class.isAssignableFrom(propertyClazz)) {
			// Should read a collection!
			return null;
		}
		return read(propertyClazz, dbo);
	}
	
	protected Map createMap() {
		return new HashMap();
	}
	
	protected Map readMap(PropertyDescriptor pd, BasicDBObject dbo, Class valueClazz) {
		Class propertyClazz = pd.getPropertyType();
		Map map = createMap();
		for (Map.Entry entry : dbo.entrySet()) {
			Object entryValue = entry.getValue();
			BeanWrapper bw = PropertyAccessorFactory.forBeanPropertyAccess(entryValue);
			initBeanWrapper(bw);
			
			if (!isSimpleType(entryValue.getClass())) {				
				map.put((String)entry.getKey(), read(valueClazz, (DBObject) entryValue));
				//Can do some reflection tricks here - 
				//throw new RuntimeException("User types not supported yet as values for Maps");
			} else {
				map.put((String)entry.getKey(), entryValue );
			}
		}
		return map;
	}



	protected void setObjectIdOnObject(BeanWrapper bw, PropertyDescriptor pd, ObjectId value) {
		// TODO strategy for setting the id field. suggest looking for public
		// property 'Id' or private field id or _id;
		if (String.class.isAssignableFrom(pd.getPropertyType())) {
			bw.setPropertyValue(pd.getName(), value);
		}
		else {
			logger.warn("Unable to map _id field "
					+ " to property " + pd.getName()
					+ ".  Should have been a 'String' property but was " 
					+ pd.getPropertyType().getName());
		}
	}

	protected boolean isValidProperty(PropertyDescriptor descriptor) {
		return (descriptor.getReadMethod() != null && descriptor
				.getWriteMethod() != null);
	}

	protected boolean isSimpleType(Class propertyType) {
		if (propertyType == null)
			return false;
		if (propertyType.isArray()) {
			return isSimpleType(propertyType.getComponentType());
		}
		return SIMPLE_TYPES.contains(propertyType.getName());
	}

	protected void initBeanWrapper(BeanWrapper bw) {
		bw.setConversionService(conversionService);
	}
	
	
	public List<Class> getGenericParameterClass(Method setMethod) {
		List<Class> actualGenericParameterTypes  = new ArrayList<Class>();
		Type[] genericParameterTypes = setMethod.getGenericParameterTypes();

		for(Type genericParameterType  : genericParameterTypes){		
		    if(genericParameterType  instanceof ParameterizedType){
		        ParameterizedType aType = (ParameterizedType) genericParameterType;
		        Type[] parameterArgTypes = aType.getActualTypeArguments();		        
		        for(Type parameterArgType : parameterArgTypes){
		        	if (parameterArgType instanceof GenericArrayType)
		            {
		                Class arrayType = (Class) ((GenericArrayType) parameterArgType).getGenericComponentType();
		                actualGenericParameterTypes.add(Array.newInstance(arrayType, 0).getClass());
		            }
		        	else {
		        		if (parameterArgType instanceof ParameterizedType) {
			        		ParameterizedType paramTypeArgs = (ParameterizedType) parameterArgType;
			        		actualGenericParameterTypes.add((Class)paramTypeArgs.getRawType());
			        	} else {
			        		 if (parameterArgType instanceof TypeVariable) {
			        			 throw new RuntimeException("Can not map " + ((TypeVariable) parameterArgType).getName());
			        		 } else {
			        			 if (parameterArgType instanceof Class) {
			        				 actualGenericParameterTypes.add((Class) parameterArgType);
			        			 } else  {
			        				 throw new RuntimeException("Can not map " + parameterArgType); 
			        			 }
			        		 }
			        	}
		        	}
		        	
		        }
		    }
		}
		return actualGenericParameterTypes;

	}

}
