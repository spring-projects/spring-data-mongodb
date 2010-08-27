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

package org.springframework.datastore.document.mongodb;

import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.NotWritablePropertyException;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.TypeMismatchException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.datastore.document.DocumentMapper;
import org.springframework.util.Assert;

import com.mongodb.DBObject;

/**
 * Class used to map properties of a Document to the corresponding properties of a business object.
 *
 * @author Thomas Risberg
 * @since 1.0
 */
public class MongoBeanPropertyDocumentMapper<T> implements DocumentMapper<DBObject, T> {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	/** The class we are mapping to */
	private Class<T> mappedClass;

	/** Map of the fields we provide mapping for */
	private Map<String, PropertyDescriptor> mappedFields;

	/** Set of bean properties we provide mapping for */
	private Set<String> mappedProperties;

	
	public MongoBeanPropertyDocumentMapper(Class<T> mappedClass) {
		initialize(mappedClass);
	}

	
	public T mapDocument(DBObject document) {
		Assert.state(this.mappedClass != null, "Mapped class was not specified");
		T mappedObject = BeanUtils.instantiate(this.mappedClass);
		BeanWrapper bw = PropertyAccessorFactory.forBeanPropertyAccess(mappedObject);
		initBeanWrapper(bw);
		
		Set<String> keys = document.keySet();
		for (String key : keys) {
			String keyToUse = ("_id".equals(key) ? "id" : key);
			PropertyDescriptor pd = this.mappedFields.get(keyToUse);
			if (pd != null) {
				try {
					Object value = document.get(key);
					try {
						bw.setPropertyValue(pd.getName(), value);
					}
					catch (TypeMismatchException e) {
						logger.warn("Intercepted TypeMismatchException for " + key + "' with value " + value +
									" when setting property '" + pd.getName() + "' of type " + pd.getPropertyType() +
									" on object: " + mappedObject);
					}
				}
				catch (NotWritablePropertyException ex) {
					throw new DataRetrievalFailureException(
							"Unable to map key " + key + " to property " + pd.getName(), ex);
				}
			}
		}
		
		return mappedObject;
	}
	
	/**
	 * Get the class that we are mapping to.
	 */
	public final Class<T> getMappedClass() {
		return this.mappedClass;
	}

	/**
	 * Initialize the mapping metadata for the given class.
	 * @param mappedClass the mapped class.
	 */
	protected void initialize(Class<T> mappedClass) {
		this.mappedClass = mappedClass;
		this.mappedFields = new HashMap<String, PropertyDescriptor>();
		this.mappedProperties = new HashSet<String>();
		PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(mappedClass);
		for (PropertyDescriptor pd : pds) {
			if (pd.getWriteMethod() != null) {
				this.mappedFields.put(pd.getName(), pd);
				this.mappedProperties.add(pd.getName());
			}
		}
	}
	
	/**
	 * Initialize the given BeanWrapper to be used for row mapping.
	 * To be called for each row.
	 * <p>The default implementation is empty. Can be overridden in subclasses.
	 * @param bw the BeanWrapper to initialize
	 */
	protected void initBeanWrapper(BeanWrapper bw) {
	}

	/**
	 * Static factory method to create a new MongoBeanPropertyDocumentMapper
	 * (with the mapped class specified only once).
	 * @param mappedClass the class that document should be mapped to
	 */
	public static <T> MongoBeanPropertyDocumentMapper<T> newInstance(Class<T> mappedClass) {
		MongoBeanPropertyDocumentMapper<T> newInstance = new MongoBeanPropertyDocumentMapper<T>(mappedClass);
		return newInstance;
	}

}
