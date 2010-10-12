package org.springframework.datastore.document.mongodb;

import java.beans.PropertyDescriptor;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
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
		// TODO check on enums.. basics.add(Enum.class.getName());
		SIMPLE_TYPES = Collections.unmodifiableSet(basics);
	}

	protected GenericConversionService conversionService = new GenericConversionService();

	public SimpleMongoConverter() {
		initializeConverters();
	}

	protected void initializeConverters() {

		conversionService.addConverter(new Converter<ObjectId, String>() {
			public String convert(ObjectId id) {
				return id.toString();
			}
		});

	}

	public SimpleMongoConverter(GenericConversionService conversionService) {
		super();
		this.conversionService = conversionService;
	}

	public void write(Object obj, DBObject dbo) {
		BeanWrapper bw = PropertyAccessorFactory.forBeanPropertyAccess(obj);
		initBeanWrapper(bw);

		PropertyDescriptor[] propertyDescriptors = BeanUtils
				.getPropertyDescriptors(obj.getClass());
		for (PropertyDescriptor pd : propertyDescriptors) {
			if (isSimpleType(pd.getPropertyType())) {
				Object value = bw.getPropertyValue(pd.getName());
				String keyToUse = ("id".equals(pd.getName()) ? "_id" : pd
						.getName());

				if (isValidProperty(pd)) {
					
					//TODO validate Enums...
					
					// This will leverage the conversion service.
					dbo.put(keyToUse, value);
				} else {
					logger.warn("Unable to map property " + pd.getName() + ".  Skipping.");
				}				
			}

		}

	}

	public Object read(Class<? extends Object> clazz, DBObject dbo) {

		Assert.state(clazz != null, "Mapped class was not specified");
		Object mappedObject = BeanUtils.instantiate(clazz);
		BeanWrapper bw = PropertyAccessorFactory
				.forBeanPropertyAccess(mappedObject);
		initBeanWrapper(bw);

		// Iterate over properties of the object.
		PropertyDescriptor[] propertyDescriptors = BeanUtils
				.getPropertyDescriptors(clazz);
		for (PropertyDescriptor pd : propertyDescriptors) {
			if (isSimpleType(pd.getPropertyType())) {
				if (dbo.containsField(pd.getName())) {
					Object value = dbo.get(pd.getName());
					if (value instanceof ObjectId) {
						setObjectIdOnObject(bw, pd, (ObjectId) value);
					} else {
						if (isValidProperty(pd)) {
							// This will leverage the conversion service.
							bw.setPropertyValue(pd.getName(),
									dbo.get(pd.getName()));
						} else {
							logger.warn("Unable to map DBObject field "
									+ pd.getName() + " to property "
									+ pd.getName() + ".  Skipping.");
						}
					}
				}
			}
		}

		return mappedObject;
	}

	protected void setObjectIdOnObject(BeanWrapper bw, PropertyDescriptor pd,
			ObjectId value) {
		// TODO strategy for setting the id field. suggest looking for public
		// property 'Id' or private field id or _id;

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

}
