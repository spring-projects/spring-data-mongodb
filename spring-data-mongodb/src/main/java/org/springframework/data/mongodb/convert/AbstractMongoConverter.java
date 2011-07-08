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

package org.springframework.data.mongodb.convert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.mongodb.convert.MongoConverters.BigDecimalToStringConverter;
import org.springframework.data.mongodb.convert.MongoConverters.BigIntegerToObjectIdConverter;
import org.springframework.data.mongodb.convert.MongoConverters.ObjectIdToBigIntegerConverter;
import org.springframework.data.mongodb.convert.MongoConverters.ObjectIdToStringConverter;
import org.springframework.data.mongodb.convert.MongoConverters.StringToBigDecimalConverter;
import org.springframework.data.mongodb.convert.MongoConverters.StringToObjectIdConverter;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Base class for {@link MongoConverter} implementations. Sets up a {@link GenericConversionService} and populates basic
 * converters. Allows registering {@link CustomConversions}.
 * 
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke ogierke@vmware.com
 */
public abstract class AbstractMongoConverter implements MongoConverter, InitializingBean {

	protected final GenericConversionService conversionService;
	protected CustomConversions conversions = new CustomConversions();

	/**
	 * Creates a new {@link AbstractMongoConverter} using the given {@link GenericConversionService}.
	 * 
	 * @param conversionService
	 */
	public AbstractMongoConverter(GenericConversionService conversionService) {
		this.conversionService = conversionService == null ? ConversionServiceFactory.createDefaultConversionService()
				: conversionService;
		this.conversionService.removeConvertible(Object.class, String.class);
	}

	/**
	 * Registers the given custom conversions with the converter.
	 * 
	 * @param conversions
	 */
	public void setCustomConversions(CustomConversions conversions) {
		this.conversions = conversions;
	}

	/**
	 * Registers converters for {@link ObjectId} handling, removes plain {@link #toString()} converter and promotes the
	 * configured {@link ConversionService} to {@link MappingBeanHelper}.
	 */
	private void initializeConverters() {

		if (!conversionService.canConvert(ObjectId.class, String.class)) {
			conversionService.addConverter(ObjectIdToStringConverter.INSTANCE);
		}
		if (!conversionService.canConvert(String.class, ObjectId.class)) {
			conversionService.addConverter(StringToObjectIdConverter.INSTANCE);
		}
		if (!conversionService.canConvert(ObjectId.class, BigInteger.class)) {
			conversionService.addConverter(ObjectIdToBigIntegerConverter.INSTANCE);
		}
		if (!conversionService.canConvert(BigInteger.class, ObjectId.class)) {
			conversionService.addConverter(BigIntegerToObjectIdConverter.INSTANCE);
		}
		
		conversions.registerConvertersIn(conversionService);
	}


	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.convert.MongoConverter#getConversionService()
	 */
	public ConversionService getConversionService() {
		return conversionService;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {
		initializeConverters();
	}

	@SuppressWarnings("unchecked")
	public Object maybeConvertObject(Object obj) {
		if (obj instanceof Enum<?>) {
			return ((Enum<?>) obj).name();
		}

		if (null != obj && conversions.isSimpleType(obj.getClass())) {
			// Doesn't need conversion
			return obj;
		}

		if (obj instanceof BasicDBList) {
			return maybeConvertList((BasicDBList) obj);
		}

		if (obj instanceof DBObject) {
			DBObject newValueDbo = new BasicDBObject();
			for (String vk : ((DBObject) obj).keySet()) {
				Object o = ((DBObject) obj).get(vk);
				newValueDbo.put(vk, maybeConvertObject(o));
			}
			return newValueDbo;
		}

		if (obj instanceof Map) {
			Map<Object, Object> m = new HashMap<Object, Object>();
			for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) obj).entrySet()) {
				m.put(entry.getKey(), maybeConvertObject(entry.getValue()));
			}
			return m;
		}

		if (obj instanceof List) {
			List<?> l = (List<?>) obj;
			List<Object> newList = new ArrayList<Object>();
			for (Object o : l) {
				newList.add(maybeConvertObject(o));
			}
			return newList;
		}

		if (obj.getClass().isArray()) {
			return maybeConvertArray((Object[]) obj);
		}

		DBObject newDbo = new BasicDBObject();
		this.write(obj, newDbo);
		return newDbo;
	}

	public Object[] maybeConvertArray(Object[] src) {
		Object[] newArr = new Object[src.length];
		for (int i = 0; i < src.length; i++) {
			newArr[i] = maybeConvertObject(src[i]);
		}
		return newArr;
	}

	public BasicDBList maybeConvertList(BasicDBList dbl) {
		BasicDBList newDbl = new BasicDBList();
		Iterator<?> iter = dbl.iterator();
		while (iter.hasNext()) {
			Object o = iter.next();
			newDbl.add(maybeConvertObject(o));
		}
		return newDbl;
	}
}
