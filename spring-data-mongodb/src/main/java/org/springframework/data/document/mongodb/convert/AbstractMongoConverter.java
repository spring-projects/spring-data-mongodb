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

import static org.springframework.data.mapping.MappingBeanHelper.isSimpleType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.document.mongodb.convert.ObjectIdConverters.BigIntegerToObjectIdConverter;
import org.springframework.data.document.mongodb.convert.ObjectIdConverters.ObjectIdToBigIntegerConverter;
import org.springframework.data.document.mongodb.convert.ObjectIdConverters.ObjectIdToStringConverter;
import org.springframework.data.document.mongodb.convert.ObjectIdConverters.StringToObjectIdConverter;
import org.springframework.data.mapping.MappingBeanHelper;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public abstract class AbstractMongoConverter implements MongoConverter, InitializingBean {

	@SuppressWarnings({ "unchecked" })
	private static final List<Class<?>> MONGO_TYPES = Arrays.asList(Number.class, Date.class, String.class,
			DBObject.class);

	protected final GenericConversionService conversionService;
	private final Set<ConvertiblePair> customTypeMapping = new HashSet<ConvertiblePair>();

	public AbstractMongoConverter(GenericConversionService conversionService) {
		this.conversionService = conversionService == null ? ConversionServiceFactory.createDefaultConversionService()
				: conversionService;
		this.conversionService.removeConvertible(Object.class, String.class);
	}

	/**
	 * Add custom {@link Converter} or {@link ConverterFactory} instances to be used that will take presidence over
	 * metadata driven conversion between of objects to/from DBObject
	 * 
	 * @param converters
	 */
	public void setCustomConverters(Set<?> converters) {
		if (null != converters) {
			for (Object c : converters) {
				registerConverter(c);
			}
		}
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
	}

	/**
	 * Inspects the given {@link Converter} for the types it can convert and registers the pair for custom type conversion
	 * in case the target type is a Mongo basic type.
	 * 
	 * @param converter
	 */
	private void registerConverter(Object converter) {
		Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), Converter.class);
		if (MONGO_TYPES.contains(arguments[1]) || MONGO_TYPES.contains(arguments[0])) {
			customTypeMapping.add(new ConvertiblePair(arguments[0], arguments[1]));
		}
		boolean added = false;
		if (converter instanceof Converter) {
			this.conversionService.addConverter((Converter<?, ?>) converter);
			added = true;
		}
		if (converter instanceof ConverterFactory) {
			this.conversionService.addConverterFactory((ConverterFactory<?, ?>) converter);
			added = true;
		}
		if (!added) {
			throw new IllegalArgumentException("Given set contains element that is neither Converter nor ConverterFactory!");
		}
	}

	protected Class<?> getCustomTarget(Class<?> source, Class<?> expectedTargetType) {
		for (ConvertiblePair typePair : customTypeMapping) {
			if (typePair.getSourceType().isAssignableFrom(source)) {

				Class<?> targetType = typePair.getTargetType();

				if (targetType.equals(expectedTargetType) || expectedTargetType == null) {
					return targetType;
				}
			}
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.document.mongodb.convert.MongoConverter#getConversionService()
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

		if (null != obj && isSimpleType(obj.getClass())) {
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
