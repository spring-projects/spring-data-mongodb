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
package org.springframework.data.mongodb.core.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.bson.types.ObjectId;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mongodb.core.convert.MongoConverters.BigDecimalToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToBigDecimalConverter;
import org.springframework.util.Assert;

import com.mongodb.DBObject;

/**
 * Value object to capture custom conversion. That is essentially a {@link List} of converters and some additional logic
 * around them. The converters are pretty much builds up two sets of types which Mongo basic types {@see #MONGO_TYPES}
 * can be converted into and from. These types will be considered simple ones (which means they neither need deeper
 * inspection nor nested conversion. Thus the {@link CustomConversions} also act as factory for {@link SimpleTypeHolder}
 * .
 * 
 * @author Oliver Gierke
 */
public class CustomConversions {

	@SuppressWarnings({ "unchecked" })
	private static final List<Class<?>> MONGO_TYPES = Arrays.asList(Number.class, Date.class, ObjectId.class, String.class,
			DBObject.class);

	private final Set<ConvertiblePair> readingPairs;
	private final Set<ConvertiblePair> writingPairs;
	private final Set<Class<?>> customSimpleTypes;
	private final SimpleTypeHolder simpleTypeHolder;

	private final List<Object> converters;

	/**
	 * Creates an empty {@link CustomConversions} object.
	 */
	CustomConversions() {
		this(new ArrayList<Object>());
	}

	/**
	 * Creates a new {@link CustomConversions} instance registering the given converters.
	 * 
	 * @param converters
	 */
	public CustomConversions(List<?> converters) {

		Assert.notNull(converters);

		this.readingPairs = new HashSet<ConvertiblePair>();
		this.writingPairs = new HashSet<ConvertiblePair>();
		this.customSimpleTypes = new HashSet<Class<?>>();
		this.customSimpleTypes.add(ObjectId.class);

		this.converters = new ArrayList<Object>();
		this.converters.add(CustomToStringConverter.INSTANCE);
		this.converters.add(BigDecimalToStringConverter.INSTANCE);
		this.converters.add(StringToBigDecimalConverter.INSTANCE);
		this.converters.addAll(converters);

		for (Object c : this.converters) {
			registerConversion(c);
		}

		this.simpleTypeHolder = new SimpleTypeHolder(customSimpleTypes, true);
	}

	/**
	 * Returns the underlying {@link SimpleTypeHolder}.
	 * 
	 * @return
	 */
	public SimpleTypeHolder getSimpleTypeHolder() {
		return simpleTypeHolder;
	}

	/**
	 * Returns whether the given type is considered to be simple.
	 * 
	 * @param type
	 * @return
	 */
	public boolean isSimpleType(Class<?> type) {
		return simpleTypeHolder.isSimpleType(type);
	}

	/**
	 * Populates the given {@link GenericConversionService} with the convertes registered.
	 * 
	 * @param conversionService
	 */
	public void registerConvertersIn(GenericConversionService conversionService) {

		for (Object converter : converters) {

			boolean added = false;

			if (converter instanceof Converter) {
				conversionService.addConverter((Converter<?, ?>) converter);
				added = true;
			}

			if (converter instanceof ConverterFactory) {
				conversionService.addConverterFactory((ConverterFactory<?, ?>) converter);
				added = true;
			}

			if (converter instanceof GenericConverter) {
				conversionService.addConverter((GenericConverter) converter);
				added = true;
			}

			if (!added) {
				throw new IllegalArgumentException("Given set contains element that is neither Converter nor ConverterFactory!");
			}
		}
	}

	/**
	 * Registers a conversion for the given converter. Inspects either generics or the {@link ConvertiblePair}s returned
	 * by a {@link GenericConverter}.
	 * 
	 * @param converter
	 */
	private void registerConversion(Object converter) {

		if (converter instanceof GenericConverter) {
			GenericConverter genericConverter = (GenericConverter) converter;
			for (ConvertiblePair pair : genericConverter.getConvertibleTypes()) {
				register(pair);
			}
		} else if (converter instanceof Converter) {
			Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), Converter.class);
			register(new ConvertiblePair(arguments[0], arguments[1]));
		} else {
			throw new IllegalArgumentException("Unsupported Converter type!");
		}
	}

	/**
	 * Registers the given {@link ConvertiblePair} as reading or writing pair depending on the type sides being basic
	 * Mongo types.
	 * 
	 * @param pair
	 */
	private void register(ConvertiblePair pair) {

		if (isMongoBasicType(pair.getSourceType())) {
			readingPairs.add(pair);
			customSimpleTypes.add(pair.getTargetType());
		}

		if (isMongoBasicType(pair.getTargetType())) {
			writingPairs.add(pair);
			customSimpleTypes.add(pair.getSourceType());
		}
	}

	/**
	 * Returns the target type to convert to in case we have a custom conversion registered to convert the given source
	 * type into a Mongo native one.
	 * 
	 * @param source must not be {@literal null}
	 * @return
	 */
	public Class<?> getCustomWriteTarget(Class<?> source) {
		return getCustomWriteTarget(source, null);
	}

	/**
	 * Returns the target type we can write an onject of the given source type to. The returned type might be a subclass
	 * oth the given expected type though. If {@code expexctedTargetType} is {@literal null} we will simply return the
	 * first target type matching or {@literal null} if no conversion can be found.
	 * 
	 * @param source must not be {@literal null}
	 * @param expectedTargetType
	 * @return
	 */
	public Class<?> getCustomWriteTarget(Class<?> source, Class<?> expectedTargetType) {
		Assert.notNull(source);
		return getCustomTarget(source, expectedTargetType, writingPairs);
	}

	/**
	 * Returns whether we have a custom conversion registered to write into a Mongo native type. The returned type might
	 * be a subclass oth the given expected type though.
	 * 
	 * @param source must not be {@literal null}
	 * @return
	 */
	public boolean hasCustomWriteTarget(Class<?> source) {
		return hasCustomWriteTarget(source, null);
	}

	/**
	 * Returns whether we have a custom conversion registered to write an object of the given source type into an object
	 * of the given Mongo native target type.
	 * 
	 * @param source must not be {@literal null}.
	 * @param expectedTargetType
	 * @return
	 */
	public boolean hasCustomWriteTarget(Class<?> source, Class<?> expectedTargetType) {
		return getCustomWriteTarget(source, expectedTargetType) != null;
	}

	/**
	 * Returns whether we have a custom conversion registered to read the given source into the given target type.
	 * 
	 * @param source must not be {@literal null}
	 * @param expectedTargetType must not be {@literal null}
	 * @return
	 */
	public boolean hasCustomReadTarget(Class<?> source, Class<?> expectedTargetType) {
		Assert.notNull(source);
		Assert.notNull(expectedTargetType);
		return getCustomTarget(source, expectedTargetType, readingPairs) != null;
	}

	/**
	 * Inspects the given {@link ConvertiblePair} for ones that have a source compatible type as source. Additionally
	 * checks assignabilty of the target type if one is given.
	 * 
	 * @param source must not be {@literal null}
	 * @param expectedTargetType
	 * @param pairs must not be {@literal null}
	 * @return
	 */
	private static Class<?> getCustomTarget(Class<?> source, Class<?> expectedTargetType, Iterable<ConvertiblePair> pairs) {

		Assert.notNull(source);
		Assert.notNull(pairs);

		for (ConvertiblePair typePair : pairs) {
			if (typePair.getSourceType().isAssignableFrom(source)) {
				Class<?> targetType = typePair.getTargetType();
				if (expectedTargetType == null || targetType.isAssignableFrom(expectedTargetType)) {
					return targetType;
				}
			}
		}

		return null;
	}

	/**
	 * Returns whether the given type is a type that Mongo can handle basically.
	 * 
	 * @param type
	 * @return
	 */
	private static boolean isMongoBasicType(Class<?> type) {
		return MONGO_TYPES.contains(type);
	}

	private enum CustomToStringConverter implements GenericConverter {
		INSTANCE;

		public Set<ConvertiblePair> getConvertibleTypes() {
			ConvertiblePair localeToString = new ConvertiblePair(Locale.class, String.class);
			ConvertiblePair booleanToString = new ConvertiblePair(Character.class, String.class);
			return new HashSet<ConvertiblePair>(Arrays.asList(localeToString, booleanToString));
		}

		public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			return source.toString();
		}
	}
}
