/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.JodaTimeConverters;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mongodb.core.convert.MongoConverters.BigDecimalToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.BigIntegerToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.DBObjectToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToBigDecimalConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToBigIntegerConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToURLConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.TermToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.URLToStringConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.util.Assert;

/**
 * Value object to capture custom conversion. That is essentially a {@link List} of converters and some additional logic
 * around them. The converters are pretty much builds up two sets of types which Mongo basic types {@see #MONGO_TYPES}
 * can be converted into and from. These types will be considered simple ones (which means they neither need deeper
 * inspection nor nested conversion. Thus the {@link CustomConversions} also act as factory for {@link SimpleTypeHolder}
 * .
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class CustomConversions {

	private static final Logger LOG = LoggerFactory.getLogger(CustomConversions.class);
	private static final String READ_CONVERTER_NOT_SIMPLE = "Registering converter from %s to %s as reading converter although it doesn't convert from a Mongo supported type! You might wanna check you annotation setup at the converter implementation.";
	private static final String WRITE_CONVERTER_NOT_SIMPLE = "Registering converter from %s to %s as writing converter although it doesn't convert to a Mongo supported type! You might wanna check you annotation setup at the converter implementation.";

	private final Set<ConvertiblePair> readingPairs;
	private final Set<ConvertiblePair> writingPairs;
	private final Set<Class<?>> customSimpleTypes;
	private final SimpleTypeHolder simpleTypeHolder;
	private final ConcurrentMap<ConvertiblePair, CacheValue> customReadTargetTypes;

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

		this.readingPairs = new LinkedHashSet<ConvertiblePair>();
		this.writingPairs = new LinkedHashSet<ConvertiblePair>();
		this.customSimpleTypes = new HashSet<Class<?>>();
		this.customReadTargetTypes = new ConcurrentHashMap<GenericConverter.ConvertiblePair, CacheValue>();

		List<Object> toRegister = new ArrayList<Object>();

		// Add user provided converters to make sure they can override the defaults
		toRegister.addAll(converters);
		toRegister.add(CustomToStringConverter.INSTANCE);
		toRegister.add(BigDecimalToStringConverter.INSTANCE);
		toRegister.add(StringToBigDecimalConverter.INSTANCE);
		toRegister.add(BigIntegerToStringConverter.INSTANCE);
		toRegister.add(StringToBigIntegerConverter.INSTANCE);
		toRegister.add(URLToStringConverter.INSTANCE);
		toRegister.add(StringToURLConverter.INSTANCE);
		toRegister.add(DBObjectToStringConverter.INSTANCE);
		toRegister.add(TermToStringConverter.INSTANCE);

		toRegister.addAll(JodaTimeConverters.getConvertersToRegister());
		toRegister.addAll(GeoConverters.getConvertersToRegister());

		for (Object c : toRegister) {
			registerConversion(c);
		}

		Collections.reverse(toRegister);

		this.converters = Collections.unmodifiableList(toRegister);
		this.simpleTypeHolder = new SimpleTypeHolder(customSimpleTypes, MongoSimpleTypes.HOLDER);
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
	 * Returns whether the given type is considered to be simple. That means it's either a general simple type or we have
	 * a writing {@link Converter} registered for a particular type.
	 * 
	 * @see SimpleTypeHolder#isSimpleType(Class)
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

		Class<?> type = converter.getClass();
		boolean isWriting = type.isAnnotationPresent(WritingConverter.class);
		boolean isReading = type.isAnnotationPresent(ReadingConverter.class);

		if (converter instanceof GenericConverter) {
			GenericConverter genericConverter = (GenericConverter) converter;
			for (ConvertiblePair pair : genericConverter.getConvertibleTypes()) {
				register(new ConverterRegistration(pair, isReading, isWriting));
			}
		} else if (converter instanceof Converter) {
			Class<?>[] arguments = GenericTypeResolver.resolveTypeArguments(converter.getClass(), Converter.class);
			register(new ConverterRegistration(arguments[0], arguments[1], isReading, isWriting));
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
	private void register(ConverterRegistration converterRegistration) {

		ConvertiblePair pair = converterRegistration.getConvertiblePair();

		if (converterRegistration.isReading()) {

			readingPairs.add(pair);

			if (LOG.isWarnEnabled() && !converterRegistration.isSimpleSourceType()) {
				LOG.warn(String.format(READ_CONVERTER_NOT_SIMPLE, pair.getSourceType(), pair.getTargetType()));
			}
		}

		if (converterRegistration.isWriting()) {

			writingPairs.add(pair);
			customSimpleTypes.add(pair.getSourceType());

			if (LOG.isWarnEnabled() && !converterRegistration.isSimpleTargetType()) {
				LOG.warn(String.format(WRITE_CONVERTER_NOT_SIMPLE, pair.getSourceType(), pair.getTargetType()));
			}
		}
	}

	/**
	 * Returns the target type to convert to in case we have a custom conversion registered to convert the given source
	 * type into a Mongo native one.
	 * 
	 * @param sourceType must not be {@literal null}
	 * @return
	 */
	public Class<?> getCustomWriteTarget(Class<?> sourceType) {
		return getCustomWriteTarget(sourceType, null);
	}

	/**
	 * Returns the target type we can write an onject of the given source type to. The returned type might be a subclass
	 * oth the given expected type though. If {@code expectedTargetType} is {@literal null} we will simply return the
	 * first target type matching or {@literal null} if no conversion can be found.
	 * 
	 * @param sourceType must not be {@literal null}
	 * @param requestedTargetType
	 * @return
	 */
	public Class<?> getCustomWriteTarget(Class<?> sourceType, Class<?> requestedTargetType) {

		Assert.notNull(sourceType);

		return getCustomTarget(sourceType, requestedTargetType, writingPairs);
	}

	/**
	 * Returns whether we have a custom conversion registered to write into a Mongo native type. The returned type might
	 * be a subclass of the given expected type though.
	 * 
	 * @param sourceType must not be {@literal null}
	 * @return
	 */
	public boolean hasCustomWriteTarget(Class<?> sourceType) {

		Assert.notNull(sourceType);
		return hasCustomWriteTarget(sourceType, null);
	}

	/**
	 * Returns whether we have a custom conversion registered to write an object of the given source type into an object
	 * of the given Mongo native target type.
	 * 
	 * @param sourceType must not be {@literal null}.
	 * @param requestedTargetType
	 * @return
	 */
	public boolean hasCustomWriteTarget(Class<?> sourceType, Class<?> requestedTargetType) {

		Assert.notNull(sourceType);
		return getCustomWriteTarget(sourceType, requestedTargetType) != null;
	}

	/**
	 * Returns whether we have a custom conversion registered to read the given source into the given target type.
	 * 
	 * @param sourceType must not be {@literal null}
	 * @param requestedTargetType must not be {@literal null}
	 * @return
	 */
	public boolean hasCustomReadTarget(Class<?> sourceType, Class<?> requestedTargetType) {

		Assert.notNull(sourceType);
		Assert.notNull(requestedTargetType);

		return getCustomReadTarget(sourceType, requestedTargetType) != null;
	}

	/**
	 * Inspects the given {@link ConvertiblePair} for ones that have a source compatible type as source. Additionally
	 * checks assignability of the target type if one is given.
	 * 
	 * @param sourceType must not be {@literal null}.
	 * @param requestedTargetType can be {@literal null}.
	 * @param pairs must not be {@literal null}.
	 * @return
	 */
	private static Class<?> getCustomTarget(Class<?> sourceType, Class<?> requestedTargetType,
			Iterable<ConvertiblePair> pairs) {

		Assert.notNull(sourceType);
		Assert.notNull(pairs);

		for (ConvertiblePair typePair : pairs) {
			if (typePair.getSourceType().isAssignableFrom(sourceType)) {
				Class<?> targetType = typePair.getTargetType();
				if (requestedTargetType == null || targetType.isAssignableFrom(requestedTargetType)) {
					return targetType;
				}
			}
		}

		return null;
	}

	/**
	 * Returns the actual target type for the given {@code sourceType} and {@code requestedTargetType}. Note that the
	 * returned {@link Class} could be an assignable type to the given {@code requestedTargetType}.
	 * 
	 * @param sourceType must not be {@literal null}.
	 * @param requestedTargetType can be {@literal null}.
	 * @return
	 */
	private Class<?> getCustomReadTarget(Class<?> sourceType, Class<?> requestedTargetType) {

		Assert.notNull(sourceType);

		if (requestedTargetType == null) {
			return null;
		}

		ConvertiblePair lookupKey = new ConvertiblePair(sourceType, requestedTargetType);
		CacheValue readTargetTypeValue = customReadTargetTypes.get(lookupKey);

		if (readTargetTypeValue != null) {
			return readTargetTypeValue.getType();
		}

		readTargetTypeValue = CacheValue.of(getCustomTarget(sourceType, requestedTargetType, readingPairs));
		CacheValue cacheValue = customReadTargetTypes.putIfAbsent(lookupKey, readTargetTypeValue);

		return cacheValue != null ? cacheValue.getType() : readTargetTypeValue.getType();
	}

	@WritingConverter
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

	/**
	 * Wrapper to safely store {@literal null} values in the type cache.
	 * 
	 * @author Patryk Wasik
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	private static class CacheValue {

		private static final CacheValue ABSENT = new CacheValue(null);

		private final Class<?> type;

		public CacheValue(Class<?> type) {
			this.type = type;
		}

		public Class<?> getType() {
			return type;
		}

		static CacheValue of(Class<?> type) {
			return type == null ? ABSENT : new CacheValue(type);
		}
	}
}
