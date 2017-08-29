/*
 * Copyright 2017 the original author or authors.
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
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.ConverterBuilder.ConverterAware;
import org.springframework.data.convert.JodaTimeConverters;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.lang.Nullable;

/**
 * Value object to capture custom conversion. {@link MongoCustomConversions} also act as factory for
 * {@link org.springframework.data.mapping.model.SimpleTypeHolder}
 *
 * @author Mark Paluch
 * @since 2.0
 * @see org.springframework.data.convert.CustomConversions
 * @see org.springframework.data.mapping.model.SimpleTypeHolder
 * @see MongoSimpleTypes
 */
public class MongoCustomConversions extends org.springframework.data.convert.CustomConversions {

	private static final StoreConversions STORE_CONVERSIONS;
	private static final List<Object> STORE_CONVERTERS;

	static {

		List<Object> converters = new ArrayList<>();

		converters.add(CustomToStringConverter.INSTANCE);
		converters.addAll(MongoConverters.getConvertersToRegister());
		converters.addAll(JodaTimeConverters.getConvertersToRegister());
		converters.addAll(GeoConverters.getConvertersToRegister());

		STORE_CONVERTERS = Collections.unmodifiableList(converters);
		STORE_CONVERSIONS = StoreConversions.of(MongoSimpleTypes.HOLDER, STORE_CONVERTERS);
	}

	/**
	 * TODO: actually checking if a ConverterFactory is capable of converting to a desired target type should be part of
	 * SD-Commons in {@link org.springframework.data.convert.CustomConversions}.
	 */

	private final GenericConversionService cs;

	/**
	 * Creates an empty {@link MongoCustomConversions} object.
	 */
	MongoCustomConversions() {
		this(Collections.emptyList());
	}

	/**
	 * Create a new {@link MongoCustomConversions} instance registering the given converters.
	 *
	 * @param converters must not be {@literal null}.
	 */
	public MongoCustomConversions(List<?> converters) {

		super(STORE_CONVERSIONS, converters);
		this.cs = converters.isEmpty() ? null : new GenericConversionService();
		if (!converters.isEmpty()) {
			converters.forEach(it -> registerConverterIn(it, cs));
		}
	}

	private void registerConverterIn(Object candidate, ConverterRegistry conversionService) {

		if (candidate instanceof Converter) {
			conversionService.addConverter(Converter.class.cast(candidate));
		}

		if (candidate instanceof ConverterFactory) {
			conversionService.addConverterFactory(ConverterFactory.class.cast(candidate));
		}

		if (candidate instanceof GenericConverter) {
			conversionService.addConverter(GenericConverter.class.cast(candidate));
		}

		if (candidate instanceof ConverterAware) {
			ConverterAware.class.cast(candidate).getConverters().forEach(it -> registerConverterIn(it, conversionService));
		}

	}

	// TODO: should actually be part of sd commons.
	@Override
	public Optional<Class<?>> getCustomWriteTarget(Class<?> sourceType, Class<?> requestedTargetType) {

		Optional<Class<?>> target = super.getCustomWriteTarget(sourceType, requestedTargetType);

		if (cs == null) {
			return target;
		}

		if (!target.isPresent()) {
			return target;
		}

		if (cs.canConvert(sourceType, requestedTargetType)) {
			return target;
		}

		return Optional.empty();
	}

	// TODO: should actually be part of sd commons
	@Override
	public boolean hasCustomReadTarget(Class<?> sourceType, Class<?> targetType) {

		boolean x = super.hasCustomReadTarget(sourceType, targetType);

		if (cs == null) {
			return x;
		}
		if (!x) {
			return false;
		}

		return cs.canConvert(sourceType, targetType);
	}

	@WritingConverter
	private enum CustomToStringConverter implements GenericConverter {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.GenericConverter#getConvertibleTypes()
		 */
		public Set<ConvertiblePair> getConvertibleTypes() {

			ConvertiblePair localeToString = new ConvertiblePair(Locale.class, String.class);
			ConvertiblePair booleanToString = new ConvertiblePair(Character.class, String.class);

			return new HashSet<>(Arrays.asList(localeToString, booleanToString));
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.GenericConverter#convert(java.lang.Object, org.springframework.core.convert.TypeDescriptor, org.springframework.core.convert.TypeDescriptor)
		 */
		public Object convert(@Nullable Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			return source != null ? source.toString() : null;
		}
	}
}
