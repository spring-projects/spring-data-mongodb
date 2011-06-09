package org.springframework.data.document.mongodb.convert;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;

/**
 * Unit tests for {@link CustomConversions}.
 * 
 * @author Oliver Gierke
 */
public class CustomConversionsUnitTests {

	@Test
	@SuppressWarnings("unchecked")
	public void findsBasicReadAndWriteConversions() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(UuidToStringConverter.INSTANCE,
				StringToUUIDConverter.INSTANCE));

		assertThat(conversions.getCustomWriteTarget(UUID.class, null), is(typeCompatibleWith(String.class)));
		assertThat(conversions.getCustomWriteTarget(String.class, null), is(nullValue()));

		assertThat(conversions.hasCustomReadTarget(String.class, UUID.class), is(true));
		assertThat(conversions.hasCustomReadTarget(String.class, Locale.class), is(false));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void considersSubtypesCorrectly() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(
				NumberToStringConverter.INSTANCE, StringToNumberConverter.INSTANCE));

		assertThat(conversions.getCustomWriteTarget(Long.class, null), is(typeCompatibleWith(String.class)));
		assertThat(conversions.hasCustomReadTarget(String.class, Long.class), is(true));
	}

	@Test
	public void considersTypesWeRegisteredConvertersForAsSimple() {

		CustomConversions conversions = new CustomConversions(				Arrays.asList(UuidToStringConverter.INSTANCE));
		assertThat(conversions.isSimpleType(UUID.class), is(true));
	}

	@Test
	public void populatesConversionServiceCorrectly() {

		GenericConversionService conversionService = ConversionServiceFactory.createDefaultConversionService();
		assertThat(conversionService.canConvert(String.class, UUID.class), is(false));

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(StringToUUIDConverter.INSTANCE));
		conversions.registerConvertersIn(conversionService);
		
		assertThat(conversionService.canConvert(String.class, UUID.class), is(true));
	}

	enum UuidToStringConverter implements Converter<UUID, String> {
		INSTANCE;

		public String convert(UUID source) {
			return source.toString();
		}
	}

	enum StringToUUIDConverter implements Converter<String, UUID> {
		INSTANCE;
		public UUID convert(String source) {
			return UUID.fromString(source);
		}
	}

	enum NumberToStringConverter implements Converter<Number, String> {
		INSTANCE;
		public String convert(Number source) {
			return source.toString();
		}
	}

	enum StringToNumberConverter implements Converter<String, Number> {
		INSTANCE;
		public Number convert(String source) {
			return 0L;
		}
	}
}
