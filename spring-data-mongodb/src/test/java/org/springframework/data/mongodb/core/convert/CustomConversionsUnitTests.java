package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.net.URL;
import java.text.DateFormat;
import java.text.Format;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToBigIntegerConverter;

import com.mongodb.DBRef;

/**
 * Unit tests for {@link CustomConversions}.
 * 
 * @author Oliver Gierke
 */
public class CustomConversionsUnitTests {

	@Test
	@SuppressWarnings("unchecked")
	public void findsBasicReadAndWriteConversions() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(FormatToStringConverter.INSTANCE,
				StringToFormatConverter.INSTANCE));

		assertThat(conversions.getCustomWriteTarget(Format.class, null), is(typeCompatibleWith(String.class)));
		assertThat(conversions.getCustomWriteTarget(String.class, null), is(nullValue()));

		assertThat(conversions.hasCustomReadTarget(String.class, Format.class), is(true));
		assertThat(conversions.hasCustomReadTarget(String.class, Locale.class), is(false));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void considersSubtypesCorrectly() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(NumberToStringConverter.INSTANCE,
				StringToNumberConverter.INSTANCE));

		assertThat(conversions.getCustomWriteTarget(Long.class, null), is(typeCompatibleWith(String.class)));
		assertThat(conversions.hasCustomReadTarget(String.class, Long.class), is(true));
	}

	@Test
	public void considersTypesWeRegisteredConvertersForAsSimple() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(FormatToStringConverter.INSTANCE));
		assertThat(conversions.isSimpleType(UUID.class), is(true));
	}

	/**
	 * @see DATAMONGO-240
	 */
	@Test
	public void considersObjectIdToBeSimpleType() {

		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.isSimpleType(ObjectId.class), is(true));
		assertThat(conversions.hasCustomWriteTarget(ObjectId.class), is(false));

	}

	/**
	 * @see DATAMONGO-240
	 */
	@Test
	public void considersCustomConverterForSimpleType() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(new Converter<ObjectId, String>() {
			public String convert(ObjectId source) {
				return source == null ? null : source.toString();
			}
		}));

		assertThat(conversions.isSimpleType(ObjectId.class), is(true));
		assertThat(conversions.hasCustomWriteTarget(ObjectId.class), is(true));
		assertThat(conversions.hasCustomReadTarget(ObjectId.class, String.class), is(true));
		assertThat(conversions.hasCustomReadTarget(ObjectId.class, Object.class), is(false));
	}

	@Test
	public void considersDBRefsToBeSimpleTypes() {

		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.isSimpleType(DBRef.class), is(true));
	}

	@Test
	public void populatesConversionServiceCorrectly() {

		GenericConversionService conversionService = new DefaultConversionService();

		CustomConversions conversions = new CustomConversions(Arrays.asList(StringToFormatConverter.INSTANCE));
		conversions.registerConvertersIn(conversionService);

		assertThat(conversionService.canConvert(String.class, Format.class), is(true));
	}

	/**
	 * @see DATAMONGO-259
	 */
	@Test
	public void doesNotConsiderTypeSimpleIfOnlyReadConverterIsRegistered() {
		CustomConversions conversions = new CustomConversions(Arrays.asList(StringToFormatConverter.INSTANCE));
		assertThat(conversions.isSimpleType(Format.class), is(false));
	}

	/**
	 * @see DATAMONGO-298
	 */
	@Test
	public void discoversConvertersForSubtypesOfMongoTypes() {

		CustomConversions conversions = new CustomConversions(Arrays.asList(StringToIntegerConverter.INSTANCE));
		assertThat(conversions.hasCustomReadTarget(String.class, Integer.class), is(true));
		assertThat(conversions.hasCustomWriteTarget(String.class, Integer.class), is(true));
	}

	/**
	 * @see DATAMONGO-342
	 */
	@Test
	public void doesNotHaveConverterForStringToBigIntegerByDefault() {

		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.hasCustomWriteTarget(String.class), is(false));
		assertThat(conversions.getCustomWriteTarget(String.class), is(nullValue()));

		conversions = new CustomConversions(Arrays.asList(StringToBigIntegerConverter.INSTANCE));
		assertThat(conversions.hasCustomWriteTarget(String.class), is(false));
		assertThat(conversions.getCustomWriteTarget(String.class), is(nullValue()));
	}

	/**
	 * @see DATAMONGO-390
	 */
	@Test
	public void considersBinaryASimpleType() {

		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.isSimpleType(Binary.class), is(true));
	}

	/**
	 * @see DATAMONGO-462
	 */
	@Test
	public void hasWriteConverterForURL() {

		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.hasCustomWriteTarget(URL.class), is(true));
	}

	/**
	 * @see DATAMONGO-462
	 */
	@Test
	public void readTargetForURL() {
		CustomConversions conversions = new CustomConversions();
		assertThat(conversions.hasCustomReadTarget(String.class, URL.class), is(true));
	}

	enum FormatToStringConverter implements Converter<Format, String> {
		INSTANCE;

		public String convert(Format source) {
			return source.toString();
		}
	}

	enum StringToFormatConverter implements Converter<String, Format> {
		INSTANCE;
		public Format convert(String source) {
			return DateFormat.getInstance();
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

	enum StringToIntegerConverter implements Converter<String, Integer> {
		INSTANCE;
		public Integer convert(String source) {
			return 0;
		}
	}
}
