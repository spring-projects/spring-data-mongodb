/*
 * Copyright 2011-2016 the original author or authors.
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

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Currency;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.types.Code;
import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalConverter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.query.Term;
import org.springframework.data.mongodb.core.script.NamedMongoScript;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

/**
 * Wrapper class to contain useful converters for the usage with Mongo.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public abstract class MongoConverters {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private MongoConverters() {}

	/**
	 * Returns the converters to be registered.
	 *
	 * @return
	 * @since 1.9
	 */
	public static Collection<Object> getConvertersToRegister() {

		List<Object> converters = new ArrayList<Object>();

		converters.add(BigDecimalToStringConverter.INSTANCE);
		converters.add(StringToBigDecimalConverter.INSTANCE);
		converters.add(BigIntegerToStringConverter.INSTANCE);
		converters.add(StringToBigIntegerConverter.INSTANCE);
		converters.add(URLToStringConverter.INSTANCE);
		converters.add(StringToURLConverter.INSTANCE);
		converters.add(DocumentToStringConverter.INSTANCE);
		converters.add(TermToStringConverter.INSTANCE);
		converters.add(NamedMongoScriptToDocumentConverter.INSTANCE);
		converters.add(DocumentToNamedMongoScriptConverter.INSTANCE);
		converters.add(CurrencyToStringConverter.INSTANCE);
		converters.add(StringToCurrencyConverter.INSTANCE);
		converters.add(AtomicIntegerToIntegerConverter.INSTANCE);
		converters.add(AtomicLongToLongConverter.INSTANCE);
		converters.add(LongToAtomicLongConverter.INSTANCE);
		converters.add(IntegerToAtomicIntegerConverter.INSTANCE);

		return converters;
	}

	/**
	 * Simple singleton to convert {@link ObjectId}s to their {@link String} representation.
	 *
	 * @author Oliver Gierke
	 */
	public static enum ObjectIdToStringConverter implements Converter<ObjectId, String> {
		INSTANCE;

		public String convert(ObjectId id) {
			return id == null ? null : id.toString();
		}
	}

	/**
	 * Simple singleton to convert {@link String}s to their {@link ObjectId} representation.
	 *
	 * @author Oliver Gierke
	 */
	public static enum StringToObjectIdConverter implements Converter<String, ObjectId> {
		INSTANCE;

		public ObjectId convert(String source) {
			return StringUtils.hasText(source) ? new ObjectId(source) : null;
		}
	}

	/**
	 * Simple singleton to convert {@link ObjectId}s to their {@link java.math.BigInteger} representation.
	 *
	 * @author Oliver Gierke
	 */
	public static enum ObjectIdToBigIntegerConverter implements Converter<ObjectId, BigInteger> {
		INSTANCE;

		public BigInteger convert(ObjectId source) {
			return source == null ? null : new BigInteger(source.toString(), 16);
		}
	}

	/**
	 * Simple singleton to convert {@link BigInteger}s to their {@link ObjectId} representation.
	 *
	 * @author Oliver Gierke
	 */
	public static enum BigIntegerToObjectIdConverter implements Converter<BigInteger, ObjectId> {
		INSTANCE;

		public ObjectId convert(BigInteger source) {
			return source == null ? null : new ObjectId(source.toString(16));
		}
	}

	public static enum BigDecimalToStringConverter implements Converter<BigDecimal, String> {
		INSTANCE;

		public String convert(BigDecimal source) {
			return source == null ? null : source.toString();
		}
	}

	public static enum StringToBigDecimalConverter implements Converter<String, BigDecimal> {
		INSTANCE;

		public BigDecimal convert(String source) {
			return StringUtils.hasText(source) ? new BigDecimal(source) : null;
		}
	}

	public static enum BigIntegerToStringConverter implements Converter<BigInteger, String> {
		INSTANCE;

		public String convert(BigInteger source) {
			return source == null ? null : source.toString();
		}
	}

	public static enum StringToBigIntegerConverter implements Converter<String, BigInteger> {
		INSTANCE;

		public BigInteger convert(String source) {
			return StringUtils.hasText(source) ? new BigInteger(source) : null;
		}
	}

	public static enum URLToStringConverter implements Converter<URL, String> {
		INSTANCE;

		public String convert(URL source) {
			return source == null ? null : source.toString();
		}
	}

	public static enum StringToURLConverter implements Converter<String, URL> {
		INSTANCE;

		private static final TypeDescriptor SOURCE = TypeDescriptor.valueOf(String.class);
		private static final TypeDescriptor TARGET = TypeDescriptor.valueOf(URL.class);

		public URL convert(String source) {

			try {
				return source == null ? null : new URL(source);
			} catch (MalformedURLException e) {
				throw new ConversionFailedException(SOURCE, TARGET, source, e);
			}
		}
	}

	@ReadingConverter
	public static enum DocumentToStringConverter implements Converter<Document, String> {

		INSTANCE;

		@Override
		public String convert(Document source) {

			if (source == null) {
				return null;
			}

			return source.toJson();
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	@WritingConverter
	public static enum TermToStringConverter implements Converter<Term, String> {

		INSTANCE;

		@Override
		public String convert(Term source) {
			return source == null ? null : source.getFormatted();
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static enum DocumentToNamedMongoScriptConverter implements Converter<Document, NamedMongoScript> {

		INSTANCE;

		@Override
		public NamedMongoScript convert(Document source) {

			if (source == null) {
				return null;
			}

			String id = source.get("_id").toString();
			Object rawValue = source.get("value");

			return new NamedMongoScript(id, ((Code) rawValue).getCode());
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static enum NamedMongoScriptToDocumentConverter implements Converter<NamedMongoScript, Document> {

		INSTANCE;

		@Override
		public Document convert(NamedMongoScript source) {

			if (source == null) {
				return new Document();
			}

			Document document = new Document();

			document.put("_id", source.getName());
			document.put("value", new Code(source.getCode()));

			return document;
		}
	}

	/**
	 * {@link Converter} implementation converting {@link Currency} into its ISO 4217 {@link String} representation.
	 *
	 * @author Christoph Strobl
	 * @since 1.9
	 */
	@WritingConverter
	public static enum CurrencyToStringConverter implements Converter<Currency, String> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public String convert(Currency source) {
			return source == null ? null : source.getCurrencyCode();
		}
	}

	/**
	 * {@link Converter} implementation converting ISO 4217 {@link String} into {@link Currency}.
	 *
	 * @author Christoph Strobl
	 * @since 1.9
	 */
	@ReadingConverter
	public static enum StringToCurrencyConverter implements Converter<String, Currency> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Currency convert(String source) {
			return StringUtils.hasText(source) ? Currency.getInstance(source) : null;
		}
	}

	/**
	 * {@link ConverterFactory} implementation using {@link NumberUtils} for number conversion and parsing. Additionally
	 * deals with {@link AtomicInteger} and {@link AtomicLong} by calling {@code get()} before performing the actual
	 * conversion.
	 *
	 * @author Christoph Strobl
	 * @since 1.9
	 */
	@WritingConverter
	public static enum NumberToNumberConverterFactory implements ConverterFactory<Number, Number>, ConditionalConverter {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.ConverterFactory#getConverter(java.lang.Class)
		 */
		@Override
		public <T extends Number> Converter<Number, T> getConverter(Class<T> targetType) {
			return new NumberToNumberConverter<T>(targetType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.ConditionalConverter#matches(org.springframework.core.convert.TypeDescriptor, org.springframework.core.convert.TypeDescriptor)
		 */
		@Override
		public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
			return !sourceType.equals(targetType);
		}

		private final static class NumberToNumberConverter<T extends Number> implements Converter<Number, T> {

			private final Class<T> targetType;

			/**
			 * Creates a new {@link NumberToNumberConverter} for the given target type.
			 *
			 * @param targetType must not be {@literal null}.
			 */
			public NumberToNumberConverter(Class<T> targetType) {

				Assert.notNull(targetType, "Target type must not be null!");

				this.targetType = targetType;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
			 */
			@Override
			public T convert(Number source) {

				if (source instanceof AtomicInteger) {
					return NumberUtils.convertNumberToTargetClass(((AtomicInteger) source).get(), this.targetType);
				}

				if (source instanceof AtomicLong) {
					return NumberUtils.convertNumberToTargetClass(((AtomicLong) source).get(), this.targetType);
				}

				return NumberUtils.convertNumberToTargetClass(source, this.targetType);
			}
		}
	}

	/**
	 * {@link ConverterFactory} implementation converting {@link AtomicLong} into {@link Long}.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	@WritingConverter
	public static enum AtomicLongToLongConverter implements Converter<AtomicLong, Long> {
		INSTANCE;

		@Override
		public Long convert(AtomicLong source) {
			return NumberUtils.convertNumberToTargetClass(source, Long.class);
		}
	}

	/**
	 * {@link ConverterFactory} implementation converting {@link AtomicInteger} into {@link Integer}.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	@WritingConverter
	public static enum AtomicIntegerToIntegerConverter implements Converter<AtomicInteger, Integer> {
		INSTANCE;

		@Override
		public Integer convert(AtomicInteger source) {
			return NumberUtils.convertNumberToTargetClass(source, Integer.class);
		}
	}

	/**
	 * {@link ConverterFactory} implementation converting {@link Long} into {@link AtomicLong}.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	@ReadingConverter
	public static enum LongToAtomicLongConverter implements Converter<Long, AtomicLong> {
		INSTANCE;

		@Override
		public AtomicLong convert(Long source) {
			return source != null ? new AtomicLong(source) : null;
		}
	}

	/**
	 * {@link ConverterFactory} implementation converting {@link Integer} into {@link AtomicInteger}.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	@ReadingConverter
	public static enum IntegerToAtomicIntegerConverter implements Converter<Integer, AtomicInteger> {
		INSTANCE;

		@Override
		public AtomicInteger convert(Integer source) {
			return source != null ? new AtomicInteger(source) : null;
		}
	}

	public static ConverterFactory<Object, Document> codecAwareObjectToDocumentConverter(CodecRegistry registry) {
		return new CodecAwareDocumentConverterFactory(registry);
	}

	public static ConverterFactory<Document, Object> codecAwareDocumentToObjectConverter(CodecRegistry registry) {
		return new CodecAwareToObjectConverterFactory(registry);
	}

	/**
	 * TODO: Move to a better place
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@WritingConverter
	static class CodecAwareDocumentConverterFactory implements ConverterFactory<Object, Document>, ConditionalConverter {

		private final CodecRegistry registry;

		CodecAwareDocumentConverterFactory(CodecRegistry registry) {
			this.registry = registry;
		}

		@Override
		public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {

			try {
				return registry.get(sourceType.getType()) != null;
			} catch (CodecConfigurationException ex) {}
			return false;
		}

		@Override
		public <T extends Document> Converter<Object, T> getConverter(Class<T> targetType) {

			return (source) -> {

				Codec codec = registry.get(source.getClass());
				StringWriter sw = new StringWriter();
				codec.encode(new JsonWriter(sw), source, EncoderContext.builder().build());
				sw.flush();
				return (T) Document.parse(sw.toString());
			};
		}
	}

	/**
	 * TODO: Move to a better place
	 *
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	@ReadingConverter
	static class CodecAwareToObjectConverterFactory implements ConverterFactory<Document, Object>, ConditionalConverter {

		private final CodecRegistry registry;

		CodecAwareToObjectConverterFactory(CodecRegistry registry) {
			this.registry = registry;
		}

		@Override
		public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {

			try {
				return registry.get(targetType.getType()) != null;
			} catch (CodecConfigurationException ex) {}
			return false;
		}

		@Override
		public <T extends Object> Converter<Document, T> getConverter(Class<T> targetType) {
			return (source) -> {

				Codec codec = registry.get(targetType);
				return (T) codec.decode(new JsonReader(source.toJson()), DecoderContext.builder().build());
			};
		}
	}
}
