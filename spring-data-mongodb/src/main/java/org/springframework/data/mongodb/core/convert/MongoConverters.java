/*
 * Copyright 2011-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.convert;

import static org.springframework.data.convert.ConverterBuilder.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BinaryVector;
import org.bson.BsonArray;
import org.bson.BsonDouble;
import org.bson.BsonReader;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.jspecify.annotations.Nullable;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalConverter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.domain.Vector;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoVector;
import org.springframework.data.mongodb.core.query.Term;
import org.springframework.data.mongodb.core.script.NamedMongoScript;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

import com.mongodb.MongoClientSettings;

/**
 * Wrapper class to contain useful converters for the usage with Mongo.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
abstract class MongoConverters {

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
	static Collection<Object> getConvertersToRegister() {

		List<Object> converters = new ArrayList<>();

		converters.add(BigDecimalToDecimal128Converter.INSTANCE);
		converters.add(Decimal128ToBigDecimalConverter.INSTANCE);
		converters.add(BigIntegerToDecimal128Converter.INSTANCE);

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
		converters.add(BinaryToByteArrayConverter.INSTANCE);
		converters.add(BsonTimestampToInstantConverter.INSTANCE);
		converters.add(NumberToNumberConverterFactory.INSTANCE);

		converters.add(VectorToBsonArrayConverter.INSTANCE);
		converters.add(ListToVectorConverter.INSTANCE);
		converters.add(BinaryVectorToMongoVectorConverter.INSTANCE);

		converters.add(reading(BsonUndefined.class, Object.class, it -> null));
		converters.add(reading(String.class, URI.class, URI::create).andWriting(URI::toString));

		return converters;
	}

	/**
	 * Simple singleton to convert {@link ObjectId}s to their {@link String} representation.
	 *
	 * @author Oliver Gierke
	 */
	enum ObjectIdToStringConverter implements Converter<ObjectId, String> {
		INSTANCE;

		public String convert(ObjectId id) {
			return id.toString();
		}
	}

	/**
	 * Simple singleton to convert {@link String}s to their {@link ObjectId} representation.
	 *
	 * @author Oliver Gierke
	 */
	enum StringToObjectIdConverter implements Converter<String, ObjectId> {
		INSTANCE;

		public @Nullable ObjectId convert(String source) {
			return StringUtils.hasText(source) ? new ObjectId(source) : null;
		}
	}

	/**
	 * Simple singleton to convert {@link ObjectId}s to their {@link java.math.BigInteger} representation.
	 *
	 * @author Oliver Gierke
	 */
	enum ObjectIdToBigIntegerConverter implements Converter<ObjectId, BigInteger> {
		INSTANCE;

		public BigInteger convert(ObjectId source) {
			return new BigInteger(source.toString(), 16);
		}
	}

	/**
	 * Simple singleton to convert {@link BigInteger}s to their {@link ObjectId} representation.
	 *
	 * @author Oliver Gierke
	 */
	enum BigIntegerToObjectIdConverter implements Converter<BigInteger, ObjectId> {
		INSTANCE;

		public ObjectId convert(BigInteger source) {
			return new ObjectId(source.toString(16));
		}
	}

	enum BigDecimalToStringConverter implements Converter<BigDecimal, String> {
		INSTANCE;

		public String convert(BigDecimal source) {
			return source.toString();
		}
	}

	/**
	 * @since 2.2
	 */
	enum BigDecimalToDecimal128Converter implements Converter<BigDecimal, Decimal128> {
		INSTANCE;

		public Decimal128 convert(BigDecimal source) {
			return new Decimal128(source);
		}
	}

	/**
	 * @since 5.0
	 */
	enum BigIntegerToDecimal128Converter implements Converter<BigInteger, Decimal128> {
		INSTANCE;

		public Decimal128 convert(BigInteger source) {
			return new Decimal128(new BigDecimal(source));
		}
	}

	enum StringToBigDecimalConverter implements Converter<String, BigDecimal> {
		INSTANCE;

		public @Nullable BigDecimal convert(String source) {
			return StringUtils.hasText(source) ? new BigDecimal(source) : null;
		}
	}

	/**
	 * @since 2.2
	 */
	enum Decimal128ToBigDecimalConverter implements Converter<Decimal128, BigDecimal> {
		INSTANCE;

		public BigDecimal convert(Decimal128 source) {
			return source.bigDecimalValue();
		}
	}

	@WritingConverter
	enum BigIntegerToStringConverter implements Converter<BigInteger, String> {
		INSTANCE;

		public String convert(BigInteger source) {
			return source.toString();
		}
	}

	@ReadingConverter
	enum StringToBigIntegerConverter implements Converter<String, BigInteger> {
		INSTANCE;

		public @Nullable BigInteger convert(String source) {
			return StringUtils.hasText(source) ? new BigInteger(source) : null;
		}
	}

	enum URLToStringConverter implements Converter<URL, String> {
		INSTANCE;

		public String convert(URL source) {
			return source.toString();
		}
	}

	enum StringToURLConverter implements Converter<String, URL> {
		INSTANCE;

		private static final TypeDescriptor SOURCE = TypeDescriptor.valueOf(String.class);
		private static final TypeDescriptor TARGET = TypeDescriptor.valueOf(URL.class);

		public URL convert(String source) {

			try {
				return new URL(source);
			} catch (MalformedURLException e) {
				throw new ConversionFailedException(SOURCE, TARGET, source, e);
			}
		}
	}

	@ReadingConverter
	enum DocumentToStringConverter implements Converter<Document, String> {

		INSTANCE;

		private final Codec<Document> codec = CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new Codec<UUID>() {

			@Override
			public void encode(BsonWriter writer, UUID value, EncoderContext encoderContext) {
				writer.writeString(value.toString());
			}

			@Override
			public Class<UUID> getEncoderClass() {
				return UUID.class;
			}

			@Override
			public UUID decode(BsonReader reader, DecoderContext decoderContext) {
				throw new IllegalStateException("decode not supported");
			}
		}), MongoClientSettings.getDefaultCodecRegistry()).get(Document.class);

		@Override
		public String convert(Document source) {
			return source.toJson(codec);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	@WritingConverter
	enum TermToStringConverter implements Converter<Term, String> {

		INSTANCE;

		@Override
		public String convert(Term source) {
			return source.getFormatted();
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@SuppressWarnings("NullAway")
	enum DocumentToNamedMongoScriptConverter implements Converter<Document, NamedMongoScript> {

		INSTANCE;

		@Override
		public @Nullable NamedMongoScript convert(Document source) {

			if (source.isEmpty()) {
				return null;
			}

			String id = source.get(FieldName.ID.name()).toString();
			Assert.notNull(id, "Script id must not be null");

			Object rawValue = source.get("value");

			Assert.isInstanceOf(Code.class, rawValue);

			return new NamedMongoScript(id, ((Code) rawValue).getCode());
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum NamedMongoScriptToDocumentConverter implements Converter<NamedMongoScript, Document> {

		INSTANCE;

		@Override
		public Document convert(NamedMongoScript source) {

			Document document = new Document();

			document.put(FieldName.ID.name(), source.getName());
			document.put("value", new Code(source.getCode()));

			return document;
		}
	}

	/**
	 * {@link Converter} implementation converting {@link Currency} into its ISO 4217-2018 {@link String} representation.
	 *
	 * @author Christoph Strobl
	 * @since 1.9
	 */
	@WritingConverter
	enum CurrencyToStringConverter implements Converter<Currency, String> {

		INSTANCE;

		@Override
		public String convert(Currency source) {
			return source.getCurrencyCode();
		}
	}

	/**
	 * {@link Converter} implementation converting ISO 4217-2018 {@link String} into {@link Currency}.
	 *
	 * @author Christoph Strobl
	 * @since 1.9
	 */
	@ReadingConverter
	enum StringToCurrencyConverter implements Converter<String, Currency> {

		INSTANCE;

		@Override
		public @Nullable Currency convert(String source) {
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
	enum NumberToNumberConverterFactory implements ConverterFactory<Number, Number>, ConditionalConverter {

		INSTANCE;

		@Override
		public <T extends Number> Converter<Number, T> getConverter(Class<T> targetType) {
			return new NumberToNumberConverter<T>(targetType);
		}

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

				Assert.notNull(targetType, "Target type must not be null");

				this.targetType = targetType;
			}

			@Override
			public T convert(Number source) {

				if (source instanceof AtomicInteger atomicInteger) {
					return NumberUtils.convertNumberToTargetClass(atomicInteger.get(), this.targetType);
				}

				if (source instanceof AtomicLong atomicLong) {
					return NumberUtils.convertNumberToTargetClass(atomicLong.get(), this.targetType);
				}

				return NumberUtils.convertNumberToTargetClass(source, this.targetType);
			}
		}
	}

	@WritingConverter
	enum VectorToBsonArrayConverter implements Converter<Vector, Object> {

		INSTANCE;

		@Override
		public Object convert(Vector source) {

			if (source instanceof MongoVector mv) {
				return mv.getSource();
			}

			double[] doubleArray = source.toDoubleArray();

			BsonArray array = new BsonArray(doubleArray.length);

			for (double v : doubleArray) {
				array.add(new BsonDouble(v));
			}

			return array;
		}
	}

	@ReadingConverter
	enum ListToVectorConverter implements Converter<List<Number>, Vector> {

		INSTANCE;

		@Override
		public Vector convert(List<Number> source) {
			return Vector.of(source);
		}
	}

	@ReadingConverter
	enum BinaryVectorToMongoVectorConverter implements Converter<BinaryVector, Vector> {

		INSTANCE;

		@Override
		public Vector convert(BinaryVector source) {
			return MongoVector.of(source);
		}
	}

	@WritingConverter
	enum ByteArrayConverterFactory implements ConverterFactory<byte[], Object>, ConditionalConverter {

		INSTANCE;

		@Override
		public <T> Converter<byte[], T> getConverter(Class<T> targetType) {
			return new ByteArrayConverter<>(targetType);
		}

		@Override
		public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
			return targetType.getType() != Object.class && !sourceType.equals(targetType);
		}

		private final static class ByteArrayConverter<T> implements Converter<byte[], T> {

			private final Class<T> targetType;

			/**
			 * Creates a new {@link ByteArrayConverter} for the given target type.
			 *
			 * @param targetType must not be {@literal null}.
			 */
			public ByteArrayConverter(Class<T> targetType) {

				Assert.notNull(targetType, "Target type must not be null");

				this.targetType = targetType;
			}

			@Override
			public T convert(byte[] source) {

				if (this.targetType == BinaryVector.class) {
					return (T) BinaryVector.int8Vector(source);
				}
				return (T) source;
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
	enum AtomicLongToLongConverter implements Converter<AtomicLong, Long> {
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
	enum AtomicIntegerToIntegerConverter implements Converter<AtomicInteger, Integer> {
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
	enum LongToAtomicLongConverter implements Converter<Long, AtomicLong> {
		INSTANCE;

		@Override
		public AtomicLong convert(Long source) {
			return new AtomicLong(source);
		}
	}

	/**
	 * {@link ConverterFactory} implementation converting {@link Integer} into {@link AtomicInteger}.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	@ReadingConverter
	enum IntegerToAtomicIntegerConverter implements Converter<Integer, AtomicInteger> {
		INSTANCE;

		@Override
		public AtomicInteger convert(Integer source) {
			return new AtomicInteger(source);
		}
	}

	/**
	 * {@link Converter} implementation converting {@link Binary} into {@code byte[]}.
	 *
	 * @author Christoph Strobl
	 * @since 2.0.1
	 */
	@ReadingConverter
	enum BinaryToByteArrayConverter implements Converter<Binary, byte[]> {

		INSTANCE;

		@Override
		public byte[] convert(Binary source) {
			return source.getData();
		}
	}

	/**
	 * {@link Converter} implementation converting {@link BsonTimestamp} into {@link Instant}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1.2
	 */
	@ReadingConverter
	enum BsonTimestampToInstantConverter implements Converter<BsonTimestamp, Instant> {

		INSTANCE;

		@Override
		public Instant convert(BsonTimestamp source) {
			return Instant.ofEpochSecond(source.getTime(), 0);
		}
	}
}
