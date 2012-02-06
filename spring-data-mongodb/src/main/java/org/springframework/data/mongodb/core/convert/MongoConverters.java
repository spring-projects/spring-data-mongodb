/*
 * Copyright 2011-2012 the original author or authors.
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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import org.bson.BSON;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.StringUtils;

/**
 * Wrapper class to contain useful converters for the usage with Mongo.
 * 
 * @author Oliver Gierke
 */
abstract class MongoConverters {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private MongoConverters() {

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

	/**
	 * Custom {@link Converter} to convert {@link UUID}s into {@link Binary}s.
	 * 
	 * @author Oliver Gierke
	 */
	public static enum UUIDToBinaryConverter implements Converter<UUID, Binary> {

		INSTANCE;

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		public Binary convert(UUID source) {

			try {
				return source == null ? null : new Binary(BSON.B_UUID, source.toString().getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new MappingException(String.format("Could nor convert UUID %s into Binary!", source.toString()), e);
			}
		}
	}

	public static enum BinaryToUUIDConverter implements Converter<Binary, UUID> {

		INSTANCE;

		private static final Logger LOG = LoggerFactory.getLogger(BinaryToUUIDConverter.class);

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		public UUID convert(Binary source) {

			if (BSON.B_UUID != source.getType()) {
				LOG.warn(String.format("Source binary %s is not an UUID actually! Trying to read it nevertheless...",
						source.toString()));
			}

			try {
				return source == null ? null : UUID.fromString(new String(source.getData(), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new MappingException(String.format("Could not convert Binary %s into UUID!", source.toString()), e);
			}
		}
	}
}
