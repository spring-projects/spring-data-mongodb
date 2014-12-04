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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;

import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.query.Term;
import org.springframework.data.mongodb.core.script.CallableMongoScript;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Wrapper class to contain useful converters for the usage with Mongo.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
abstract class MongoConverters {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private MongoConverters() {}

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
	public static enum DBObjectToStringConverter implements Converter<DBObject, String> {

		INSTANCE;

		@Override
		public String convert(DBObject source) {
			return source == null ? null : source.toString();
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
	@ReadingConverter
	public static enum DBObjectToCallableMongoScriptCoverter implements Converter<DBObject, CallableMongoScript> {

		INSTANCE;

		@Override
		public CallableMongoScript convert(DBObject source) {

			if (source == null) {
				return null;
			}

			String id = source.get("_id").toString();
			Object rawValue = source.get("value");

			if (rawValue != null) {
				return new CallableMongoScript(id, rawValue.toString());
			}

			return new CallableMongoScript(id);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@WritingConverter
	public static enum CallableMongoScriptToDBObjectConverter implements Converter<CallableMongoScript, DBObject> {

		INSTANCE;

		@Override
		public DBObject convert(CallableMongoScript source) {

			if (source == null) {
				return new BasicDBObject();
			}

			BasicDBObjectBuilder builder = new BasicDBObjectBuilder();

			builder.append("_id", source.getName());
			if (source.getCode() != null) {
				builder.append("value", source.getCode());
			}

			return builder.get();
		}
	}
}
