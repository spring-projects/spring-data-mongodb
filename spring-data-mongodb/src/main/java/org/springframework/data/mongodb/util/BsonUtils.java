/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.mongodb.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.bson.BSONObject;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.bson.types.ObjectId;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoClientSettings;

/**
 * Internal API for operations on {@link Bson} elements that can be either {@link Document} or {@link DBObject}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public class BsonUtils {

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> T get(Bson bson, String key) {
		return (T) asMap(bson).get(key);
	}

	/**
	 * Remove {@code _id : null} from the given {@link Bson} if present.
	 * 
	 * @param bson must not be {@literal null}.
	 * @since 2.5
	 */
	public static void removeNullId(Bson bson) {

		if (!contains(bson, "_id", null)) {
			return;
		}

		removeFrom(bson, "_id");
	}

	/**
	 * Check if a given entry (key/value pair) is present in the given {@link Bson}.
	 *
	 * @param bson must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if (key/value pair) is present.
	 * @since 2.5
	 */
	public static boolean contains(Bson bson, String key, @Nullable Object value) {

		if (bson instanceof Document) {

			Document doc = (Document) bson;
			return doc.containsKey(key) && ObjectUtils.nullSafeEquals(doc.get(key), value);
		}
		if (bson instanceof BSONObject) {

			BSONObject bsonObject = (BSONObject) bson;
			return bsonObject.containsField(key) && ObjectUtils.nullSafeEquals(bsonObject.get(key), value);
		}

		Map<String, Object> map = asMap(bson);
		return map.containsKey(key) && ObjectUtils.nullSafeEquals(map.get(key), value);
	}

	public static Map<String, Object> asMap(Bson bson) {

		if (bson instanceof Document) {
			return (Document) bson;
		}
		if (bson instanceof BasicDBObject) {
			return ((BasicDBObject) bson);
		}
		if (bson instanceof DBObject) {
			return ((DBObject) bson).toMap();
		}

		return (Map) bson.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
	}

	public static void addToMap(Bson bson, String key, @Nullable Object value) {

		if (bson instanceof Document) {

			((Document) bson).put(key, value);
			return;
		}
		if (bson instanceof BSONObject) {

			((BSONObject) bson).put(key, value);
			return;
		}

		throw new IllegalArgumentException(String.format(
				"Cannot add key/value pair to %s. as map. Given Bson must be a Document or BSONObject!", bson.getClass()));
	}

	/**
	 * Add all entries from the given {@literal source} {@link Map} to the {@literal target}.
	 *
	 * @param target must not be {@literal null}.
	 * @param source must not be {@literal null}.
	 * @since 2.5
	 */
	public static void addAllToMap(Bson target, Map<String, ?> source) {

		if (target instanceof Document) {

			((Document) target).putAll(source);
			return;
		}

		if (target instanceof BSONObject) {

			((BSONObject) target).putAll(source);
			return;
		}

		throw new IllegalArgumentException(
				String.format("Cannot add all to %s. Given Bson must be a Document or BSONObject.", target.getClass()));
	}

	/**
	 * Remove the given {@literal key} from the {@link Bson} value.
	 *
	 * @param bson must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @since 2.5
	 */
	static void removeFrom(Bson bson, String key) {

		if (bson instanceof Document) {

			((Document) bson).remove(key);
			return;
		}

		if (bson instanceof BSONObject) {

			((BSONObject) bson).removeField(key);
			return;
		}

		throw new IllegalArgumentException(
				String.format("Cannot remove from %s. Given Bson must be a Document or BSONObject.", bson.getClass()));
	}

	/**
	 * Extract the corresponding plain value from {@link BsonValue}. Eg. plain {@link String} from
	 * {@link org.bson.BsonString}.
	 *
	 * @param value must not be {@literal null}.
	 * @return
	 * @since 2.1
	 */
	public static Object toJavaType(BsonValue value) {

		switch (value.getBsonType()) {
			case INT32:
				return value.asInt32().getValue();
			case INT64:
				return value.asInt64().getValue();
			case STRING:
				return value.asString().getValue();
			case DECIMAL128:
				return value.asDecimal128().doubleValue();
			case DOUBLE:
				return value.asDouble().getValue();
			case BOOLEAN:
				return value.asBoolean().getValue();
			case OBJECT_ID:
				return value.asObjectId().getValue();
			case DB_POINTER:
				return new DBRef(value.asDBPointer().getNamespace(), value.asDBPointer().getId());
			case BINARY:
				return value.asBinary().getData();
			case DATE_TIME:
				return new Date(value.asDateTime().getValue());
			case SYMBOL:
				return value.asSymbol().getSymbol();
			case ARRAY:
				return value.asArray().toArray();
			case DOCUMENT:
				return Document.parse(value.asDocument().toJson());
			default:
				return value;
		}
	}

	/**
	 * Convert a given simple value (eg. {@link String}, {@link Long}) to its corresponding {@link BsonValue}.
	 *
	 * @param source must not be {@literal null}.
	 * @return the corresponding {@link BsonValue} representation.
	 * @throws IllegalArgumentException if {@literal source} does not correspond to a {@link BsonValue} type.
	 * @since 3.0
	 */
	public static BsonValue simpleToBsonValue(Object source) {

		if (source instanceof BsonValue) {
			return (BsonValue) source;
		}

		if (source instanceof ObjectId) {
			return new BsonObjectId((ObjectId) source);
		}

		if (source instanceof String) {
			return new BsonString((String) source);
		}

		if (source instanceof Double) {
			return new BsonDouble((Double) source);
		}

		if (source instanceof Integer) {
			return new BsonInt32((Integer) source);
		}

		if (source instanceof Long) {
			return new BsonInt64((Long) source);
		}

		if (source instanceof byte[]) {
			return new BsonBinary((byte[]) source);
		}

		if (source instanceof Boolean) {
			return new BsonBoolean((Boolean) source);
		}

		if (source instanceof Float) {
			return new BsonDouble((Float) source);
		}

		throw new IllegalArgumentException(String.format("Unable to convert %s (%s) to BsonValue.", source,
				source != null ? source.getClass().getName() : "null"));
	}

	/**
	 * Merge the given {@link Document documents} into on in the given order. Keys contained within multiple documents are
	 * overwritten by their follow ups.
	 *
	 * @param documents must not be {@literal null}. Can be empty.
	 * @return the document containing all key value pairs.
	 * @since 2.2
	 */
	public static Document merge(Document... documents) {

		if (ObjectUtils.isEmpty(documents)) {
			return new Document();
		}

		if (documents.length == 1) {
			return documents[0];
		}

		Document target = new Document();
		Arrays.asList(documents).forEach(target::putAll);
		return target;
	}

	/**
	 * @param source
	 * @param orElse
	 * @return
	 * @since 2.2
	 */
	public static Document toDocumentOrElse(String source, Function<String, Document> orElse) {

		if (StringUtils.trimLeadingWhitespace(source).startsWith("{")) {
			return Document.parse(source);
		}

		return orElse.apply(source);
	}

	/**
	 * Serialize the given {@link Document} as Json applying default codecs if necessary.
	 *
	 * @param source
	 * @return
	 * @since 2.2.1
	 */
	@Nullable
	public static String toJson(@Nullable Document source) {

		if (source == null) {
			return null;
		}

		try {
			return source.toJson();
		} catch (Exception e) {
			return toJson((Object) source);
		}
	}

	/**
	 * Check if a given String looks like {@link Document#parse(String) parsable} json.
	 *
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the given value looks like a json document.
	 * @since 3.0
	 */
	public static boolean isJsonDocument(@Nullable String value) {
		return StringUtils.hasText(value) && (value.startsWith("{") && value.endsWith("}"));
	}

	/**
	 * Check if a given String looks like {@link org.bson.BsonArray#parse(String) parsable} json array.
	 *
	 * @param value can be {@literal null}.
	 * @return {@literal true} if the given value looks like a json array.
	 * @since 3.0
	 */
	public static boolean isJsonArray(@Nullable String value) {
		return StringUtils.hasText(value) && (value.startsWith("[") && value.endsWith("]"));
	}

	/**
	 * Parse the given {@literal json} to {@link Document} applying transformations as specified by a potentially given
	 * {@link org.bson.codecs.Codec}.
	 *
	 * @param json must not be {@literal null}.
	 * @param codecRegistryProvider can be {@literal null}. In that case the default {@link DocumentCodec} is used.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException if the required argument is {@literal null}.
	 * @since 3.0
	 */
	public static Document parse(String json, @Nullable CodecRegistryProvider codecRegistryProvider) {

		Assert.notNull(json, "Json must not be null!");

		if (codecRegistryProvider == null) {
			return Document.parse(json);
		}

		return Document.parse(json, codecRegistryProvider.getCodecFor(Document.class)
				.orElseGet(() -> new DocumentCodec(codecRegistryProvider.getCodecRegistry())));
	}

	/**
	 * Returns given object as {@link Collection}. Will return the {@link Collection} as is if the source is a
	 * {@link Collection} already, will convert an array into a {@link Collection} or simply create a single element
	 * collection for everything else.
	 *
	 * @param source
	 * @return
	 */
	public static Collection<?> asCollection(Object source) {

		if (source instanceof Collection) {
			return (Collection<?>) source;
		}

		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}

	@Nullable
	private static String toJson(@Nullable Object value) {

		if (value == null) {
			return null;
		}

		try {
			return value instanceof Document
					? ((Document) value).toJson(MongoClientSettings.getDefaultCodecRegistry().get(Document.class))
					: serializeValue(value);

		} catch (Exception e) {

			if (value instanceof Collection) {
				return toString((Collection<?>) value);
			} else if (value instanceof Map) {
				return toString((Map<?, ?>) value);
			} else if (ObjectUtils.isArray(value)) {
				return toString(Arrays.asList(ObjectUtils.toObjectArray(value)));
			}

			throw e instanceof JsonParseException ? (JsonParseException) e : new JsonParseException(e);
		}
	}

	private static String serializeValue(@Nullable Object value) {

		if (value == null) {
			return "null";
		}

		String documentJson = new Document("toBeEncoded", value).toJson();
		return documentJson.substring(documentJson.indexOf(':') + 1, documentJson.length() - 1).trim();
	}

	private static String toString(Map<?, ?> source) {

		return iterableToDelimitedString(source.entrySet(), "{ ", " }",
				entry -> String.format("\"%s\" : %s", entry.getKey(), toJson(entry.getValue())));
	}

	private static String toString(Collection<?> source) {
		return iterableToDelimitedString(source, "[ ", " ]", BsonUtils::toJson);
	}

	private static <T> String iterableToDelimitedString(Iterable<T> source, String prefix, String suffix,
			Converter<? super T, String> transformer) {

		StringJoiner joiner = new StringJoiner(", ", prefix, suffix);

		StreamSupport.stream(source.spliterator(), false).map(transformer::convert).forEach(joiner::add);

		return joiner.toString();
	}
}
