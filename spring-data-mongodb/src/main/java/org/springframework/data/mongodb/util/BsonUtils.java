/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.bson.*;
import org.bson.codecs.Codec;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.FieldName.Type;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
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

	/**
	 * The empty document (immutable). This document is serializable.
	 *
	 * @since 3.2.5
	 */
	public static final Document EMPTY_DOCUMENT = new EmptyDocument();

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> T get(Bson bson, String key) {
		return (T) asMap(bson).get(key);
	}

	/**
	 * Return the {@link Bson} object as {@link Map}. Depending on the input type, the return value can be either a casted
	 * version of {@code bson} or a converted (detached from the original value).
	 *
	 * @param bson
	 * @return
	 */
	public static Map<String, Object> asMap(Bson bson) {
		return asMap(bson, MongoClientSettings.getDefaultCodecRegistry());
	}

	/**
	 * Return the {@link Bson} object as {@link Map}. Depending on the input type, the return value can be either a casted
	 * version of {@code bson} or a converted (detached from the original value) using the given {@link CodecRegistry} to
	 * obtain {@link org.bson.codecs.Codec codecs} that might be required for conversion.
	 *
	 * @param bson can be {@literal null}.
	 * @param codecRegistry must not be {@literal null}.
	 * @return never {@literal null}. Returns an empty {@link Map} if input {@link Bson} is {@literal null}.
	 * @since 4.0
	 */
	public static Map<String, Object> asMap(@Nullable Bson bson, CodecRegistry codecRegistry) {

		if (bson == null) {
			return Collections.emptyMap();
		}

		if (bson instanceof Document document) {
			return document;
		}
		if (bson instanceof BasicDBObject dbo) {
			return dbo;
		}
		if (bson instanceof DBObject dbo) {
			return dbo.toMap();
		}

		return new Document(bson.toBsonDocument(Document.class, codecRegistry));
	}

	/**
	 * Return the {@link Bson} object as {@link Document}. Depending on the input type, the return value can be either a
	 * casted version of {@code bson} or a converted (detached from the original value).
	 *
	 * @param bson
	 * @return
	 * @since 3.2.5
	 */
	public static Document asDocument(Bson bson) {
		return asDocument(bson, MongoClientSettings.getDefaultCodecRegistry());
	}

	/**
	 * Return the {@link Bson} object as {@link Document}. Depending on the input type, the return value can be either a
	 * casted version of {@code bson} or a converted (detached from the original value) using the given
	 * {@link CodecRegistry} to obtain {@link org.bson.codecs.Codec codecs} that might be required for conversion.
	 *
	 * @param bson
	 * @param codecRegistry must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 4.0
	 */
	public static Document asDocument(Bson bson, CodecRegistry codecRegistry) {

		Map<String, Object> map = asMap(bson, codecRegistry);

		if (map instanceof Document document) {
			return document;
		}

		return new Document(map);
	}

	/**
	 * Return the {@link Bson} object as mutable {@link Document} containing all entries from {@link Bson}.
	 *
	 * @param bson
	 * @return a mutable {@link Document} containing all entries from {@link Bson}.
	 * @since 3.2.5
	 */
	public static Document asMutableDocument(Bson bson) {

		if (bson instanceof EmptyDocument) {
			bson = new Document(asDocument(bson));
		}

		if (bson instanceof Document document) {
			return document;
		}

		Map<String, Object> map = asMap(bson);

		if (map instanceof Document document) {
			return document;
		}

		return new Document(map);
	}

	public static void addToMap(Bson bson, String key, @Nullable Object value) {

		if (bson instanceof Document document) {

			document.put(key, value);
			return;
		}
		if (bson instanceof BSONObject bsonObject) {

			bsonObject.put(key, value);
			return;
		}

		throw new IllegalArgumentException(String.format(
				"Cannot add key/value pair to %s; as map given Bson must be a Document or BSONObject", bson.getClass()));
	}

	/**
	 * Add all entries from the given {@literal source} {@link Map} to the {@literal target}.
	 *
	 * @param target must not be {@literal null}.
	 * @param source must not be {@literal null}.
	 * @since 3.2
	 */
	public static void addAllToMap(Bson target, Map<String, ?> source) {

		if (target instanceof Document document) {

			document.putAll(source);
			return;
		}

		if (target instanceof BSONObject bsonObject) {

			bsonObject.putAll(source);
			return;
		}

		throw new IllegalArgumentException(
				String.format("Cannot add all to %s; Given Bson must be a Document or BSONObject.", target.getClass()));
	}

	/**
	 * Check if a given entry (key/value pair) is present in the given {@link Bson}.
	 *
	 * @param bson must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return {@literal true} if (key/value pair) is present.
	 * @since 3.2
	 */
	public static boolean contains(Bson bson, String key, @Nullable Object value) {

		if (bson instanceof Document document) {
			return document.containsKey(key) && ObjectUtils.nullSafeEquals(document.get(key), value);
		}
		if (bson instanceof BSONObject bsonObject) {
			return bsonObject.containsField(key) && ObjectUtils.nullSafeEquals(bsonObject.get(key), value);
		}

		Map<String, Object> map = asMap(bson);
		return map.containsKey(key) && ObjectUtils.nullSafeEquals(map.get(key), value);
	}

	/**
	 * Remove {@code _id : null} from the given {@link Bson} if present.
	 *
	 * @param bson must not be {@literal null}.
	 * @since 3.2
	 */
	public static boolean removeNullId(Bson bson) {

		if (!contains(bson, FieldName.ID.name(), null)) {
			return false;
		}

		removeFrom(bson, FieldName.ID.name());
		return true;
	}

	/**
	 * Remove the given {@literal key} from the {@link Bson} value.
	 *
	 * @param bson must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @since 3.2
	 */
	static void removeFrom(Bson bson, String key) {

		if (bson instanceof Document document) {

			document.remove(key);
			return;
		}

		if (bson instanceof BSONObject bsonObject) {

			bsonObject.removeField(key);
			return;
		}

		throw new IllegalArgumentException(
				String.format("Cannot remove from %s given Bson must be a Document or BSONObject.", bson.getClass()));
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

		return switch (value.getBsonType()) {
			case INT32 -> value.asInt32().getValue();
			case INT64 -> value.asInt64().getValue();
			case STRING -> value.asString().getValue();
			case DECIMAL128 -> value.asDecimal128().doubleValue();
			case DOUBLE -> value.asDouble().getValue();
			case BOOLEAN -> value.asBoolean().getValue();
			case OBJECT_ID -> value.asObjectId().getValue();
			case DB_POINTER -> new DBRef(value.asDBPointer().getNamespace(), value.asDBPointer().getId());
			case BINARY -> value.asBinary().getData();
			case DATE_TIME -> new Date(value.asDateTime().getValue());
			case SYMBOL -> value.asSymbol().getSymbol();
			case ARRAY -> value.asArray().toArray();
			case DOCUMENT -> Document.parse(value.asDocument().toJson());
			default -> value;
		};
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
		return simpleToBsonValue(source, MongoClientSettings.getDefaultCodecRegistry());
	}

	/**
	 * Convert a given simple value (eg. {@link String}, {@link Long}) to its corresponding {@link BsonValue}.
	 *
	 * @param source must not be {@literal null}.
	 * @param codecRegistry The {@link CodecRegistry} used as a fallback to convert types using native {@link Codec}. Must
	 *          not be {@literal null}.
	 * @return the corresponding {@link BsonValue} representation.
	 * @throws IllegalArgumentException if {@literal source} does not correspond to a {@link BsonValue} type.
	 * @since 4.2
	 */
	@SuppressWarnings("unchecked")
	public static BsonValue simpleToBsonValue(Object source, CodecRegistry codecRegistry) {

		if (source instanceof BsonValue bsonValue) {
			return bsonValue;
		}

		if (source instanceof ObjectId objectId) {
			return new BsonObjectId(objectId);
		}

		if (source instanceof String stringValue) {
			return new BsonString(stringValue);
		}

		if (source instanceof Double doubleValue) {
			return new BsonDouble(doubleValue);
		}

		if (source instanceof Integer integerValue) {
			return new BsonInt32(integerValue);
		}

		if (source instanceof Long longValue) {
			return new BsonInt64(longValue);
		}

		if (source instanceof byte[] byteArray) {
			return new BsonBinary(byteArray);
		}

		if (source instanceof Boolean booleanValue) {
			return new BsonBoolean(booleanValue);
		}

		if (source instanceof Float floatValue) {
			return new BsonDouble(floatValue);
		}

		if (source instanceof Binary binary) {
			return new BsonBinary(binary.getType(), binary.getData());
		}

		if (source instanceof Date date) {
			new BsonDateTime(date.getTime());
		}

		try {

			Object value = source;
			if (ClassUtils.isPrimitiveArray(source.getClass())) {
				value = CollectionUtils.arrayToList(source);
			}

			Codec codec = codecRegistry.get(value.getClass());
			BsonCapturingWriter writer = new BsonCapturingWriter(value.getClass());
			codec.encode(writer, value,
					ObjectUtils.isArray(value) || value instanceof Collection<?> ? EncoderContext.builder().build() : null);
			return writer.getCapturedValue();
		} catch (CodecConfigurationException e) {
			throw new IllegalArgumentException(
					String.format("Unable to convert %s to BsonValue.", source != null ? source.getClass().getName() : "null"));
		}
	}

	/**
	 * Merge the given {@link Document documents} into on in the given order. Keys contained within multiple documents are
	 * overwritten by their follow-ups.
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

		if (source.stripLeading().startsWith("{")) {
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

		if (!StringUtils.hasText(value)) {
			return false;
		}

		String potentialJson = value.trim();
		return potentialJson.startsWith("{") && potentialJson.endsWith("}");
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

		Assert.notNull(json, "Json must not be null");

		if (codecRegistryProvider == null) {
			return Document.parse(json);
		}

		return Document.parse(json, codecRegistryProvider.getCodecFor(Document.class)
				.orElseGet(() -> new DocumentCodec(codecRegistryProvider.getCodecRegistry())));
	}

	/**
	 * Resolve the value for a given key. If the given {@link Bson} value contains the key the value is immediately
	 * returned. If not and the key contains a path using the dot ({@code .}) notation it will try to resolve the path by
	 * inspecting the individual parts. If one of the intermediate ones is {@literal null} or cannot be inspected further
	 * (wrong) type, {@literal null} is returned.
	 *
	 * @param bson the source to inspect. Must not be {@literal null}.
	 * @param key the key to lookup. Must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 3.0.8
	 */
	@Nullable
	public static Object resolveValue(Bson bson, String key) {
		return resolveValue(asMap(bson), key);
	}

	/**
	 * Resolve the value for a given {@link FieldName field name}. If the given name is a {@link Type#KEY} the value is
	 * obtained from the target {@link Bson} immediately. If the given fieldName is a {@link Type#PATH} maybe using the
	 * dot ({@code .}) notation it will try to resolve the path by inspecting the individual parts. If one of the
	 * intermediate ones is {@literal null} or cannot be inspected further (wrong) type, {@literal null} is returned.
	 *
	 * @param bson the source to inspect. Must not be {@literal null}.
	 * @param fieldName the name to lookup. Must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 4.2
	 */
	public static Object resolveValue(Bson bson, FieldName fieldName) {
		return resolveValue(asMap(bson), fieldName);
	}

	/**
	 * Resolve the value for a given {@link FieldName field name}. If the given name is a {@link Type#KEY} the value is
	 * obtained from the target {@link Bson} immediately. If the given fieldName is a {@link Type#PATH} maybe using the
	 * dot ({@code .}) notation it will try to resolve the path by inspecting the individual parts. If one of the
	 * intermediate ones is {@literal null} or cannot be inspected further (wrong) type, {@literal null} is returned.
	 *
	 * @param source the source to inspect. Must not be {@literal null}.
	 * @param fieldName the key to lookup. Must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 4.2
	 */
	@Nullable
	public static Object resolveValue(Map<String, Object> source, FieldName fieldName) {

		if (fieldName.isKey()) {
			return source.get(fieldName.name());
		}

		String[] parts = fieldName.parts();

		for (int i = 1; i < parts.length; i++) {

			Object result = source.get(parts[i - 1]);

			if (!(result instanceof Bson resultBson)) {
				return null;
			}

			source = asMap(resultBson);
		}

		return source.get(parts[parts.length - 1]);
	}

	/**
	 * Resolve the value for a given key. If the given {@link Map} value contains the key the value is immediately
	 * returned. If not and the key contains a path using the dot ({@code .}) notation it will try to resolve the path by
	 * inspecting the individual parts. If one of the intermediate ones is {@literal null} or cannot be inspected further
	 * (wrong) type, {@literal null} is returned.
	 *
	 * @param source the source to inspect. Must not be {@literal null}.
	 * @param key the key to lookup. Must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 4.1
	 */
	@Nullable
	public static Object resolveValue(Map<String, Object> source, String key) {

		if (source.containsKey(key)) {
			return source.get(key);
		}

		return resolveValue(source, FieldName.path(key));
	}

	public static boolean hasValue(Bson bson, FieldName fieldName) {

		Map<String, Object> source = asMap(bson);
		if (fieldName.isKey()) {
			return source.containsKey(fieldName.name());
		}

		String[] parts = fieldName.parts();
		Object result;

		for (int i = 1; i < parts.length; i++) {

			result = source.get(parts[i - 1]);
			source = getAsMap(result);

			if (source == null) {
				return false;
			}
		}

		return source.containsKey(parts[parts.length - 1]);

	}

	/**
	 * Returns whether the underlying {@link Bson bson} has a value ({@literal null} or non-{@literal null}) for the given
	 * {@code key}.
	 *
	 * @param bson the source to inspect. Must not be {@literal null}.
	 * @param key the key to lookup. Must not be {@literal null}.
	 * @return {@literal true} if no non {@literal null} value present.
	 * @since 3.0.8
	 */
	public static boolean hasValue(Bson bson, String key) {
		return hasValue(bson, FieldName.path(key));
	}

	/**
	 * Returns the given source object as map, i.e. {@link Document}s and maps as is or {@literal null} otherwise.
	 *
	 * @param source can be {@literal null}.
	 * @return can be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	private static Map<String, Object> getAsMap(Object source) {

		if (source instanceof Document document) {
			return document;
		}

		if (source instanceof BasicDBObject basicDBObject) {
			return basicDBObject;
		}

		if (source instanceof DBObject dbObject) {
			return dbObject.toMap();
		}

		if (source instanceof Map) {
			return (Map<String, Object>) source;
		}

		return null;
	}

	/**
	 * Returns the given source object as {@link Bson}, i.e. {@link Document}s and maps as is or throw
	 * {@link IllegalArgumentException}.
	 *
	 * @param source
	 * @return the converted/casted source object.
	 * @throws IllegalArgumentException if {@code source} cannot be converted/cast to {@link Bson}.
	 * @since 3.2.3
	 * @see #supportsBson(Object)
	 */
	@SuppressWarnings("unchecked")
	public static Bson asBson(Object source) {

		if (source instanceof Document document) {
			return document;
		}

		if (source instanceof BasicDBObject basicDBObject) {
			return basicDBObject;
		}

		if (source instanceof DBObject dbObject) {
			return new Document(dbObject.toMap());
		}

		if (source instanceof Map) {
			return new Document((Map<String, Object>) source);
		}

		throw new IllegalArgumentException(String.format("Cannot convert %s to Bson", source));
	}

	/**
	 * Returns the given source can be used/converted as {@link Bson}.
	 *
	 * @param source
	 * @return {@literal true} if the given source can be converted to {@link Bson}.
	 * @since 3.2.3
	 */
	public static boolean supportsBson(Object source) {
		return source instanceof DBObject || source instanceof Map;
	}

	/**
	 * Returns given object as {@link Collection}. Will return the {@link Collection} as is if the source is a
	 * {@link Collection} already, will convert an array into a {@link Collection} or simply create a single element
	 * collection for everything else.
	 *
	 * @param source must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 3.2
	 */
	public static Collection<?> asCollection(Object source) {

		if (source instanceof Collection<?> collection) {
			return collection;
		}

		return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
	}

	@Nullable
	private static String toJson(@Nullable Object value) {

		if (value == null) {
			return null;
		}

		try {
			return value instanceof Document document
					? document.toJson(MongoClientSettings.getDefaultCodecRegistry().get(Document.class))
					: serializeValue(value);

		} catch (Exception e) {

			if (value instanceof Collection<?> collection) {
				return toString(collection);
			} else if (value instanceof Map<?, ?> map) {
				return toString(map);
			} else if (ObjectUtils.isArray(value)) {
				return toString(Arrays.asList(ObjectUtils.toObjectArray(value)));
			}

			throw e instanceof JsonParseException jsonParseException ? jsonParseException : new JsonParseException(e);
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

		// Avoid String.format for performance
		return iterableToDelimitedString(source.entrySet(), "{ ", " }",
				entry -> "\"" + entry.getKey() + "\" : " + toJson(entry.getValue()));
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

	static class BsonCapturingWriter extends AbstractBsonWriter {

		private final List<BsonValue> values = new ArrayList<>(0);

		public BsonCapturingWriter(Class<?> type) {
			super(new BsonWriterSettings());

			if (ClassUtils.isAssignable(Map.class, type)) {
				setContext(new Context(null, BsonContextType.DOCUMENT));
			} else if (ClassUtils.isAssignable(List.class, type) || type.isArray()) {
				setContext(new Context(null, BsonContextType.ARRAY));
			} else {
				setContext(new Context(null, BsonContextType.DOCUMENT));
			}
		}

		@Nullable
		BsonValue getCapturedValue() {

			if (values.isEmpty()) {
				return null;
			}
			if (!getContext().getContextType().equals(BsonContextType.ARRAY)) {
				return values.get(0);
			}

			return new BsonArray(values);
		}

		@Override
		protected void doWriteStartDocument() {

		}

		@Override
		protected void doWriteEndDocument() {

		}

		@Override
		public void writeStartArray() {
			setState(State.VALUE);
		}

		@Override
		public void writeEndArray() {
			setState(State.NAME);
		}

		@Override
		protected void doWriteStartArray() {

		}

		@Override
		protected void doWriteEndArray() {

		}

		@Override
		protected void doWriteBinaryData(BsonBinary value) {
			values.add(value);
		}

		@Override
		protected void doWriteBoolean(boolean value) {
			values.add(BsonBoolean.valueOf(value));
		}

		@Override
		protected void doWriteDateTime(long value) {
			values.add(new BsonDateTime(value));
		}

		@Override
		protected void doWriteDBPointer(BsonDbPointer value) {
			values.add(value);
		}

		@Override
		protected void doWriteDouble(double value) {
			values.add(new BsonDouble(value));
		}

		@Override
		protected void doWriteInt32(int value) {
			values.add(new BsonInt32(value));
		}

		@Override
		protected void doWriteInt64(long value) {
			values.add(new BsonInt64(value));
		}

		@Override
		protected void doWriteDecimal128(Decimal128 value) {
			values.add(new BsonDecimal128(value));
		}

		@Override
		protected void doWriteJavaScript(String value) {
			values.add(new BsonJavaScript(value));
		}

		@Override
		protected void doWriteJavaScriptWithScope(String value) {
			throw new UnsupportedOperationException("Cannot capture JavaScriptWith");
		}

		@Override
		protected void doWriteMaxKey() {}

		@Override
		protected void doWriteMinKey() {}

		@Override
		protected void doWriteNull() {
			values.add(new BsonNull());
		}

		@Override
		protected void doWriteObjectId(ObjectId value) {
			values.add(new BsonObjectId(value));
		}

		@Override
		protected void doWriteRegularExpression(BsonRegularExpression value) {
			values.add(value);
		}

		@Override
		protected void doWriteString(String value) {
			values.add(new BsonString(value));
		}

		@Override
		protected void doWriteSymbol(String value) {
			values.add(new BsonSymbol(value));
		}

		@Override
		protected void doWriteTimestamp(BsonTimestamp value) {
			values.add(value);
		}

		@Override
		protected void doWriteUndefined() {
			values.add(new BsonUndefined());
		}

		@Override
		public void flush() {
			values.clear();
		}
	}
}
