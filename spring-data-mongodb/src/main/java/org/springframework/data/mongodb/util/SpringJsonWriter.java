/*
 * Copyright 2025 the original author or authors.
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

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;

import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.util.StringUtils;

/**
 * Internal {@link BsonWriter} implementation that allows to render {@link #writePlaceholder(String) placeholders} as
 * {@code ?0}.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
@NullUnmarked
class SpringJsonWriter implements BsonWriter {

	private final StringBuffer buffer;

	private enum JsonContextType {
		TOP_LEVEL, DOCUMENT, ARRAY,
	}

	private enum State {
		INITIAL, NAME, VALUE, DONE
	}

	private static class JsonContext {

		private final JsonContext parentContext;
		private final JsonContextType contextType;
		private boolean hasElements;

		JsonContext(final JsonContext parentContext, final JsonContextType contextType) {
			this.parentContext = parentContext;
			this.contextType = contextType;
		}

		JsonContext nestedDocument() {
			return new JsonContext(this, JsonContextType.DOCUMENT);
		}

		JsonContext nestedArray() {
			return new JsonContext(this, JsonContextType.ARRAY);
		}
	}

	private JsonContext context = new JsonContext(null, JsonContextType.TOP_LEVEL);
	private State state = State.INITIAL;

	public SpringJsonWriter(StringBuffer buffer) {
		this.buffer = buffer;
	}

	@Override
	public void flush() {}

	@Override
	public void writeBinaryData(BsonBinary binary) {

		preWriteValue();
		writeStartDocument();

		writeName("$binary");

		writeStartDocument();
		writeName("base64");
		writeString(Base64.getEncoder().encodeToString(binary.getData()));
		writeName("subType");
		writeInt32(binary.getBsonType().getValue());
		writeEndDocument();

		writeEndDocument();
	}

	@Override
	public void writeBinaryData(String name, BsonBinary binary) {

		writeName(name);
		writeBinaryData(binary);
	}

	@Override
	public void writeBoolean(boolean value) {

		preWriteValue();
		write(value ? "true" : "false");
		setNextState();
	}

	@Override
	public void writeBoolean(String name, boolean value) {

		writeName(name);
		writeBoolean(value);
	}

	@Override
	public void writeDateTime(long value) {

		// "$date": "2018-11-10T22:26:12.111Z"
		writeStartDocument();
		writeName("$date");
		writeString(ZonedDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.of("Z")).format(ISO_OFFSET_DATE_TIME));
		writeEndDocument();
	}

	@Override
	public void writeDateTime(String name, long value) {

		writeName(name);
		writeDateTime(value);
	}

	@Override
	public void writeDBPointer(BsonDbPointer value) {

	}

	@Override
	public void writeDBPointer(String name, BsonDbPointer value) {

	}

	@Override // {"$numberDouble":"10.5"}
	public void writeDouble(double value) {

		writeStartDocument();
		writeName("$numberDouble");
		writeString(Double.valueOf(value).toString());
		writeEndDocument();
	}

	@Override
	public void writeDouble(String name, double value) {

		writeName(name);
		writeDouble(value);
	}

	@Override
	public void writeEndArray() {
		write("]");
		context = context.parentContext;
		if (context.contextType == JsonContextType.TOP_LEVEL) {
			state = State.DONE;
		} else {
			setNextState();
		}
	}

	@Override
	public void writeEndDocument() {
		buffer.append("}");
		context = context.parentContext;
		if (context.contextType == JsonContextType.TOP_LEVEL) {
			state = State.DONE;
		} else {
			setNextState();
		}
	}

	@Override
	public void writeInt32(int value) {

		writeStartDocument();
		writeName("$numberInt");
		writeString(Integer.valueOf(value).toString());
		writeEndDocument();
	}

	@Override
	public void writeInt32(String name, int value) {

		writeName(name);
		writeInt32(value);
	}

	@Override
	public void writeInt64(long value) {

		writeStartDocument();
		writeName("$numberLong");
		writeString(Long.valueOf(value).toString());
		writeEndDocument();
	}

	@Override
	public void writeInt64(String name, long value) {

		writeName(name);
		writeInt64(value);
	}

	@Override
	public void writeDecimal128(Decimal128 value) {

		// { "$numberDecimal": "<number>" }
		writeStartDocument();
		writeName("$numberDecimal");
		writeString(value.toString());
		writeEndDocument();
	}

	@Override
	public void writeDecimal128(String name, Decimal128 value) {

		writeName(name);
		writeDecimal128(value);
	}

	@Override
	public void writeJavaScript(String code) {

		writeStartDocument();
		writeName("$code");
		writeString(code);
		writeEndDocument();
	}

	@Override
	public void writeJavaScript(String name, String code) {

		writeName(name);
		writeJavaScript(code);
	}

	@Override
	public void writeJavaScriptWithScope(String code) {

	}

	@Override
	public void writeJavaScriptWithScope(String name, String code) {

	}

	@Override
	public void writeMaxKey() {

		writeStartDocument();
		writeName("$maxKey");
		buffer.append(1);
		writeEndDocument();
	}

	@Override
	public void writeMaxKey(String name) {
		writeName(name);
		writeMaxKey();
	}

	@Override
	public void writeMinKey() {

		writeStartDocument();
		writeName("$minKey");
		buffer.append(1);
		writeEndDocument();
	}

	@Override
	public void writeMinKey(String name) {
		writeName(name);
		writeMinKey();
	}

	@Override
	public void writeName(String name) {
		if (context.hasElements) {
			write(",");
		} else {
			context.hasElements = true;
		}

		writeString(name);
		buffer.append(":");
		state = State.VALUE;
	}

	@Override
	public void writeNull() {
		buffer.append("null");
	}

	@Override
	public void writeNull(String name) {
		writeName(name);
		writeNull();
	}

	@Override
	public void writeObjectId(ObjectId objectId) {
		writeStartDocument();
		writeName("$oid");
		writeString(objectId.toHexString());
		writeEndDocument();
	}

	@Override
	public void writeObjectId(String name, ObjectId objectId) {
		writeName(name);
		writeObjectId(objectId);
	}

	@Override
	public void writeRegularExpression(BsonRegularExpression regularExpression) {

		writeStartDocument();
		writeName("$regex");

		write("/");
		write(regularExpression.getPattern());
		write("/");

		if (StringUtils.hasText(regularExpression.getOptions())) {
			writeName("$options");
			writeString(regularExpression.getOptions());
		}

		writeEndDocument();
	}

	@Override
	public void writeRegularExpression(String name, BsonRegularExpression regularExpression) {
		writeName(name);
		writeRegularExpression(regularExpression);
	}

	@Override
	public void writeStartArray() {

		preWriteValue();
		write("[");
		context = context.nestedArray();
	}

	@Override
	public void writeStartArray(String name) {
		writeName(name);
		writeStartArray();
	}

	@Override
	public void writeStartDocument() {

		preWriteValue();
		write("{");
		context = context.nestedDocument();
		state = State.NAME;
	}

	@Override
	public void writeStartDocument(String name) {
		writeName(name);
		writeStartDocument();
	}

	@Override
	public void writeString(String value) {
		write("'");
		write(value);
		write("'");
	}

	@Override
	public void writeString(String name, String value) {
		writeName(name);
		writeString(value);
	}

	@Override
	public void writeSymbol(String value) {

		writeStartDocument();
		writeName("$symbol");
		writeString(value);
		writeEndDocument();
	}

	@Override
	public void writeSymbol(String name, String value) {

		writeName(name);
		writeSymbol(value);
	}

	@Override // {"$timestamp": {"t": <t>, "i": <i>}}
	public void writeTimestamp(BsonTimestamp value) {

		preWriteValue();
		writeStartDocument();
		writeName("$timestamp");
		writeStartDocument();
		writeName("t");
		buffer.append(value.getTime());
		writeName("i");
		buffer.append(value.getInc());
		writeEndDocument();
		writeEndDocument();
	}

	@Override
	public void writeTimestamp(String name, BsonTimestamp value) {

		writeName(name);
		writeTimestamp(value);
	}

	@Override
	public void writeUndefined() {

		writeStartDocument();
		writeName("$undefined");
		writeBoolean(true);
		writeEndDocument();
	}

	@Override
	public void writeUndefined(String name) {

		writeName(name);
		writeUndefined();
	}

	@Override
	public void pipe(BsonReader reader) {

	}

	/**
	 * @param placeholder
	 */
	public void writePlaceholder(String placeholder) {
		write(placeholder);
	}

	private void write(String str) {
		buffer.append(str);
	}

	private void preWriteValue() {

		if (context.contextType == JsonContextType.ARRAY) {
			if (context.hasElements) {
				write(",");
			}
		}
		context.hasElements = true;
	}

	private void setNextState() {
		if (context.contextType == JsonContextType.ARRAY) {
			state = State.VALUE;
		} else {
			state = State.NAME;
		}
	}
}
