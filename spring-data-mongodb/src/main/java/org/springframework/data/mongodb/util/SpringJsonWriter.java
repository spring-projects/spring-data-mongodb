/*
 * Copyright 2025. the original author or authors.
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

/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.util;

import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
public class SpringJsonWriter implements BsonWriter {

    @Override
    public void flush() {

    }

    @Override
    public void writeBinaryData(BsonBinary binary) {

    }

    @Override
    public void writeBinaryData(String name, BsonBinary binary) {

    }

    @Override
    public void writeBoolean(boolean value) {

    }

    @Override
    public void writeBoolean(String name, boolean value) {

    }

    @Override
    public void writeDateTime(long value) {

    }

    @Override
    public void writeDateTime(String name, long value) {

    }

    @Override
    public void writeDBPointer(BsonDbPointer value) {

    }

    @Override
    public void writeDBPointer(String name, BsonDbPointer value) {

    }

    @Override
    public void writeDouble(double value) {

    }

    @Override
    public void writeDouble(String name, double value) {

    }

    @Override
    public void writeEndArray() {

    }

    @Override
    public void writeEndDocument() {

    }

    @Override
    public void writeInt32(int value) {

    }

    @Override
    public void writeInt32(String name, int value) {

    }

    @Override
    public void writeInt64(long value) {

    }

    @Override
    public void writeInt64(String name, long value) {

    }

    @Override
    public void writeDecimal128(Decimal128 value) {

    }

    @Override
    public void writeDecimal128(String name, Decimal128 value) {

    }

    @Override
    public void writeJavaScript(String code) {

    }

    @Override
    public void writeJavaScript(String name, String code) {

    }

    @Override
    public void writeJavaScriptWithScope(String code) {

    }

    @Override
    public void writeJavaScriptWithScope(String name, String code) {

    }

    @Override
    public void writeMaxKey() {

    }

    @Override
    public void writeMaxKey(String name) {

    }

    @Override
    public void writeMinKey() {

    }

    @Override
    public void writeMinKey(String name) {

    }

    @Override
    public void writeName(String name) {

    }

    @Override
    public void writeNull() {

    }

    @Override
    public void writeNull(String name) {

    }

    @Override
    public void writeObjectId(ObjectId objectId) {

    }

    @Override
    public void writeObjectId(String name, ObjectId objectId) {

    }

    @Override
    public void writeRegularExpression(BsonRegularExpression regularExpression) {

    }

    @Override
    public void writeRegularExpression(String name, BsonRegularExpression regularExpression) {

    }

    @Override
    public void writeStartArray() {

    }

    @Override
    public void writeStartArray(String name) {

    }

    @Override
    public void writeStartDocument() {

    }

    @Override
    public void writeStartDocument(String name) {

    }

    @Override
    public void writeString(String value) {

    }

    @Override
    public void writeString(String name, String value) {

    }

    @Override
    public void writeSymbol(String value) {

    }

    @Override
    public void writeSymbol(String name, String value) {

    }

    @Override
    public void writeTimestamp(BsonTimestamp value) {

    }

    @Override
    public void writeTimestamp(String name, BsonTimestamp value) {

    }

    @Override
    public void writeUndefined() {

    }

    @Override
    public void writeUndefined(String name) {

    }

    @Override
    public void pipe(BsonReader reader) {

    }

    public void writePlaceholder(String placeholder) {

    }
}
