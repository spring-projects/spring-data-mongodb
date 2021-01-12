/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.mongodb.util.json;

import static org.assertj.core.api.Assertions.*;

import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.util.BsonUtils;

/**
 * @author Christoph Strobl
 */
class BsonUtilsTest {

	@Test // DATAMONGO-625
	void simpleToBsonValue() {

		assertThat(BsonUtils.simpleToBsonValue(Long.valueOf(10))).isEqualTo(new BsonInt64(10));
		assertThat(BsonUtils.simpleToBsonValue(new Integer(10))).isEqualTo(new BsonInt32(10));
		assertThat(BsonUtils.simpleToBsonValue(Double.valueOf(0.1D))).isEqualTo(new BsonDouble(0.1D));
		assertThat(BsonUtils.simpleToBsonValue("value")).isEqualTo(new BsonString("value"));
	}

	@Test // DATAMONGO-625
	void primitiveToBsonValue() {
		assertThat(BsonUtils.simpleToBsonValue(10L)).isEqualTo(new BsonInt64(10));
	}

	@Test // DATAMONGO-625
	void objectIdToBsonValue() {

		ObjectId source = new ObjectId();
		assertThat(BsonUtils.simpleToBsonValue(source)).isEqualTo(new BsonObjectId(source));
	}

	@Test // DATAMONGO-625
	void bsonValueToBsonValue() {

		BsonObjectId source = new BsonObjectId(new ObjectId());
		assertThat(BsonUtils.simpleToBsonValue(source)).isSameAs(source);
	}

	@Test // DATAMONGO-625
	void unsupportedToBsonValue() {
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> BsonUtils.simpleToBsonValue(new Object()));
	}
}
