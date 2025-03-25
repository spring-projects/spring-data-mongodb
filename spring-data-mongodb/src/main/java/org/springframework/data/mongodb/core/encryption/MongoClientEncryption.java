/*
 * Copyright 2023-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.encryption;

import java.util.Map;
import java.util.function.Supplier;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.springframework.data.mongodb.core.encryption.EncryptionKey.Type;
import org.springframework.data.mongodb.core.encryption.EncryptionOptions.QueryableEncryptionOptions;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.util.Assert;

import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RangeOptions;
import com.mongodb.client.vault.ClientEncryption;

/**
 * {@link ClientEncryption} based {@link Encryption} implementation.
 *
 * @author Christoph Strobl
 * @author Ross Lawley
 * @since 4.1
 */
public class MongoClientEncryption implements Encryption<BsonValue, BsonBinary> {

	private final Supplier<ClientEncryption> source;

	MongoClientEncryption(Supplier<ClientEncryption> source) {
		this.source = source;
	}

	/**
	 * Create a new {@link MongoClientEncryption} instance for the given {@link ClientEncryption}.
	 *
	 * @param clientEncryption must not be {@literal null}.
	 * @return new instance of {@link MongoClientEncryption}.
	 */
	public static MongoClientEncryption just(ClientEncryption clientEncryption) {

		Assert.notNull(clientEncryption, "ClientEncryption must not be null");

		return new MongoClientEncryption(() -> clientEncryption);
	}

	@Override
	public BsonValue decrypt(BsonBinary value) {
		return getClientEncryption().decrypt(value);
	}

	@Override
	public BsonBinary encrypt(BsonValue value, EncryptionOptions options) {
		return getClientEncryption().encrypt(value, createEncryptOptions(options));
	}

	@Override
	public BsonDocument encryptExpression(BsonDocument value, EncryptionOptions options) {
		return getClientEncryption().encryptExpression(value, createEncryptOptions(options));
	}

	public ClientEncryption getClientEncryption() {
		return source.get();
	}

	private EncryptOptions createEncryptOptions(EncryptionOptions options) {

		EncryptOptions encryptOptions = new EncryptOptions(options.algorithm());

		if (Type.ALT.equals(options.key().type())) {
			encryptOptions = encryptOptions.keyAltName(options.key().value().toString());
		} else {
			encryptOptions = encryptOptions.keyId((BsonBinary) options.key().value());
		}

		if (options.queryableEncryptionOptions().isEmpty()) {
			return encryptOptions;
		}

		QueryableEncryptionOptions qeOptions = options.queryableEncryptionOptions();
		if (qeOptions.getQueryType() != null) {
			encryptOptions.queryType(qeOptions.getQueryType());
		}
		if (qeOptions.getContentionFactor() != null) {
			encryptOptions.contentionFactor(qeOptions.getContentionFactor());
		}
		if (!qeOptions.getAttributes().isEmpty()) {
			encryptOptions.rangeOptions(rangeOptions(qeOptions.getAttributes()));
		}
		return encryptOptions;
	}

	protected RangeOptions rangeOptions(Map<String, Object> attributes) {

		RangeOptions encryptionRangeOptions = new RangeOptions();
		if (attributes.isEmpty()) {
			return encryptionRangeOptions;
		}

		if (attributes.containsKey("min")) {
			encryptionRangeOptions.min(BsonUtils.simpleToBsonValue(attributes.get("min")));
		}
		if (attributes.containsKey("max")) {
			encryptionRangeOptions.max(BsonUtils.simpleToBsonValue(attributes.get("max")));
		}
		if (attributes.containsKey("trimFactor")) {
			Object trimFactor = attributes.get("trimFactor");
			Assert.isInstanceOf(Integer.class, trimFactor, () -> String
					.format("Expected to find a %s but it turned out to be %s.", Integer.class, trimFactor.getClass()));

			encryptionRangeOptions.trimFactor((Integer) trimFactor);
		}

		if (attributes.containsKey("sparsity")) {
			Object sparsity = attributes.get("sparsity");
			Assert.isInstanceOf(Number.class, sparsity,
					() -> String.format("Expected to find a %s but it turned out to be %s.", Long.class, sparsity.getClass()));
			encryptionRangeOptions.sparsity(((Number) sparsity).longValue());
		}

		if (attributes.containsKey("precision")) {
			Object precision = attributes.get("precision");
			Assert.isInstanceOf(Number.class, precision, () -> String
					.format("Expected to find a %s but it turned out to be %s.", Integer.class, precision.getClass()));
			encryptionRangeOptions.precision(((Number) precision).intValue());
		}
		return encryptionRangeOptions;
	}

}
