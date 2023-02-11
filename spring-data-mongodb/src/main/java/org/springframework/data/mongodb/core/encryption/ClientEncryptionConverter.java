/*
 * Copyright 2023 the original author or authors.
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

import java.util.Collections;

import org.bson.BsonBinary;
import org.bson.BsonValue;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;

import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;

/**
 * @author Christoph Strobl
 */
public class ClientEncryptionConverter implements EncryptingConverter<Object, Object> {

	private ClientEncryption clientEncryption;
	private final KeyIdProvider<BsonBinary> keyIdProvider;

	public ClientEncryptionConverter(ClientEncryption clientEncryption, KeyIdProvider<BsonBinary> keyIdProvider) {

		this.clientEncryption = clientEncryption;
		if (keyIdProvider != null) {
			this.keyIdProvider = keyIdProvider;
		} else {
			Lazy<BsonBinary> dataKey = Lazy.of(() -> clientEncryption.createDataKey("local",
					new DataKeyOptions().keyAltNames(Collections.singletonList("mySuperSecretKey"))));
			this.keyIdProvider = (ctx) -> dataKey.get();
		}
	}

	@Nullable
	@Override
	public Object read(Object value, MongoConversionContext context) {

		ManualEncryptionContext encryptionContext = buildEncryptionContext(context);
		Object decrypted = encryptionContext.decrypt(value, clientEncryption);
		return decrypted instanceof BsonValue ? BsonUtils.toJavaType((BsonValue) decrypted) : decrypted;
	}

	@Nullable
	@Override
	public BsonBinary write(Object value, MongoConversionContext context) {

		ManualEncryptionContext encryptionContext = buildEncryptionContext(context);
		return encryptionContext.encrypt(value, clientEncryption);
	}

	@Override
	public Object decrypt(Object value, EncryptionContext context) {
		return null;
	}

	@Override
	public Object encrypt(Object value, EncryptionContext context) {
		return null;
	}

	public ManualEncryptionContext buildEncryptionContext(MongoConversionContext context) {
		return new ManualEncryptionContext(context, this.keyIdProvider);
	}
}
