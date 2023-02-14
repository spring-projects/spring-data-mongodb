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

import org.bson.BsonBinary;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 4.1
 */
public class ManualEncryptionContext implements EncryptionContext {

	private final MongoConversionContext conversionContext;
	private final MongoPersistentProperty persistentProperty;
	private final KeyIdProvider<BsonBinary> keyIdProvider;
	private final Lazy<Encrypted> encryption;

	public ManualEncryptionContext(MongoConversionContext conversionContext, KeyIdProvider<BsonBinary> keyIdProvider) {
		this.conversionContext = conversionContext;
		this.persistentProperty = conversionContext.getProperty();
		this.encryption = Lazy.of(() -> persistentProperty.findAnnotation(Encrypted.class));
		this.keyIdProvider = keyIdProvider;
	}

	@Override
	public MongoPersistentProperty getProperty() {
		return conversionContext.getProperty();
	}

	@Nullable
	@Override
	public Object lookupValue(String path) {
		return conversionContext.getValue(path);
	}

	@Override
	public MongoConversionContext getSourceContext() {
		return conversionContext;
	}
}
