/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Collections;
import java.util.Map;

import org.bson.BsonDocument;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.lang.Nullable;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;

/**
 * {@link FactoryBean} for creating {@link AutoEncryptionSettings} using the {@link AutoEncryptionSettings.Builder}.
 *
 * @author Christoph Strobl
 * @since 2.2
 */
public class MongoEncryptionSettingsFactoryBean implements FactoryBean<AutoEncryptionSettings> {

	private boolean bypassAutoEncryption;
	private String keyVaultNamespace;
	private Map<String, Object> extraOptions;
	private MongoClientSettings keyVaultClientSettings;
	private Map<String, Map<String, Object>> kmsProviders;
	private Map<String, BsonDocument> schemaMap;

	/**
	 * @param bypassAutoEncryption
	 * @see AutoEncryptionSettings.Builder#bypassAutoEncryption(boolean)
	 */
	public void setBypassAutoEncryption(boolean bypassAutoEncryption) {
		this.bypassAutoEncryption = bypassAutoEncryption;
	}

	/**
	 * @param extraOptions
	 * @see AutoEncryptionSettings.Builder#extraOptions(Map)
	 */
	public void setExtraOptions(Map<String, Object> extraOptions) {
		this.extraOptions = extraOptions;
	}

	/**
	 * @param keyVaultNamespace
	 * @see AutoEncryptionSettings.Builder#keyVaultNamespace(String)
	 */
	public void setKeyVaultNamespace(String keyVaultNamespace) {
		this.keyVaultNamespace = keyVaultNamespace;
	}

	/**
	 * @param keyVaultClientSettings
	 * @see AutoEncryptionSettings.Builder#keyVaultMongoClientSettings(MongoClientSettings)
	 */
	public void setKeyVaultClientSettings(MongoClientSettings keyVaultClientSettings) {
		this.keyVaultClientSettings = keyVaultClientSettings;
	}

	/**
	 * @param kmsProviders
	 * @see AutoEncryptionSettings.Builder#kmsProviders(Map)
	 */
	public void setKmsProviders(Map<String, Map<String, Object>> kmsProviders) {
		this.kmsProviders = kmsProviders;
	}

	/**
	 * @param schemaMap
	 * @see AutoEncryptionSettings.Builder#schemaMap(Map)
	 */
	public void setSchemaMap(Map<String, BsonDocument> schemaMap) {
		this.schemaMap = schemaMap;
	}

	@Override
	public AutoEncryptionSettings getObject() {

		return AutoEncryptionSettings.builder() //
				.bypassAutoEncryption(bypassAutoEncryption) //
				.keyVaultNamespace(keyVaultNamespace) //
				.keyVaultMongoClientSettings(keyVaultClientSettings) //
				.kmsProviders(orEmpty(kmsProviders)) //
				.extraOptions(orEmpty(extraOptions)) //
				.schemaMap(orEmpty(schemaMap)) //
				.build();
	}

	private <K, V> Map<K, V> orEmpty(@Nullable Map<K, V> source) {
		return source != null ? source : Collections.emptyMap();
	}

	@Override
	public Class<?> getObjectType() {
		return AutoEncryptionSettings.class;
	}
}
