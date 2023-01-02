/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.util.List;

import org.bson.Document;
import org.springframework.util.ObjectUtils;

import com.mongodb.client.model.Collation;

/**
 * Value Object providing a methods for accessing collection/view information within a raw {@link Document}.
 * 
 * @author Christoph Strobl
 */
public class CollectionInfo {

	private final Document source;

	public static CollectionInfo from(Document source) {
		return new CollectionInfo(source);
	}

	CollectionInfo(Document source) {
		this.source = source;
	}

	/**
	 * @return the collection/view name.
	 */
	public String getName() {
		return source.getString("name");
	}

	/**
	 * @return {@literal true} if the {@literal type} equals {@literal view}.
	 */
	public boolean isView() {
		return ObjectUtils.nullSafeEquals("view", source.get("type"));
	}

	/**
	 * @return the {@literal options.viewOn} value.
	 * @throws IllegalStateException if not {@link #isView() a view}.
	 */
	public String getViewTarget() {

		if (isView()) {
			return getOptionValue("viewOn", String.class);
		}
		throw new IllegalStateException(getName() + " is not a view");
	}

	/**
	 * @return the {@literal options.pipeline} value.
	 * @throws IllegalStateException if not {@link #isView() a view}.
	 */
	public List<Document> getViewPipeline() {

		if (isView()) {
			return getOptions().getList("pipeline", Document.class);
		}

		throw new IllegalStateException(getName() + " is not a view");
	}

	/**
	 * @return the {@literal options.collation} value.
	 * @throws IllegalStateException if not {@link #isView() a view}.
	 */
	public Collation getCollation() {

		return org.springframework.data.mongodb.core.query.Collation.from(getOptionValue("collation", Document.class))
				.toMongoCollation();
	}

	private Document getOptions() {
		return source.get("options", Document.class);
	}

	private <T> T getOptionValue(String key, Class<T> type) {
		return getOptions().get(key, type);
	}
}
