/*
 * Copyright 2014-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * {@link CompoundWildcardIndexDefinition} is a specific {@link Index} that includes one {@link WildcardIndex} and
 * one or more non-wildcard fields.
 *
 * @author Julia Lee
 * @since 4.4
 */
public class CompoundWildcardIndexDefinition extends WildcardIndex {

	private final Document indexKeys;

	/**
	 * Creates a new {@link CompoundWildcardIndexDefinition} for the given {@literal wildcardPath} and {@literal keys}.
	 * If {@literal wildcardPath} is empty, the wildcard index will apply to the root entity, using {@code $**}.
	 * <br />
	 *
	 * @param wildcardPath can be a {@literal empty} {@link String}.
	 */
	public CompoundWildcardIndexDefinition(String wildcardPath, Document indexKeys) {

		super(wildcardPath);
		this.indexKeys = indexKeys;
	}

	@Override
	public Document getIndexKeys() {

		Document document = new Document();
		document.putAll(indexKeys);
		document.putAll(super.getIndexKeys());
		return document;
	}

	@Override
	public Document getIndexOptions() {

		Document options = super.getIndexOptions();
		return options;
	}
}
