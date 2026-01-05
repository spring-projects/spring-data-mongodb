/*
 * Copyright 2024-present the original author or authors.
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
package org.springframework.data.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.jspecify.annotations.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * @author Christoph Strobl
 */
public class CapturingTransactionOptionsResolver implements MongoTransactionOptionsResolver {

	private final MongoTransactionOptionsResolver delegateResolver;
	private final List<MongoTransactionOptions> capturedOptions = new ArrayList<>(10);

	public CapturingTransactionOptionsResolver(MongoTransactionOptionsResolver delegateResolver) {
		this.delegateResolver = delegateResolver;
	}

	@Override
	public @Nullable String getLabelPrefix() {
		return delegateResolver.getLabelPrefix();
	}

	@Override
	public MongoTransactionOptions convert(Map<String, String> source) {

		MongoTransactionOptions options = delegateResolver.convert(source);
		capturedOptions.add(options);
		return options;
	}

	public void clear() {
		capturedOptions.clear();
	}

	public List<MongoTransactionOptions> getCapturedOptions() {
		return capturedOptions;
	}

	public MongoTransactionOptions getLastCapturedOption() {
		return CollectionUtils.lastElement(capturedOptions);
	}
}
