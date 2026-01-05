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

import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A {@link TransactionOptionResolver} reading MongoDB specific {@link MongoTransactionOptions transaction options} from
 * a {@link TransactionDefinition}. Implementations of {@link MongoTransactionOptions} may choose a specific
 * {@link #getLabelPrefix() prefix} for {@link TransactionAttribute#getLabels() transaction attribute labels} to avoid
 * evaluating non-store specific ones.
 * <p>
 * {@link TransactionAttribute#getLabels()} evaluated by default should follow the property style using {@code =} to
 * separate key and value pairs.
 * <p>
 * By default {@link #resolve(TransactionDefinition)} will filter labels by the {@link #getLabelPrefix() prefix} and
 * strip the prefix from the label before handing the pruned {@link Map} to the {@link #convert(Map)} function.
 * <p>
 * A transaction definition with labels targeting MongoDB may look like the following:
 * <p>
 * <code class="java">
 * &#64;Transactional(label = { "mongo:readConcern=majority" })
 * </code>
 *
 * @author Christoph Strobl
 * @since 4.3
 */
public interface MongoTransactionOptionsResolver extends TransactionOptionResolver<MongoTransactionOptions> {

	/**
	 * Obtain the default {@link MongoTransactionOptionsResolver} implementation using a {@literal mongo:}
	 * {@link #getLabelPrefix() prefix}.
	 *
	 * @return instance of default {@link MongoTransactionOptionsResolver} implementation.
	 */
	static MongoTransactionOptionsResolver defaultResolver() {
		return DefaultMongoTransactionOptionsResolver.INSTANCE;
	}

	/**
	 * Get the prefix used to filter applicable {@link TransactionAttribute#getLabels() labels}.
	 *
	 * @return {@literal null} if no label defined.
	 */
	@Nullable
	String getLabelPrefix();

	/**
	 * Resolve {@link MongoTransactionOptions} from a given {@link TransactionDefinition} by evaluating
	 * {@link TransactionAttribute#getLabels()} labels if possible.
	 * <p>
	 * Splits applicable labels property style using {@literal =} as deliminator and removes a potential
	 * {@link #getLabelPrefix() prefix} before calling {@link #convert(Map)} with filtered label values.
	 *
	 * @param definition
	 * @return {@link MongoTransactionOptions#NONE} in case the given {@link TransactionDefinition} is not a
	 *         {@link TransactionAttribute} if no matching {@link TransactionAttribute#getLabels() labels} could be found.
	 * @throws IllegalArgumentException for options that do not map to valid transactions options or malformatted labels.
	 */
	@Override
	default MongoTransactionOptions resolve(TransactionDefinition definition) {

		if (!(definition instanceof TransactionAttribute attribute)) {
			return MongoTransactionOptions.NONE;
		}

		if (attribute.getLabels().isEmpty()) {
			return MongoTransactionOptions.NONE;
		}

		Map<String, String> attributeMap = attribute.getLabels().stream()
				.filter(it -> !StringUtils.hasText(getLabelPrefix()) || it.startsWith(getLabelPrefix()))
				.map(it -> StringUtils.hasText(getLabelPrefix()) ? it.substring(getLabelPrefix().length()) : it).map(it -> {

					String[] kvPair = StringUtils.split(it, "=");
					Assert.isTrue(kvPair != null && kvPair.length == 2,
							() -> "No value present for transaction option %s".formatted(kvPair != null ? kvPair[0] : it));
					return kvPair;
				})

				.collect(Collectors.toMap(it -> it[0].trim(), it -> it[1].trim()));

		return attributeMap.isEmpty() ? MongoTransactionOptions.NONE : convert(attributeMap);
	}

	/**
	 * Convert the given {@link Map} into an instance of {@link MongoTransactionOptions}.
	 *
	 * @param options never {@literal null}.
	 * @return never {@literal null}.
	 * @throws IllegalArgumentException for invalid options.
	 */
	MongoTransactionOptions convert(Map<String, String> options);
}
