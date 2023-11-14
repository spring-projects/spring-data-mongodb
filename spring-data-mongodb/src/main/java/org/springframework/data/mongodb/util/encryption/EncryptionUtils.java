/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.util.encryption;

import java.util.Base64;
import java.util.UUID;
import java.util.function.Supplier;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.types.Binary;
import org.springframework.data.mongodb.util.spel.ExpressionUtils;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Internal utility class for dealing with encryption related matters.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
public final class EncryptionUtils {

	/**
	 * Resolve a given plain {@link String} value into the store native {@literal keyId} format, considering potential
	 * {@link Expression expressions}. <br />
	 * The potential keyId is probed against an {@link UUID#fromString(String) UUID value} or decoded from the
	 * {@literal base64} representation prior to conversion into its {@link Binary} format.
	 *
	 * @param value the source value to resolve the keyId for. Must not be {@literal null}.
	 * @param evaluationContext a {@link Supplier} used to provide the {@link EvaluationContext} in case an
	 *          {@link Expression} is {@link ExpressionUtils#detectExpression(String) detected}.
	 * @return can be {@literal null}.
	 * @throws IllegalArgumentException if one of the required arguments is {@literal null}.
	 */
	@Nullable
	public static Object resolveKeyId(String value, Supplier<EvaluationContext> evaluationContext) {

		Assert.notNull(value, "Value must not be null");

		Object potentialKeyId = value;
		Expression expression = ExpressionUtils.detectExpression(value);
		if (expression != null) {
			potentialKeyId = expression.getValue(evaluationContext.get());
			if (!(potentialKeyId instanceof String)) {
				return potentialKeyId;
			}
		}

		try {
			return new Binary(BsonBinarySubType.UUID_STANDARD,
					new BsonBinary(UUID.fromString(potentialKeyId.toString())).getData());
		} catch (IllegalArgumentException e) {

			return new Binary(BsonBinarySubType.UUID_STANDARD, Base64.getDecoder().decode(potentialKeyId.toString()));
		}
	}
}
