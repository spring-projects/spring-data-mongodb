/*
 * Copyright 2013-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AggregationExpressionTransformer.AggregationExpressionTransformationContext;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.spel.ExpressionNode;
import org.springframework.data.mongodb.core.spel.ExpressionTransformationContextSupport;
import org.springframework.data.mongodb.core.spel.ExpressionTransformer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Interface to type an {@link ExpressionTransformer} to the contained
 * {@link AggregationExpressionTransformationContext}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.4
 */
interface AggregationExpressionTransformer
		extends ExpressionTransformer<AggregationExpressionTransformationContext<ExpressionNode>> {

	/**
	 * A special {@link ExpressionTransformationContextSupport} to be aware of the {@link AggregationOperationContext}.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	class AggregationExpressionTransformationContext<T extends ExpressionNode>
			extends ExpressionTransformationContextSupport<T> {

		private final AggregationOperationContext aggregationContext;

		/**
		 * Creates an {@link AggregationExpressionTransformationContext}.
		 *
		 * @param currentNode must not be {@literal null}.
		 * @param parentNode may be {@literal null}.
		 * @param previousOperationObject may be {@literal null}.
		 * @param context must not be {@literal null}.
		 */
		public AggregationExpressionTransformationContext(T currentNode, @Nullable ExpressionNode parentNode,
				@Nullable Document previousOperationObject, AggregationOperationContext context) {

			super(currentNode, parentNode, previousOperationObject);

			Assert.notNull(context, "AggregationOperationContext must not be null");
			this.aggregationContext = context;
		}

		/**
		 * Returns the underlying {@link AggregationOperationContext}.
		 *
		 * @return never {@literal null}.
		 */
		public AggregationOperationContext getAggregationContext() {
			return aggregationContext;
		}

		/**
		 * Returns the {@link FieldReference} for the current {@link ExpressionNode}.
		 *
		 * @return never {@literal null}.
		 */
		public FieldReference getFieldReference() {
			return aggregationContext.getReference(getCurrentNode().getName());
		}
	}
}
