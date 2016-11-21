/*
 * Copyright 2016. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.spel;

import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.OperatorNot;

/**
 * @author Christoph Strobl
 * @since 1.10
 */
public class NotOperatorNode extends ExpressionNode {

	private final OperatorNot operatorNode;

	/**
	 * Creates a new {@link ExpressionNode} from the given {@link OperatorNot} and {@link ExpressionState}.
	 *
	 * @param node must not be {@literal null}.
	 * @param state must not be {@literal null}.
	 */
	protected NotOperatorNode(OperatorNot node, ExpressionState state) {

		super(node, state);
		this.operatorNode = node;
	}

	public String getMongoOperator() {
		return "$not";
	}
}
