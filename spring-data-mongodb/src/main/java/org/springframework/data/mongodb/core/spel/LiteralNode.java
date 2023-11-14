/*
 * Copyright 2013-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.spel;

import java.util.Set;

import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.ast.BooleanLiteral;
import org.springframework.expression.spel.ast.FloatLiteral;
import org.springframework.expression.spel.ast.IntLiteral;
import org.springframework.expression.spel.ast.Literal;
import org.springframework.expression.spel.ast.LongLiteral;
import org.springframework.expression.spel.ast.NullLiteral;
import org.springframework.expression.spel.ast.RealLiteral;
import org.springframework.expression.spel.ast.StringLiteral;
import org.springframework.lang.Nullable;

/**
 * A node representing a literal in an expression.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LiteralNode extends ExpressionNode {

	private static final Set<Class<?>> SUPPORTED_LITERAL_TYPES = Set.of(BooleanLiteral.class, FloatLiteral.class,
			IntLiteral.class, LongLiteral.class, NullLiteral.class, RealLiteral.class, StringLiteral.class);
	private final Literal literal;

	/**
	 * Creates a new {@link LiteralNode} from the given {@link Literal} and {@link ExpressionState}.
	 *
	 * @param node must not be {@literal null}.
	 * @param state must not be {@literal null}.
	 */
	LiteralNode(Literal node, ExpressionState state) {
		super(node, state);
		this.literal = node;
	}

	/**
	 * Returns whether the given {@link ExpressionNode} is a unary minus.
	 *
	 * @param parent
	 * @return
	 */
	public boolean isUnaryMinus(@Nullable ExpressionNode parent) {

		if (!(parent instanceof OperatorNode operatorNode)) {
			return false;
		}

		return operatorNode.isUnaryMinus();
	}

	@Override
	public boolean isLiteral() {
		return SUPPORTED_LITERAL_TYPES.contains(literal.getClass());
	}
}
