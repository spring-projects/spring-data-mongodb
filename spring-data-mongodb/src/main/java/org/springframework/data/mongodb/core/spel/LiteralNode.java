/*
 * Copyright 2013-2016 the original author or authors.
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

import java.util.Collections;
import java.util.HashSet;
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

/**
 * A node representing a literal in an expression.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class LiteralNode extends ExpressionNode {

	private static final Set<Class<?>> SUPPORTED_LITERAL_TYPES;
	private final Literal literal;

	static {

		Set<Class<?>> supportedTypes = new HashSet<Class<?>>(7, 1);
		supportedTypes.add(BooleanLiteral.class);
		supportedTypes.add(FloatLiteral.class);
		supportedTypes.add(IntLiteral.class);
		supportedTypes.add(LongLiteral.class);
		supportedTypes.add(NullLiteral.class);
		supportedTypes.add(RealLiteral.class);
		supportedTypes.add(StringLiteral.class);

		SUPPORTED_LITERAL_TYPES = Collections.unmodifiableSet(supportedTypes);
	}

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
	public boolean isUnaryMinus(ExpressionNode parent) {

		if (!(parent instanceof OperatorNode)) {
			return false;
		}

		OperatorNode operator = (OperatorNode) parent;
		return operator.isUnaryMinus() && operator.getRight() == null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.spel.ExpressionNode#isLiteral()
	 */
	@Override
	public boolean isLiteral() {
		return SUPPORTED_LITERAL_TYPES.contains(literal.getClass());
	}
}
