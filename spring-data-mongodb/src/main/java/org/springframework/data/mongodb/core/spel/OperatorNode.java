/*
 * Copyright 2013 the original author or authors.
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
import java.util.HashMap;
import java.util.Map;

import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.ast.OpDivide;
import org.springframework.expression.spel.ast.OpMinus;
import org.springframework.expression.spel.ast.OpModulus;
import org.springframework.expression.spel.ast.OpMultiply;
import org.springframework.expression.spel.ast.OpPlus;
import org.springframework.expression.spel.ast.Operator;

/**
 * An {@link ExpressionNode} representing an operator.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class OperatorNode extends ExpressionNode {

	private static final Map<String, String> OPERATORS;

	static {

		Map<String, String> map = new HashMap<String, String>(6);

		map.put("+", "$add");
		map.put("-", "$subtract");
		map.put("*", "$multiply");
		map.put("/", "$divide");
		map.put("%", "$mod");

		map.put("and", "and");
		map.put("or", "or");
		map.put("!", "not");

		OPERATORS = Collections.unmodifiableMap(map);
	}

	private final Operator operator;

	/**
	 * Creates a new {@link OperatorNode} from the given {@link Operator} and {@link ExpressionState}.
	 *
	 * @param node must not be {@literal null}.
	 * @param state must not be {@literal null}.
	 */
	OperatorNode(Operator node, ExpressionState state) {
		super(node, state);
		this.operator = node;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.spel.ExpressionNode#isMathematicalOperation()
	 */
	@Override
	public boolean isMathematicalOperation() {
		return operator instanceof OpMinus || operator instanceof OpPlus || operator instanceof OpMultiply
				|| operator instanceof OpDivide || operator instanceof OpModulus;
	}

	/**
	 * Returns whether the operator is unary.
	 * 
	 * @return
	 */
	public boolean isUnaryOperator() {
		return operator.getRightOperand() == null;
	}

	/**
	 * Returns the Mongo expression of the operator.
	 * 
	 * @return
	 */
	public String getMongoOperator() {
		return OPERATORS.get(operator.getOperatorName());
	}

	/**
	 * Returns whether the operator is a unary minus, e.g. -1.
	 * 
	 * @return
	 */
	public boolean isUnaryMinus() {
		return isUnaryOperator() && operator instanceof OpMinus;
	}

	/**
	 * Returns the left operand as {@link ExpressionNode}.
	 * 
	 * @return
	 */
	public ExpressionNode getLeft() {
		return from(operator.getLeftOperand());
	}

	/**
	 * Returns the right operand as {@link ExpressionNode}.
	 * 
	 * @return
	 */
	public ExpressionNode getRight() {
		return from(operator.getRightOperand());
	}
}
