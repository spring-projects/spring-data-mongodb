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

import java.util.Map;
import java.util.Set;

import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.ast.*;

/**
 * An {@link ExpressionNode} representing an operator.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class OperatorNode extends ExpressionNode {

	private static final Map<String, String> OPERATORS = Map.ofEntries(Map.entry("+", "$add"),
			Map.entry("-", "$subtract"), Map.entry("*", "$multiply"), Map.entry("/", "$divide"), Map.entry("%", "$mod"),
			Map.entry("^", "$pow"), Map.entry("==", "$eq"), Map.entry("!=", "$ne"), Map.entry(">", "$gt"),
			Map.entry(">=", "$gte"), Map.entry("<", "$lt"), Map.entry("<=", "$lte"), Map.entry("and", "$and"),
			Map.entry("or", "$or"));

	private static final Set<Class> SUPPORTED_MATH_OPERATORS = Set.of(OpMinus.class, OpPlus.class, OpMultiply.class,
			OpDivide.class, OpModulus.class, OperatorPower.class, OpNE.class, OpEQ.class, OpGT.class, OpGE.class, OpLT.class,
			OpLE.class);

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

	@Override
	public boolean isMathematicalOperation() {
		return SUPPORTED_MATH_OPERATORS.contains(operator.getClass());
	}

	@Override
	public boolean isLogicalOperator() {
		return operator instanceof OpOr || operator instanceof OpAnd;
	}

	/**
	 * Returns whether the operator is unary.
	 *
	 * @return
	 */
	public boolean isUnaryOperator() {
		return operator.getChildCount() == 1;
	}

	/**
	 * Returns the Mongo expression of the operator.
	 *
	 * @return
	 */
	public String getMongoOperator() {

		if (!OPERATORS.containsKey(operator.getOperatorName())) {
			throw new IllegalArgumentException(String.format(
					"Unknown operator name; Cannot translate %s into its MongoDB aggregation function representation",
					operator.getOperatorName()));
		}

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
