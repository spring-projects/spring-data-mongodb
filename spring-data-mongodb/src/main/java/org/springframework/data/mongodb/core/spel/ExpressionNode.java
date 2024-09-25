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
package org.springframework.data.mongodb.core.spel;

import java.util.Collections;
import java.util.Iterator;

import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.Literal;
import org.springframework.expression.spel.ast.MethodReference;
import org.springframework.expression.spel.ast.Operator;
import org.springframework.expression.spel.ast.OperatorNot;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A value object for nodes in an expression. Allows iterating ove potentially available child {@link ExpressionNode}s.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ExpressionNode implements Iterable<ExpressionNode> {

	private static final Iterator<ExpressionNode> EMPTY_ITERATOR = Collections.<ExpressionNode> emptySet().iterator();

	private final SpelNode node;
	private final ExpressionState state;

	/**
	 * Creates a new {@link ExpressionNode} from the given {@link SpelNode} and {@link ExpressionState}.
	 *
	 * @param node must not be {@literal null}.
	 * @param state must not be {@literal null}.
	 */
	protected ExpressionNode(SpelNode node, ExpressionState state) {

		Assert.notNull(node, "SpelNode must not be null");
		Assert.notNull(state, "ExpressionState must not be null");

		this.node = node;
		this.state = state;
	}

	/**
	 * Factory method to create {@link ExpressionNode}'s according to the given {@link SpelNode} and
	 * {@link ExpressionState}.
	 *
	 * @param node
	 * @param state must not be {@literal null}.
	 * @return an {@link ExpressionNode} for the given {@link SpelNode} or {@literal null} if {@literal null} was given
	 *         for the {@link SpelNode}.
	 */
	public static ExpressionNode from(SpelNode node, ExpressionState state) {

		if (node instanceof Operator operator) {
			return new OperatorNode(operator, state);
		}

		if (node instanceof MethodReference methodReference) {
			return new MethodReferenceNode(methodReference, state);
		}

		if (node instanceof Literal literal) {
			return new LiteralNode(literal, state);
		}

		if (node instanceof OperatorNot operatorNot) {
			return new NotOperatorNode(operatorNot, state);
		}

		return new ExpressionNode(node, state);
	}

	/**
	 * Returns the name of the {@link ExpressionNode}.
	 *
	 * @return
	 */
	public String getName() {
		return node.toStringAST();
	}

	/**
	 * Returns whether the current {@link ExpressionNode} is backed by the given type.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public boolean isOfType(Class<?> type) {

		Assert.notNull(type, "Type must not be empty");
		return type.isAssignableFrom(node.getClass());
	}

	/**
	 * Returns whether the given {@link ExpressionNode} is representing the same backing node type as the current one.
	 *
	 * @param node
	 * @return
	 */
	boolean isOfSameTypeAs(@Nullable ExpressionNode node) {
		return node != null && this.node.getClass().equals(node.node.getClass());
	}

	/**
	 * Returns whether the {@link ExpressionNode} is a mathematical operation.
	 *
	 * @return
	 */
	public boolean isMathematicalOperation() {
		return false;
	}

	/**
	 * Returns whether the {@link ExpressionNode} is a logical conjunction operation like {@code &&, ||}.
	 *
	 * @return
	 * @since 1.10
	 */
	public boolean isLogicalOperator() {
		return false;
	}

	/**
	 * Returns whether the {@link ExpressionNode} is a literal.
	 *
	 * @return
	 */
	public boolean isLiteral() {
		return false;
	}

	/**
	 * Returns the value of the current node.
	 *
	 * @return
	 */
	@Nullable
	public Object getValue() {
		return node.getValue(state);
	}

	/**
	 * Returns whether the current node has child nodes.
	 *
	 * @return
	 */
	public boolean hasChildren() {
		return node.getChildCount() != 0;
	}

	/**
	 * Returns the child {@link ExpressionNode} with the given index.
	 *
	 * @param index must not be negative.
	 * @return
	 */
	public ExpressionNode getChild(int index) {

		Assert.isTrue(index >= 0, "Index must be greater or equal to zero");
		return from(node.getChild(index), state);
	}

	/**
	 * Returns whether the {@link ExpressionNode} has a first child node that is not of the given type.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public boolean hasfirstChildNotOfType(Class<?> type) {

		Assert.notNull(type, "Type must not be null");
		return hasChildren() && !node.getChild(0).getClass().equals(type);
	}

	/**
	 * Creates a new {@link ExpressionNode} from the given {@link SpelNode}.
	 *
	 * @param node
	 * @return
	 */
	protected ExpressionNode from(SpelNode node) {
		return from(node, state);
	}

	@Override
	public Iterator<ExpressionNode> iterator() {

		if (!hasChildren()) {
			return EMPTY_ITERATOR;
		}

		return new Iterator<ExpressionNode>() {

			int index = 0;

			@Override
			public boolean hasNext() {
				return index < node.getChildCount();
			}

			@Override
			public ExpressionNode next() {
				return from(node.getChild(index++));
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
