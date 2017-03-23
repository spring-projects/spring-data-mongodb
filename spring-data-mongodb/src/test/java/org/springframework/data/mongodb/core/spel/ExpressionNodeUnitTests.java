/*
 * Copyright 2013-2017 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.expression.spel.ExpressionState;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.OpDivide;
import org.springframework.expression.spel.ast.OpMinus;
import org.springframework.expression.spel.ast.OpMultiply;
import org.springframework.expression.spel.ast.OpPlus;

/**
 * Unit tests for {@link ExpressionNode}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class ExpressionNodeUnitTests {

	@Mock ExpressionState state;

	@Mock OpMinus minus;
	@Mock OpPlus plus;
	@Mock OpDivide divide;
	@Mock OpMultiply multiply;

	Collection<? extends SpelNode> operators;

	@Before
	public void setUp() {
		this.operators = Arrays.asList(minus, plus, divide, multiply);
	}

	@Test // DATAMONGO-774
	public void createsOperatorNodeForOperations() {

		for (SpelNode operator : operators) {
			assertThat(ExpressionNode.from(operator, state), is(instanceOf(OperatorNode.class)));
		}
	}
}
