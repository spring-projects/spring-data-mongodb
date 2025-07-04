/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import org.springframework.javapoet.CodeBlock;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
class ExpressionSnippet implements Snippet {

	private final CodeBlock block;
	private final boolean requiresEvaluation;

	public ExpressionSnippet(CodeBlock block) {
		this(block, false);
	}

	public ExpressionSnippet(Snippet block) {
		this(block.code(), block instanceof ExpressionSnippet eb && eb.requiresEvaluation());
	}

	public ExpressionSnippet(CodeBlock block, boolean requiresEvaluation) {
		this.block = block;
		this.requiresEvaluation = requiresEvaluation;
	}

	public static ExpressionSnippet empty() {
		return new ExpressionSnippet(CodeBlock.builder().build());
	}

	public boolean requiresEvaluation() {
		return requiresEvaluation;
	}

	public CodeBlock code() {
		return block;
	}
}
