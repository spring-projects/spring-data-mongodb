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

import org.jspecify.annotations.Nullable;
import org.springframework.core.ResolvableType;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
class VariableSnippet extends ExpressionSnippet {

	private final String variableName;
	private final @Nullable TypeName typeName;

	public VariableSnippet(String variableName, Snippet delegate) {
		this((TypeName) null, variableName, delegate);
	}

	public VariableSnippet(Class<?> typeName, String variableName, Snippet delegate) {
		this(TypeName.get(typeName), variableName, delegate);
	}

	public VariableSnippet(@Nullable TypeName typeName, String variableName, Snippet delegate) {
		super(delegate);
		this.typeName = typeName;
		this.variableName = variableName;
	}

	static VariableBuilderImp variable(String name) {
		return new VariableBuilderImp(null, name);
	}

	static VariableBuilderImp variable(Class<?> typeName, String name) {
		return variable(TypeName.get(typeName), name);
	}

	static VariableBuilderImp variable(ResolvableType resolvableType, String name) {
		return variable(TypeName.get(resolvableType.getType()), name);
	}

	static VariableBuilderImp variable(TypeName typeName, String name) {
		return new VariableBuilderImp(typeName, name);
	}

	static class VariableBuilderImp implements VariableBuilder {

		private final @Nullable TypeName typeName;
		private final String variableName;

		private CodeBlock.@Nullable Builder target;

		VariableBuilderImp(@Nullable TypeName typeName, String variableName) {
			this.typeName = typeName;
			this.variableName = variableName;
		}

		@Override
		public VariableSnippet of(CodeBlock codeBlock) {

			VariableSnippet variableSnippet = new VariableSnippet(typeName, variableName, Snippet.just(codeBlock));
			if (target != null) {
				variableSnippet.renderDeclaration(target);
			}
			return variableSnippet;
		}

		VariableBuilderImp appendTo(@Nullable Builder target) {
			this.target = target;
			return this;
		}
	}

	@Override
	public CodeBlock code() {
		return CodeBlock.of("$L", variableName);
	}

	public String getVariableName() {
		return variableName;
	}

	void renderDeclaration(CodeBlock.Builder builder) {
		if (typeName != null) {
			builder.addStatement("$T $L = $L", typeName, variableName, super.code());
		} else {
			builder.addStatement("var $L = $L", variableName, super.code());
		}
	}

	static VariableBlockBuilder create(Snippet snippet) {
		return variableName -> create(variableName, snippet);
	}

	static VariableSnippet create(String variableName, Snippet snippet) {
		return new VariableSnippet(variableName, snippet);
	}

	interface VariableBlockBuilder {
		VariableSnippet variableName(String variableName);
	}
}
