/*
 * Copyright 2026-present the original author or authors.
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
lexer grammar MqlStringLexer;

@header {
/**
 * Splits the content of quoted strings and regular expressions into literal text, parameter placeholders and value
 * expressions.
 * <p>
 * Applied to the content of {@code STRING} and {@code REGEX} tokens produced by {@code Mql.g4}. A separate lexer
 * because the main grammar is a combined grammar and ANTLR allows lexical modes only in lexer grammars.
 *
 * @author Christoph Strobl
 * @since 5.3
 */
}

/*
    Lexer rules
 */

EXPRESSION
    : [?:] '#' '{' EXPRESSION_PART* '}'
    | [?:] '$' '{' PROPERTY_PART* '}'
    ;

PLACEHOLDER : '?' [0-9]+ ;

TEXT : ~[?:]+ ;

ANY : . ;

fragment EXPRESSION_PART
    : '\'' ~'\''* '\''
    | '"' ~'"'* '"'
    | '{' EXPRESSION_PART* '}'
    | ~['"{}]
    ;

fragment PROPERTY_PART
    : '{' PROPERTY_PART* '}'
    | ~[{}]
    ;
