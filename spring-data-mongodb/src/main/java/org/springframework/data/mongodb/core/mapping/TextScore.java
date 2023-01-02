/*
 * Copyright 2014-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.ReadOnlyProperty;

/**
 * {@link TextScore} marks the property to be considered as the on server calculated {@literal textScore} when doing
 * full text search. <br />
 * <b>NOTE</b> Property will not be written when saving entity and may be {@literal null} if the document is retrieved
 * by a regular (i.e. {@literal $text}) query.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.6
 */
@ReadOnlyProperty
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TextScore {

}
