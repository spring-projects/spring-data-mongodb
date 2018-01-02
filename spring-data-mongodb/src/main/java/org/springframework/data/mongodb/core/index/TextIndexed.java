/*
 * Copyright 2014-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link TextIndexed} marks a field to be part of the text index. As there can be only one text index per collection
 * all fields marked with {@link TextIndexed} are combined into one single index. <br />
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.6
 */
@Documented
@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface TextIndexed {

	/**
	 * Defines the significance of the filed relative to other indexed fields. The value directly influences the documents
	 * score. <br/>
	 * Defaulted to {@literal 1.0}.
	 *
	 * @return
	 */
	float weight() default 1.0F;
}
