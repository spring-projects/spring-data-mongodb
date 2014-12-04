/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core.script;

import org.springframework.util.Assert;

/**
 * {@link ServerSideJavaScript} implementation that can be saved or directly executed.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class ExecutableMongoScript implements ServerSideJavaScript {

	private final String code;

	/**
	 * Creates new {@link ExecutableMongoScript}.
	 * 
	 * @param code must not be {@literal null} or {@literal empty}.
	 */
	public ExecutableMongoScript(String code) {

		Assert.hasText(code, "Code must not be null or empty!");
		this.code = code;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.script.MongoScript#getCode()
	 */
	@Override
	public String getCode() {
		return this.code;
	}

}
