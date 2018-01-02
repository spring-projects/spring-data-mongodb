/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb.config;

/**
 * Constants to declare bean names used by the namespace configuration.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Martin Baumgartner
 * @author Christoph Strobl
 */
public abstract class BeanNames {

	public static final String MAPPING_CONTEXT_BEAN_NAME = "mongoMappingContext";

	static final String INDEX_HELPER_BEAN_NAME = "indexCreationHelper";
	static final String MONGO_BEAN_NAME = "mongoClient";
	static final String DB_FACTORY_BEAN_NAME = "mongoDbFactory";
	static final String VALIDATING_EVENT_LISTENER_BEAN_NAME = "validatingMongoEventListener";
	static final String IS_NEW_STRATEGY_FACTORY_BEAN_NAME = "isNewStrategyFactory";
	static final String DEFAULT_CONVERTER_BEAN_NAME = "mappingConverter";
	static final String MONGO_TEMPLATE_BEAN_NAME = "mongoTemplate";
	static final String GRID_FS_TEMPLATE_BEAN_NAME = "gridFsTemplate";
}
