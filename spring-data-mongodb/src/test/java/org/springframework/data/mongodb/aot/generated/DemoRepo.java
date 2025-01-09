/*
 * Copyright 2025. the original author or authors.
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

/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.aot.generated;

import java.util.List;

import example.aot.User;
import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.StringBasedMongoQuery;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
public class DemoRepo {


    MongoOperations operations;

    List<User> method1(String username) {

        BindableMongoExpression filter = new BindableMongoExpression("{ 'username', ?0 }", operations.getConverter(), new Object[]{username});
        Query query = new BasicQuery(filter.toDocument());

        return operations.query(User.class)
            .as(User.class)
            .matching(query)
            .all();
    }
}
