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
package example.aot;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.repository.CrudRepository;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
public interface UserRepository extends CrudRepository<User, String> {

	List<User> findUserNoArgumentsBy();

	User findOneByUsername(String username);

	Optional<User> findOptionalOneByUsername(String username);

	Long countUsersByLastname(String lastname);

	Boolean existsUserByLastname(String lastname);

	List<User> findByLastnameStartingWith(String lastname);

	List<User> findTop2ByLastnameStartingWith(String lastname);

	List<User> findByLastnameStartingWithOrderByUsername(String lastname);

	List<User> findByLastnameStartingWith(String lastname, Limit limit);

	List<User> findByLastnameStartingWith(String lastname, Sort sort);

	List<User> findByLastnameStartingWith(String lastname, Sort sort, Limit limit);

	List<User> findByLastnameStartingWith(String lastname, Pageable page);

	Page<User> findPageOfUsersByLastnameStartingWith(String lastname, Pageable page);

	Slice<User> findSliceOfUserByLastnameStartingWith(String lastname, Pageable page);

	@Query("{ 'lastname' : { '$regex' : '^?0' } }")
	List<User> findAnnotatedQueryByLastname(String lastname);

	@Query("""
			{
			    'lastname' : {
			        '$regex' : '^?0'
			    }
			}""")
	List<User> findAnnotatedMultilineQueryByLastname(String username);

	@Query("{ 'lastname' : { '$regex' : '^?0' } }")
	List<User> findAnnotatedQueryByLastname(String lastname, Limit limit);

	@Query("{ 'lastname' : { '$regex' : '^?0' } }")
	List<User> findAnnotatedQueryByLastname(String lastname, Sort sort);

	@Query("{ 'lastname' : { '$regex' : '^?0' } }")
	List<User> findAnnotatedQueryByLastname(String lastname, Limit limit, Sort sort);

	@Query("{ 'lastname' : { '$regex' : '^?0' } }")
	List<User> findAnnotatedQueryByLastname(String lastname, Pageable pageable);

	@Query("{ 'lastname' : { '$regex' : '^?0' } }")
	Page<User> findAnnotatedQueryPageOfUsersByLastname(String lastname, Pageable pageable);

	@Query("{ 'lastname' : { '$regex' : '^?0' } }")
	Slice<User> findAnnotatedQuerySliceOfUsersByLastname(String lastname, Pageable pageable);

	@Query(sort = "{ 'username' : 1 }")
	List<User> findWithAnnotatedSortByLastnameStartingWith(String lastname);

	@ReadPreference("secondary")
	User findWithReadPreferenceByUsername(String username);

	Page<UserProjection> findUserProjectionBy(Pageable pageable);

	@Query(sort = "{ 'last_name' : -1}")
	List<User> findByLastnameAfter(String lastname);

	@Query(fields = "{ '_id' : -1}")
	List<User> findByLastnameBefore(String lastname);

	List<User> findByLastnameOrderByFirstnameDesc(String lastname);

	List<User> findUserByLastnameLike(String lastname);

}
