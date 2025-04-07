/*
 * Copyright 2025 the original author or authors.
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
package example.aot;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.Hint;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReadPreference;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.data.repository.CrudRepository;

/**
 * @author Christoph Strobl
 */
public interface UserRepository extends CrudRepository<User, String> {

	/* Derived Queries */

	List<User> findUserNoArgumentsBy();

	User findOneByUsername(String username);

	Optional<User> findOptionalOneByUsername(String username);

	Long countUsersByLastname(String lastname);

	Boolean existsUserByLastname(String lastname);

	List<User> findByLastnameStartingWith(String lastname);

	List<User> findByLastnameEndsWith(String postfix);

	List<User> findByFirstnameLike(String firstname);

	List<User> findByFirstnameNotLike(String firstname);

	List<User> findByUsernameIn(Collection<String> usernames);

	List<User> findByUsernameNotIn(Collection<String> usernames);

	List<User> findByFirstnameAndLastname(String firstname, String lastname);

	List<User> findByFirstnameOrLastname(String firstname, String lastname);

	List<User> findByVisitsBetween(long from, long to);

	List<User> findByLastSeenGreaterThan(Instant time);

	List<User> findByVisitsExists(boolean exists);

	List<User> findByLastnameNot(String lastname);

	List<User> findTop2ByLastnameStartingWith(String lastname);

	List<User> findByLastnameStartingWithOrderByUsername(String lastname);

	List<User> findByLastnameStartingWith(String lastname, Limit limit);

	List<User> findByLastnameStartingWith(String lastname, Sort sort);

	List<User> findByLastnameStartingWith(String lastname, Sort sort, Limit limit);

	List<User> findByLastnameStartingWith(String lastname, Pageable page);

	Page<User> findPageOfUsersByLastnameStartingWith(String lastname, Pageable page);

	Slice<User> findSliceOfUserByLastnameStartingWith(String lastname, Pageable page);

	// TODO: Streaming
	// TODO: Scrolling
	// TODO: GeoQueries
	// TODO: TextSearch

	/* Annotated Queries */

	@Query("{ 'username' : ?0 }")
	User findAnnotatedQueryByUsername(String username);

	@Query(value = "{ 'lastname' : { '$regex' : '^?0' } }", count = true)
	Long countAnnotatedQueryByLastname(String lastname);

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

	/* deletes */

	User deleteByUsername(String username);

	@Query(value = "{ 'username' : ?0 }", delete = true)
	User deleteAnnotatedQueryByUsername(String username);

	Long deleteByLastnameStartingWith(String lastname);

	@Query(value = "{ 'lastname' : { '$regex' : '^?0' } }", delete = true)
	Long deleteAnnotatedQueryByLastnameStartingWith(String lastname);

	List<User> deleteUsersByLastnameStartingWith(String lastname);

	@Query(value = "{ 'lastname' : { '$regex' : '^?0' } }", delete = true)
	List<User> deleteUsersAnnotatedQueryByLastnameStartingWith(String lastname);

	/* Updates */

	@Update("{ '$inc' : { 'visits' : ?1 } }")
	int findUserAndIncrementVisitsByLastname(String lastname, int increment);

	@Query("{ 'lastname' : ?0 }")
	@Update("{ '$inc' : { 'visits' : ?1 } }")
	int updateAllByLastname(String lastname, int increment);

	@Update(pipeline = { "{ '$set' : { 'visits' : { '$ifNull' : [ {'$add' : [ '$visits', ?1 ] }, ?1 ] } } }" })
	void findAndIncrementVisitsViaPipelineByLastname(String lastname, int increment);

	/* Derived With Annotated Options */

	@Query(sort = "{ 'username' : 1 }")
	List<User> findWithAnnotatedSortByLastnameStartingWith(String lastname);

	@Query(fields = "{ 'username' : 1 }")
	List<User> findWithAnnotatedFieldsProjectionByLastnameStartingWith(String lastname);

	@ReadPreference("no-such-read-preference")
	User findWithReadPreferenceByUsername(String username);

	// TODO: hints

	/* Projecting Queries */

	List<UserProjection> findUserProjectionByLastnameStartingWith(String lastname);

	Page<UserProjection> findUserProjectionByLastnameStartingWith(String lastname, Pageable page);

	/* Aggregations */

	@Aggregation(pipeline = { //
			"{ '$match' : { 'last_name' : { '$ne' : null } } }", //
			"{ '$project': { '_id' : '$last_name' } }" })
	List<String> findAllLastnames();

	@Aggregation(pipeline = { //
			"{ '$match' : { 'last_name' : { '$ne' : null } } }", //
			"{ '$group': { '_id' : '$last_name', names : { $addToSet : '$?0' } } }" })
	List<UserAggregate> groupByLastnameAnd(String property);

	@Aggregation(pipeline = { //
			"{ '$match' : { 'last_name' : { '$ne' : null } } }", //
			"{ '$group': { '_id' : '$last_name', names : { $addToSet : '$?0' } } }" })
	List<UserAggregate> groupByLastnameAnd(String property, Pageable pageable);

	@Aggregation(pipeline = { //
			"{ '$match' : { 'last_name' : { '$ne' : null } } }", //
			"{ '$group': { '_id' : '$last_name', names : { $addToSet : '$?0' } } }" })
	Slice<UserAggregate> groupByLastnameAndReturnPage(String property, Pageable pageable);

	@Aggregation(pipeline = { //
			"{ '$match' : { 'last_name' : { '$ne' : null } } }", //
			"{ '$group': { '_id' : '$last_name', names : { $addToSet : '$?0' } } }" })
	AggregationResults<UserAggregate> groupByLastnameAndAsAggregationResults(String property);

	@Aggregation(pipeline = { //
			"{ '$match' : { 'posts' : { '$ne' : null } } }", //
			"{ '$project': { 'nrPosts' : {'$size': '$posts' } } }", //
			"{ '$group' : { '_id' : null, 'total' : { $sum: '$nrPosts' } } }" })
	int sumPosts();

	@Hint("ln-idx")
	@Aggregation(pipeline = { //
			"{ '$match' : { 'last_name' : { '$ne' : null } } }", //
			"{ '$project': { '_id' : '$last_name' } }" })
	List<String> findAllLastnamesUsingIndex();

	@ReadPreference("no-such-read-preference")
	@Aggregation(pipeline = { //
			"{ '$match' : { 'last_name' : { '$ne' : null } } }", //
			"{ '$project': { '_id' : '$last_name' } }" })
	List<String> findAllLastnamesWithReadPreference();

	@Aggregation(pipeline = { //
			"{ '$match' : { 'last_name' : { '$ne' : null } } }", //
			"{ '$project': { '_id' : '$last_name' } }" }, collation = "no_collation")
	List<String> findAllLastnamesWithCollation();

	class UserAggregate {

		@Id //
		private final String lastname;
		private final Set<String> names;

		public UserAggregate(String lastname, Collection<String> names) {
			this.lastname = lastname;
			this.names = new HashSet<>(names);
		}

		public String getLastname() {
			return this.lastname;
		}

		public Set<String> getNames() {
			return this.names;
		}

		@Override
		public String toString() {
			return "UserAggregate{" + "lastname='" + lastname + '\'' + ", names=" + names + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			UserAggregate that = (UserAggregate) o;
			return Objects.equals(lastname, that.lastname) && names.equals(that.names);
		}

		@Override
		public int hashCode() {
			return Objects.hash(lastname, names);
		}
	}
}
