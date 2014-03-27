package org.springframework.data.mongodb.core.mapping;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.config.AbstractIntegrationTests;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.PagingAndSortingTreeSetTest.Entity;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Guto Bortolozzo
 * see DATAMONGO-887
 */
@ContextConfiguration
abstract class PagingAndSortingTreeSetConfiguration extends AbstractIntegrationTests{
	
	@Configuration
	static class Config {
		
		@Bean
		public SourceRepository sourceRepository(){
			return new SourceRepositoryImpl();
		}
	}
	
	@Autowired protected SourceRepository repository;
	
	@Autowired protected MongoOperations operations;
	
	interface SourceRepository extends PagingAndSortingRepository<Entity, ObjectId> {}
	
	static class SourceRepositoryImpl implements SourceRepository {

		@Override
		public Iterable<Entity> findAll(Sort sort) {
			return null;
		}

		@Override
		public Page<Entity> findAll(Pageable pageable) {
			return null;
		}

		@Override
		public <S extends Entity> S save(S entity) {
			return null;
		}

		@Override
		public <S extends Entity> Iterable<S> save(Iterable<S> entities) {
			return null;
		}

		@Override
		public Entity findOne(ObjectId id) {
			return null;
		}

		@Override
		public boolean exists(ObjectId id) {
			return false;
		}

		@Override
		public Iterable<Entity> findAll() {
			return null;
		}

		@Override
		public Iterable<Entity> findAll(Iterable<ObjectId> ids) {
			return null;
		}

		@Override
		public long count() {
			return 0;
		}

		@Override
		public void delete(ObjectId id) {}

		@Override
		public void delete(Entity entity) {}

		@Override
		public void delete(Iterable<? extends Entity> entities) {}

		@Override
		public void deleteAll() {}
	}
}
