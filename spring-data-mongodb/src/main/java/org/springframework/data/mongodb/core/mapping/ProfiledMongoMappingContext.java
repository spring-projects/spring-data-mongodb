package org.springframework.data.mongodb.core.mapping;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.util.TypeInformation;

public class ProfiledMongoMappingContext extends MongoMappingContext {

	private ApplicationContext context;

	@Override
	protected <T> BasicMongoPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		ProfiledMongoPersistanceEntity<T> entity = new ProfiledMongoPersistanceEntity<T>(typeInformation);

		if (context != null) {
			entity.setApplicationContext(context);
		}

		return entity;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		this.context = applicationContext;
		super.setApplicationContext(applicationContext);
	}
}
