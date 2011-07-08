package org.springframework.data.mongodb;

import org.springframework.transaction.support.ResourceHolder;
import org.springframework.transaction.support.ResourceHolderSynchronization;

class MongoSynchronization extends ResourceHolderSynchronization<ResourceHolder, Object> {

	public MongoSynchronization(ResourceHolder resourceHolder, Object resourceKey) {
		super(resourceHolder, resourceKey);
	}
}
