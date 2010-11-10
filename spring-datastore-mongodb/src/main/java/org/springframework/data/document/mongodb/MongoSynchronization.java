package org.springframework.data.document.mongodb;

import org.springframework.transaction.support.ResourceHolder;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronization;

public class MongoSynchronization extends ResourceHolderSynchronization
		implements TransactionSynchronization {

	public MongoSynchronization(ResourceHolder resourceHolder,
			Object resourceKey) {
		super(resourceHolder, resourceKey);
	}

}
