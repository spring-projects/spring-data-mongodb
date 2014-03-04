package org.springframework.data.mongodb.core.batch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
abstract class AbstractBatch<T>{
	
	@Autowired MongoOperations template;
	
	List<T> elements = new ArrayList<T>();
	
	Integer batchSize = 0;

	protected AbstractBatch(Integer batchSize) {
		this.batchSize = batchSize;
	}
	
	protected boolean mustFlush(){
		return elements.size() >= batchSize;
	}
}
