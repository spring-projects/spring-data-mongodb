package org.springframework.data.mongodb.core.batch;

import java.util.List;

import org.springframework.stereotype.Repository;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
@Repository
public interface BatchInsertOperations {

	public void insert(Object element);
	
	public void insertAll(List<? extends Object> elements);
	
	void flush();
	
	void clear();
}
