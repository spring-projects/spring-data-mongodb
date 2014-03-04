package org.springframework.data.mongodb.core.batch;

import java.util.List;

import org.springframework.stereotype.Repository;

/**
 * @author Joao Bortolozzo
 * see DATAMONGO-867
 */
@Repository
public interface BatchInsertOperations<T> {

	public void insert(T element);
	
	public void insert(List<T> elements);
	
	void flush();
	
	void clear();
}
