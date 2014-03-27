package org.springframework.data.mongodb.core.mapping;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;

/**
 * @author Guto Bortolozzo
 */
@Configuration
public class PagingAndSortingTreeSetTest extends PagingAndSortingTreeSetConfiguration{
	
	@Test
	public void testExistsInstanceOfPagingRepository(){
		assertNotNull(repository);
	}
	
	@Test
	public void testFindForEntityWithFieldOfTreeMap() {
		
		Entity entity = new Entity();
		
		entity.treeMap.put("foo", new Element());

		testEntity(entity, "Entity [treeMap={foo=Element [foo=Bar]}, hashMap={}, hashSet=[]]");
	}
	
	@Test
	public void testFindForEntityWithFieldOfHashMap() {
		
		Entity entity = new Entity();
		
		entity.hashMap.put("foo", new Element());

		testEntity(entity, "Entity [treeMap={}, hashMap={foo=Element [foo=Bar]}, hashSet=[]]");
	}
	
	@Test
	public void testFindForEntityWithFieldOfHashSet() {
		
		Entity entity = new Entity();
		
		entity.hashSet.add(new Element());

		testEntity(entity, "Entity [treeMap={}, hashMap={}, hashSet=[Element [foo=Bar]]]");
	}

	private void testEntity(Entity entity, String toString) {
		
		operations.insert(entity);
		
		List<Entity> all = operations.findAll(Entity.class);
		
		assertThat(all, hasSize(1));
		
		Entity searched = all.get(0);
		
		assertThat(searched.toString(), is(toString));
		assertThat(searched.id, is(entity.id));
	}
	
	@Document(collection = "test_collection")
	static class Entity {
		
		@Id
		String id;
		TreeMap<String,Element> treeMap;
		HashMap<String, Element> hashMap;
		HashSet<Element> hashSet;
		
		public Entity() {
			id = ObjectId.get().toString();
			treeMap = new TreeMap<String, Element>();
			hashMap = new HashMap<String, Element>();
			hashSet = new HashSet<Element>();
		}

		@Override
		public String toString() {
			return "Entity [treeMap=" + treeMap + ", hashMap=" + hashMap+ ", hashSet=" + hashSet + "]";
		}
	}
	
	static class Element { 
		String foo = "Bar";

		@Override
		public String toString() {
			return "Element [foo=" + foo + "]";
		}
	}
}