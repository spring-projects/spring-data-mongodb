package org.springframework.data.mongodb.core.index;

import java.util.Objects;

/**
 * Represents a unique pair of type and collection, used for keeping track of
 *
 * @author Nick Balkissoon
 */
public class TypeCollectionPair {

	private final Class type;
	private final String collection;

	public TypeCollectionPair(Class type, String collection) {
		this.type = type;
		this.collection = collection;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TypeCollectionPair that = (TypeCollectionPair) o;
		return Objects.equals(type, that.type) &&
				Objects.equals(collection, that.collection);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, collection);
	}
}
