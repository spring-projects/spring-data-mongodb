package org.springframework.data.mongodb.core.mapping;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.util.TypeInformation;

import java.util.concurrent.ConcurrentHashMap;

public class ProfiledMongoPersistanceEntity<T> extends  BasicMongoPersistentEntity<T> {


	private ConcurrentHashMap<String,MongoPersistentProperty> propertyCache = new ConcurrentHashMap<String, MongoPersistentProperty>();
	private ConcurrentHashMap<PersistentProperty<?>,Boolean> constructorArgumentCache = new ConcurrentHashMap<PersistentProperty<?>, Boolean>();

	public ProfiledMongoPersistanceEntity(TypeInformation<T> typeInformation) {
		super(typeInformation);
	}

	@Override
	public void addPersistentProperty(MongoPersistentProperty property) {

		super.addPersistentProperty(property);
		propertyCache.put(property.getName(), property);
	}

	@Override
	public MongoPersistentProperty getPersistentProperty(String name) {
		return propertyCache.get(name);
	}

	@Override
	public boolean isConstructorArgument(PersistentProperty<?> property) {

		Boolean toReturn;

		if ((toReturn = constructorArgumentCache.get(property)) == null) {
			toReturn = super.isConstructorArgument(property);
			constructorArgumentCache.put(property, toReturn);
		}
		return toReturn;
	}
}
