package org.springframework.data.mongodb.core.mapping.event;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.CascadeType;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Used in {@link CascadingMongoEventListenerTests}.
 * Each field with {@link Address} type represents association with different cascade operation.
 *
 * @author Maciej Walkowiak
 */
@Document
public class Person {
	@Id
	private ObjectId id;

	@DBRef(cascadeType = CascadeType.SAVE)
	private Address addressWithCascadeSave;

	@DBRef(cascadeType = CascadeType.DELETE)
	private Address addressWithCascadeDelete;

	@DBRef(cascadeType = CascadeType.ALL)
	private Address addressWithCascadeAll;

	@DBRef(cascadeType = CascadeType.NONE)
	private Address addressWithCascadeNone;

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public Address getAddressWithCascadeSave() {
		return addressWithCascadeSave;
	}

	public void setAddressWithCascadeSave(Address addressWithCascadeSave) {
		this.addressWithCascadeSave = addressWithCascadeSave;
	}

	public Address getAddressWithCascadeDelete() {
		return addressWithCascadeDelete;
	}

	public void setAddressWithCascadeDelete(Address addressWithCascadeDelete) {
		this.addressWithCascadeDelete = addressWithCascadeDelete;
	}

	public Address getAddressWithCascadeAll() {
		return addressWithCascadeAll;
	}

	public void setAddressWithCascadeAll(Address addressWithCascadeAll) {
		this.addressWithCascadeAll = addressWithCascadeAll;
	}

	public Address getAddressWithCascadeNone() {
		return addressWithCascadeNone;
	}

	public void setAddressWithCascadeNone(Address addressWithCascadeNone) {
		this.addressWithCascadeNone = addressWithCascadeNone;
	}
}
