package org.springframework.data.mongodb;

public class SomeEnumTest {

	public enum StringEnum {
		ONE, TWO, FIVE;
	}

	public enum NumberEnum {
		ONE(1), TWO(2), FIVE(5);

		private int value;

		public int value() {
			return value;
		}

		NumberEnum(int value) {
			this.value = value;
		}

	}

	private StringEnum stringEnum;

	private NumberEnum numberEnum;

	private String id;

	private String name;

	public StringEnum getStringEnum() {
		return stringEnum;
	}

	public void setStringEnum(StringEnum stringEnum) {
		this.stringEnum = stringEnum;
	}

	public NumberEnum getNumberEnum() {
		return numberEnum;
	}

	public void setNumberEnum(NumberEnum numberEnum) {
		this.numberEnum = numberEnum;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
