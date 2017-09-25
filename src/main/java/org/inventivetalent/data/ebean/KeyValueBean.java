package org.inventivetalent.data.ebean;

import javax.persistence.Entity;
import java.beans.ConstructorProperties;

@Entity
@SuppressWarnings({"unused", "WeakerAccess"})
public class KeyValueBean extends KeyBean {

	private String value;

	public KeyValueBean() {
	}

	@ConstructorProperties({
			"name",
			"value"})
	public KeyValueBean(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
