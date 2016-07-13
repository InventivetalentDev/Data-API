package org.inventivetalent.data.ebean;

public interface BeanProvider<B extends KeyValueBean> {

	B provide(String key, String value);

}
