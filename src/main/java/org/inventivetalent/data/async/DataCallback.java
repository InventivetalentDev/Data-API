package org.inventivetalent.data.async;

import javax.annotation.Nullable;

public interface DataCallback<V> {

	void provide(@Nullable V v);

}
