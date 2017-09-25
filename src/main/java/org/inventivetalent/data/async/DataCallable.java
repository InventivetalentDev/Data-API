package org.inventivetalent.data.async;

import lombok.NonNull;

public interface DataCallable<V> {

	@NonNull
	V provide();

}
