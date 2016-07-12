package org.inventivetalent.data.async;

import javax.annotation.Nonnull;

public interface DataCallable<V> {

	@Nonnull
	V provide();

}
