package io.github.tsegismont.eblink.vertx3;

import io.vertx.core.spi.cluster.ChoosableIterable;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class WrappedChoosableIterable<V> implements ChoosableIterable<V> {

  private final V inserted;
  private final ChoosableIterable<V> delegate;

  public WrappedChoosableIterable(V fakeClusterNodeInfo, ChoosableIterable<V> delegate) {
    this.inserted = fakeClusterNodeInfo;
    this.delegate = delegate;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public V choose() {
    return delegate.isEmpty() ? inserted:delegate.choose();
  }

  @Override
  public Iterator<V> iterator() {
    Iterator<V> iter = delegate.iterator();
    return new Iterator<V>() {

      AtomicBoolean first = new AtomicBoolean(true);

      @Override
      public boolean hasNext() {
        return first.get() || iter.hasNext();
      }

      @Override
      public V next() {
        return first.compareAndSet(true, false) ? inserted:iter.next();
      }
    };
  }
}
