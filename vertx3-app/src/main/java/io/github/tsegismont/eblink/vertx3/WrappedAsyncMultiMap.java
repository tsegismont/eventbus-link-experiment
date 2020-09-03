package io.github.tsegismont.eblink.vertx3;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;

import java.util.Set;
import java.util.function.Predicate;

public class WrappedAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {

  private final AsyncMultiMap<K, V> delegate;
  private final Set<String> addresses;
  private final V fakeClusterNodeInfo;

  public WrappedAsyncMultiMap(AsyncMultiMap<K, V> delegate, Set<String> addresses, ClusterNodeInfo fakeClusterNodeInfo) {
    this.delegate = delegate;
    this.addresses = addresses;
    this.fakeClusterNodeInfo = (V) fakeClusterNodeInfo;
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    delegate.add(k, v, completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
    if (addresses.contains(k)) {
      delegate.get(k, ar -> {
        if (ar.succeeded()) {
          asyncResultHandler.handle(Future.succeededFuture(new WrappedChoosableIterable<>(fakeClusterNodeInfo, ar.result())));
        } else {
          asyncResultHandler.handle(ar);
        }
      });
    } else {
      delegate.get(k, asyncResultHandler);
    }
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    delegate.remove(k, v, completionHandler);
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    delegate.removeAllForValue(v, completionHandler);
  }

  @Override
  public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
    delegate.removeAllMatching(p, completionHandler);
  }
}
