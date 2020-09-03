package io.github.tsegismont.eblink.vertx3;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class EventBusLinkClusterManager implements ClusterManager {

  private final ClusterManager delegate;
  private final Set<String> addresses;
  private final ClusterNodeInfo fakeClusterNodeInfo;

  public EventBusLinkClusterManager(ClusterManager delegate, Set<String> addresses, String host, int port) {
    this.delegate = Objects.requireNonNull(delegate);
    this.addresses = Objects.requireNonNull(addresses);
    ServerID serverID = new ServerID(port, Objects.requireNonNull(host));
    this.fakeClusterNodeInfo = new ClusterNodeInfo("__vertx.eventbus.link.fake.node", serverID);
  }

  @Override
  public void setVertx(Vertx vertx) {
    delegate.setVertx(vertx);
  }

  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {
    if ("__vertx.subs".equals(name)) {
      delegate.<K, V>getAsyncMultiMap(name, ar -> {
        if (ar.succeeded()) {
          asyncResultHandler.handle(Future.succeededFuture(new WrappedAsyncMultiMap<>(ar.result(), addresses, fakeClusterNodeInfo)));
        } else {
          asyncResultHandler.handle(ar);
        }
      });
    } else {
      delegate.getAsyncMultiMap(name, asyncResultHandler);
    }
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
    delegate.getAsyncMap(name, asyncResultHandler);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return delegate.getSyncMap(name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
    delegate.getLockWithTimeout(name, timeout, resultHandler);
  }

  @Override
  public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
    delegate.getCounter(name, resultHandler);
  }

  @Override
  public String getNodeID() {
    return delegate.getNodeID();
  }

  @Override
  public List<String> getNodes() {
    return delegate.getNodes();
  }

  @Override
  public void nodeListener(NodeListener listener) {
    delegate.nodeListener(listener);
  }

  @Override
  public void join(Handler<AsyncResult<Void>> resultHandler) {
    delegate.join(resultHandler);
  }

  @Override
  public void leave(Handler<AsyncResult<Void>> resultHandler) {
    delegate.leave(resultHandler);
  }

  @Override
  public boolean isActive() {
    return delegate.isActive();
  }
}
