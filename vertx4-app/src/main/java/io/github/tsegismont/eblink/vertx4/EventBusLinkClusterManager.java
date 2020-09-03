package io.github.tsegismont.eblink.vertx4;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;

import java.util.*;

public class EventBusLinkClusterManager implements ClusterManager {

  private final ClusterManager delegate;
  private final Set<String> addresses;
  private final RegistrationInfo fakeRegistration;

  public EventBusLinkClusterManager(ClusterManager delegate, Set<String> addresses, String host, int port) {
    this.delegate = Objects.requireNonNull(delegate);
    this.addresses = Objects.requireNonNull(addresses);
    fakeRegistration = new RegistrationInfo("__vertx.eventbus.link.fake.node", 0, false);
    Objects.requireNonNull(host);
  }

  @Override
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    delegate.init(vertx, new WrappedNodeSelector(nodeSelector, addresses, fakeRegistration));
  }

  @Override
  public <K, V> void getAsyncMap(String s, Promise<AsyncMap<K, V>> promise) {
    delegate.getAsyncMap(s, promise);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String s) {
    return delegate.getSyncMap(s);
  }

  @Override
  public void getLockWithTimeout(String s, long l, Promise<Lock> promise) {
    delegate.getLockWithTimeout(s, l, promise);
  }

  @Override
  public void getCounter(String s, Promise<Counter> promise) {
    delegate.getCounter(s, promise);
  }

  @Override
  public String getNodeId() {
    return delegate.getNodeId();
  }

  @Override
  public List<String> getNodes() {
    return delegate.getNodes();
  }

  @Override
  public void nodeListener(NodeListener nodeListener) {
    delegate.nodeListener(nodeListener);
  }

  @Override
  public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
    delegate.setNodeInfo(nodeInfo, promise);
  }

  @Override
  public NodeInfo getNodeInfo() {
    return delegate.getNodeInfo();
  }

  @Override
  public void getNodeInfo(String s, Promise<NodeInfo> promise) {
    delegate.getNodeInfo(s, promise);
  }

  @Override
  public void join(Promise<Void> promise) {
    delegate.join(promise);
  }

  @Override
  public void leave(Promise<Void> promise) {
    delegate.leave(promise);
  }

  @Override
  public boolean isActive() {
    return delegate.isActive();
  }

  @Override
  public void addRegistration(String s, RegistrationInfo registrationInfo, Promise<Void> promise) {
    delegate.addRegistration(s, registrationInfo, promise);
  }

  @Override
  public void removeRegistration(String s, RegistrationInfo registrationInfo, Promise<Void> promise) {
    delegate.removeRegistration(s, registrationInfo, promise);
  }

  @Override
  public void getRegistrations(String s, Promise<List<RegistrationInfo>> promise) {
    if (addresses.contains(s)) {
      Promise<List<RegistrationInfo>> p = Promise.promise();
      delegate.getRegistrations(s, p);
      p.future().map(actual -> {
        List<RegistrationInfo> registrations = new ArrayList<>(actual.size() + 1);
        registrations.addAll(actual);
        registrations.add(fakeRegistration);
        return registrations;
      }).onComplete(promise);
    } else {
      delegate.getRegistrations(s, promise);
    }
  }

  @Override
  public String clusterHost() {
    return delegate.clusterHost();
  }

  @Override
  public String clusterPublicHost() {
    return delegate.clusterPublicHost();
  }
}
