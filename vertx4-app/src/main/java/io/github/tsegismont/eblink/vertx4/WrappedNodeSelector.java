package io.github.tsegismont.eblink.vertx4;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class WrappedNodeSelector implements NodeSelector {

  private final NodeSelector delegate;
  private final Set<String> addresses;
  private final RegistrationInfo fakeRegistration;

  public WrappedNodeSelector(NodeSelector delegate, Set<String> addresses, RegistrationInfo fakeRegistration) {
    this.delegate = delegate;
    this.addresses = addresses;
    this.fakeRegistration = fakeRegistration;
  }

  @Override
  public void init(Vertx vertx, ClusterManager clusterManager) {
    delegate.init(vertx, clusterManager);
  }

  @Override
  public void eventBusStarted() {
    delegate.eventBusStarted();
  }

  @Override
  public void selectForSend(Message<?> message, Promise<String> promise) {
    delegate.selectForSend(message, promise);
  }

  @Override
  public void selectForPublish(Message<?> message, Promise<Iterable<String>> promise) {
    delegate.selectForPublish(message, promise);
  }

  @Override
  public void registrationsUpdated(RegistrationUpdateEvent registrationUpdateEvent) {
    String address = registrationUpdateEvent.address();
    if (addresses.contains(address)) {
      List<RegistrationInfo> actual = registrationUpdateEvent.registrations();
      List<RegistrationInfo> registrations = new ArrayList<>(actual.size() + 1);
      registrations.addAll(actual);
      registrations.add(fakeRegistration);
      delegate.registrationsUpdated(new RegistrationUpdateEvent(address, registrations));
    } else {
      delegate.registrationsUpdated(registrationUpdateEvent);
    }
  }

  @Override
  public void registrationsLost() {
    delegate.registrationsLost();
  }
}
