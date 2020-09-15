package io.github.tsegismont.eblink.vertx4;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

public class EventBusLinkMessage<T> implements Message<T> {

  private final String address;
  private final T body;

  public EventBusLinkMessage(String address, Object body) {
    this.address = address;
    this.body = (T) body;
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public MultiMap headers() {
    return null;
  }

  @Override
  public T body() {
    return body;
  }

  @Override
  public String replyAddress() {
    return null;
  }

  @Override
  public boolean isSend() {
    return true;
  }

  @Override
  public void reply(Object message) {
    // FIXME
    throw new UnsupportedOperationException();
  }

  @Override
  public void reply(Object message, DeliveryOptions options) {
    // FIXME
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> Future<Message<R>> replyAndRequest(Object message, DeliveryOptions options) {
    // FIXME
    throw new UnsupportedOperationException();
  }

  @Override
  public void fail(int failureCode, String message) {
    // FIXME
    throw new UnsupportedOperationException();
  }
}
