package io.github.tsegismont.eblink.vertx4;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

public class EventBusLinkMessage<T> implements Message<T> {

  private final EventBusLink eventBusLink;
  private final String replyId;
  private final String address;
  private final T body;

  public EventBusLinkMessage(EventBusLink eventBusLink, String replyId, String address, Object body) {
    this.eventBusLink = eventBusLink;
    this.replyId = replyId;
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
    reply(message, new DeliveryOptions());
  }

  @Override
  public void reply(Object message, DeliveryOptions options) {
    if (replyId != null) {
      eventBusLink.reply(address, replyId, message, options);
    }
  }

  @Override
  public <R> Future<Message<R>> replyAndRequest(Object message, DeliveryOptions options) {
    if (replyId == null) {
      throw new IllegalStateException();
    }
    return eventBusLink.requestAndReply(address, replyId, message, options);
  }

  @Override
  public void fail(int failureCode, String message) {
    if (replyId != null) {
      eventBusLink.reply(address, replyId, new ReplyException(ReplyFailure.RECIPIENT_FAILURE, failureCode, message), null);
    }
  }
}
