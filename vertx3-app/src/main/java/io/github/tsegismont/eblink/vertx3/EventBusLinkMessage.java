package io.github.tsegismont.eblink.vertx3;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
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
  public <R> void reply(Object message, Handler<AsyncResult<Message<R>>> replyHandler) {
    reply(message, new DeliveryOptions(), replyHandler);
  }

  @Override
  public void reply(Object message, DeliveryOptions options) {
    if (replyId != null) {
      eventBusLink.reply(address, replyId, message, options);
    }
  }

  @Override
  public <R> void reply(Object message, DeliveryOptions options, Handler<AsyncResult<Message<R>>> replyHandler) {
    if (replyId == null) {
      throw new IllegalStateException();
    }
    eventBusLink.requestAndReply(address, replyId, message, options, replyHandler);
  }

  @Override
  public void fail(int failureCode, String message) {
    if (replyId != null) {
      eventBusLink.reply(address, replyId, new ReplyException(ReplyFailure.RECIPIENT_FAILURE, failureCode, message), null);
    }
  }
}
