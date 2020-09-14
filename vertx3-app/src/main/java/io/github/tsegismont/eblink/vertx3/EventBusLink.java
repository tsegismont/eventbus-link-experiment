package io.github.tsegismont.eblink.vertx3;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;

import java.util.Set;

public class EventBusLink implements EventBus {

  private final Vertx vertx;
  private final EventBus delegate;
  private final Set<String> addresses;
  private final CodecManager codecManager;
  private final HttpClient httpClient;
  private WebSocket webSocket;

  public EventBusLink(Vertx vertx, Set<String> addresses, String linkHost, int linkPort) {
    this.vertx = vertx;
    this.delegate = vertx.eventBus();
    this.addresses = addresses;
    codecManager = new CodecManager();
    httpClient = vertx.createHttpClient(new HttpClientOptions().setDefaultHost(linkHost).setDefaultPort(linkPort));
  }

  @Override
  public EventBus send(String address, Object message) {
    return send(address, message, new DeliveryOptions());
  }

  @Override
  @Deprecated
  public <T> EventBus send(String address, Object message, Handler<AsyncResult<Message<T>>> replyHandler) {
    return request(address, message, replyHandler);
  }

  @Override
  public EventBus send(String address, Object message, DeliveryOptions options) {
    if (addresses.contains(address)) {
      JsonObject json = createPayload(address, message, options, "send");
      with(ws -> {
        if (ws.succeeded()) {
          ws.result().writeBinaryMessage(json.toBuffer(), ar -> {
            if (ar.failed()) {
              ar.cause().printStackTrace();
            }
          });
        } else {
          ws.cause().printStackTrace();
        }
      });
    } else {
      delegate.send(address, message, options);
    }
    return this;
  }

  private void with(Handler<AsyncResult<WebSocket>> handler) {
    if (webSocket == null) {
      httpClient.webSocket("/", ar -> {
        if (ar.succeeded()) {
          if (webSocket == null) {
            webSocket = ar.result();
            webSocket.closeHandler(v -> webSocket = null);
          } else {
            ar.result().close();
          }
          handler.handle(Future.succeededFuture(webSocket));
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
    }
  }

  private JsonObject createPayload(String address, Object message, DeliveryOptions options, String method) {
    MessageCodec codec = codecManager.lookupCodec(message, options.getCodecName());
    Buffer buffer = Buffer.buffer();
    codec.encodeToWire(buffer, message);
    JsonObject json = new JsonObject()
        .put("method", method)
        .put("address", address)
        .put("options", options.toJson())
        .put("codec", codec.name())
        .put("body", buffer.getBytes());
    return json;
  }

  @Override
  @Deprecated
  public <T> EventBus send(String address, Object message, DeliveryOptions options, Handler<AsyncResult<Message<T>>> replyHandler) {
    return request(address, message, options, replyHandler);
  }

  @Override
  public <T> EventBus request(String address, Object message, Handler<AsyncResult<Message<T>>> replyHandler) {
    return request(address, message, new DeliveryOptions(), replyHandler);
  }

  @Override
  public <T> EventBus request(String address, Object message, DeliveryOptions options, Handler<AsyncResult<Message<T>>> replyHandler) {
    // FIXME
    delegate.request(address, message, options, replyHandler);
    return this;
  }

  @Override
  public EventBus publish(String address, Object message) {
    return publish(address, message, new DeliveryOptions());
  }

  @Override
  public EventBus publish(String address, Object message, DeliveryOptions options) {
    JsonObject json = createPayload(address, message, options, "publish");
    with(ws -> {
      if (ws.succeeded()) {
        ws.result().writeBinaryMessage(json.toBuffer(), ar -> {
          if (ar.failed()) {
            ar.cause().printStackTrace();
          }
        });
      } else {
        ws.cause().printStackTrace();
      }
    });
    delegate.publish(address, message, options);
    return this;
  }

  @Override
  public <T> MessageConsumer<T> consumer(String address) {
    return delegate.consumer(address);
  }

  @Override
  public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {
    return delegate.consumer(address, handler);
  }

  @Override
  public <T> MessageConsumer<T> localConsumer(String address) {
    return delegate.localConsumer(address);
  }

  @Override
  public <T> MessageConsumer<T> localConsumer(String address, Handler<Message<T>> handler) {
    return delegate.localConsumer(address, handler);
  }

  @Override
  public <T> MessageProducer<T> sender(String address) {
    return sender(address, new DeliveryOptions());
  }

  @Override
  public <T> MessageProducer<T> sender(String address, DeliveryOptions options) {
    // FIXME
    return delegate.sender(address, options);
  }

  @Override
  public <T> MessageProducer<T> publisher(String address) {
    return publisher(address, new DeliveryOptions());
  }

  @Override
  public <T> MessageProducer<T> publisher(String address, DeliveryOptions options) {
    // FIXME
    return delegate.publisher(address, options);
  }

  @Override
  public EventBus registerCodec(MessageCodec codec) {
    codecManager.registerCodec(codec);
    delegate.registerCodec(codec);
    return this;
  }

  @Override
  public EventBus unregisterCodec(String name) {
    codecManager.unregisterCodec(name);
    delegate.unregisterCodec(name);
    return this;
  }

  @Override
  public <T> EventBus registerDefaultCodec(Class<T> clazz, MessageCodec<T, ?> codec) {
    codecManager.registerDefaultCodec(clazz, codec);
    delegate.registerDefaultCodec(clazz, codec);
    return this;
  }

  @Override
  public EventBus unregisterDefaultCodec(Class clazz) {
    codecManager.unregisterDefaultCodec(clazz);
    delegate.unregisterDefaultCodec(clazz);
    return this;
  }

  @Override
  public void start(Handler<AsyncResult<Void>> completionHandler) {
    delegate.start(completionHandler);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    delegate.close(completionHandler);
  }

  @Override
  public <T> EventBus addOutboundInterceptor(Handler<DeliveryContext<T>> interceptor) {
    delegate.addOutboundInterceptor(interceptor);
    return this;
  }

  @Override
  public <T> EventBus removeOutboundInterceptor(Handler<DeliveryContext<T>> interceptor) {
    delegate.removeOutboundInterceptor(interceptor);
    return this;
  }

  @Override
  public <T> EventBus addInboundInterceptor(Handler<DeliveryContext<T>> interceptor) {
    delegate.addInboundInterceptor(interceptor);
    return this;
  }

  @Override
  public <T> EventBus removeInboundInterceptor(Handler<DeliveryContext<T>> interceptor) {
    delegate.removeInboundInterceptor(interceptor);
    return this;
  }

  @Override
  public boolean isMetricsEnabled() {
    return delegate.isMetricsEnabled();
  }
}
