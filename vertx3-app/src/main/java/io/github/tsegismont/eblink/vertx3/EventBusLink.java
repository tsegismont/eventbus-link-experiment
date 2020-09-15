package io.github.tsegismont.eblink.vertx3;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.core.eventbus.impl.MessageImpl;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class EventBusLink implements EventBus {

  private final Vertx vertx;
  private final EventBus delegate;
  private final Set<String> addresses;
  private final CodecManager codecManager;
  private final HttpClient httpClient;
  private final ConcurrentMap<String, Handler<JsonObject>> replyHandlers;
  private WebSocket webSocket;

  public EventBusLink(Vertx vertx, Set<String> addresses, String host, String linkHost, int linkPort) {
    this.vertx = vertx;
    this.delegate = vertx.eventBus();
    this.addresses = addresses;
    codecManager = new CodecManager();
    httpClient = vertx.createHttpClient(new HttpClientOptions().setDefaultHost(linkHost).setDefaultPort(linkPort));
    vertx.createHttpServer()
        .webSocketHandler(this::handleWebsocketLink)
        .listen(linkPort, host);
    replyHandlers = new ConcurrentHashMap<>();
  }

  private void handleWebsocketLink(ServerWebSocket serverWebSocket) {
    serverWebSocket.binaryMessageHandler(buffer -> {
      JsonObject json = buffer.toJsonObject();
      String method = json.getString("method");
      String address = json.getString("address");
      DeliveryOptions options = new DeliveryOptions(json.getJsonObject("options"));
      String codec = json.getString("codec");
      Buffer body = Buffer.buffer(json.getBinary("body"));
      MessageCodec messageCodec = Arrays.stream(new CodecManager().systemCodecs()).filter(mc -> mc.name().equals(codec)).findFirst().get();
      Object msg = messageCodec.decodeFromWire(0, body);
      if (method.equals("send")) {
        vertx.eventBus().send(address, msg, options);
      } else if (method.equals("request")) {
        String replyId = json.getString("replyId");
        vertx.eventBus().request(address, msg, options, ar -> {
          JsonObject reply;
          if (ar.succeeded()) {
            reply = createReplyPayload((MessageImpl) ar.result(), replyId);
          } else {
            reply = createReplyPayload((ReplyException) ar.cause(), replyId);
          }
          send(serverWebSocket, reply);
        });
      } else if (method.equals("publish")) {
        vertx.eventBus().publish(address, msg, options);
      }
    });
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
      send(json);
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
            webSocket.closeHandler(v -> webSocket = null).binaryMessageHandler(buffer -> {
              JsonObject json = buffer.toJsonObject();
              String replyId = json.getString("replyId");
              Handler<JsonObject> h = replyHandlers.remove(replyId);
              if (h != null) {
                h.handle(json);
              }
            });
          } else {
            ar.result().close();
          }
          handler.handle(Future.succeededFuture(webSocket));
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
    } else {
      handler.handle(Future.succeededFuture(webSocket));
    }
  }

  private JsonObject createPayload(String address, Object message, DeliveryOptions options, String method) {
    return createPayload(address, message, options, method, null);
  }

  private JsonObject createPayload(String address, Object message, DeliveryOptions options, String method, String replyId) {
    MessageCodec codec = codecManager.lookupCodec(message, options.getCodecName());
    Buffer buffer = Buffer.buffer();
    codec.encodeToWire(buffer, message);
    JsonObject json = new JsonObject()
        .put("method", method)
        .put("address", address)
        .put("options", options.toJson())
        .put("codec", codec.name())
        .put("body", buffer.getBytes());
    if (replyId != null) {
      json.put("replyId", replyId);
    }
    return json;
  }

  private JsonObject createReplyPayload(MessageImpl message, String replyId) {
    MessageCodec codec = message.codec();
    Buffer buffer = Buffer.buffer();
    codec.encodeToWire(buffer, message.body());
    JsonObject json = new JsonObject()
        .put("replyId", replyId)
        .put("codec", codec.name())
        .put("body", buffer.getBytes());
    return json;
  }

  private JsonObject createReplyPayload(ReplyException e, String replyId) {
    MessageCodec codec = CodecManager.REPLY_EXCEPTION_MESSAGE_CODEC;
    Buffer buffer = Buffer.buffer();
    codec.encodeToWire(buffer, e);
    JsonObject json = new JsonObject()
        .put("replyId", replyId)
        .put("failure", true)
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
    if (addresses.contains(address)) {
      String replyId = UUID.randomUUID().toString();
      replyHandlers.put(replyId, json -> {
        String codec = json.getString("codec");
        Buffer body = Buffer.buffer(json.getBinary("body"));
        if (json.getBoolean("failure") == Boolean.TRUE) {
          ReplyException e = CodecManager.REPLY_EXCEPTION_MESSAGE_CODEC.decodeFromWire(0, body);
          replyHandler.handle(Future.failedFuture(e));
        } else {
          MessageCodec messageCodec = Arrays.stream(new CodecManager().systemCodecs()).filter(mc -> mc.name().equals(codec)).findFirst().get();
          Object msg = messageCodec.decodeFromWire(0, body);
          replyHandler.handle(Future.succeededFuture(new EventBusLinkMessage<>(address, msg)));
        }
      });
      JsonObject json = createPayload(address, message, options, "request", replyId);
      send(json);
      vertx.setTimer(options.getSendTimeout(), l -> replyHandlers.remove(replyId));
    } else {
      delegate.request(address, message, options, replyHandler);
    }
    return this;
  }

  private void send(ServerWebSocket serverWebSocket, JsonObject json) {
    serverWebSocket.writeBinaryMessage(json.toBuffer(), war -> {
      if (war.failed()) {
        war.cause().printStackTrace();
      }
    });
  }

  private void send(JsonObject json) {
    with(ws -> {
      if (ws.succeeded()) {
        ws.result().writeBinaryMessage(json.toBuffer(), war -> {
          if (war.failed()) {
            war.cause().printStackTrace();
          }
        });
      } else {
        ws.cause().printStackTrace();
      }
    });
  }

  @Override
  public EventBus publish(String address, Object message) {
    return publish(address, message, new DeliveryOptions());
  }

  @Override
  public EventBus publish(String address, Object message, DeliveryOptions options) {
    JsonObject json = createPayload(address, message, options, "publish");
    send(json);
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
