package io.github.tsegismont.eblink.vertx4;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;

import java.util.*;
import java.util.function.BiConsumer;

import static java.util.stream.Collectors.toSet;

public class App extends AbstractVerticle {

  private static final String HOST = System.getenv().getOrDefault("VERTX_HOST", "127.0.0.1");
  private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("VERTX_PORT", "8080"));
  private static final String LINK_HOST = System.getenv("VERTX_LINK_HOST");
  private static final int LINK_PORT = Integer.parseInt(System.getenv().getOrDefault("VERTX_LINK_PORT", "43652"));
  private static final Set<String> ADDRESSES;

  static {
    String addresses = System.getenv("VERTX_LINK_ADDRESSES");
    if (addresses != null) {
      ADDRESSES = Arrays.stream(addresses.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(toSet());
    } else {
      ADDRESSES = Collections.emptySet();
    }
  }

  private final Map<String, BiConsumer<String, DeliveryOptions>> handlers;

  private EventBus eventBus;

  public App() {
    handlers = new HashMap<>();
    handlers.put("send", this::send);
    handlers.put("request", this::request);
    handlers.put("publish", this::publish);
    handlers.put("pingPong", this::pingPong);
  }

  @Override
  public void start() {
    eventBus = wrapEventBus();

    consumer("foo");
    consumer(HOST);
    vertx.eventBus().consumer("pingPong", this::handlePing);

    vertx.createHttpServer()
        .requestHandler(this::handleRequest)
        .listen(PORT, HOST);
  }

  private void handlePing(Message<Integer> msg) {
    int value = msg.body();
    System.out.printf("%s received message on address %s%n%s%n", HOST, "pingPong", value);
    if (value < 10) {
      msg.<Integer>replyAndRequest(value, ar -> {
        if (ar.succeeded()) {
          handlePing(ar.result());
        } else {
          System.out.printf("%s received failure%n", HOST);
          ar.cause().printStackTrace();
        }
      });
    } else {
      msg.reply(value);
    }
    System.out.printf("%s has replied%n", HOST);
  }

  private EventBus wrapEventBus() {
    return new EventBusLink(vertx, ADDRESSES, HOST, LINK_HOST, LINK_PORT);
  }

  private void consumer(String address) {
    vertx.eventBus().consumer(address, msg -> {
      System.out.printf("%s received message on address %s%n%s%n", HOST, address, msg.body());
      if (msg.headers().get("method").equals("request")) {
        msg.reply(HOST);
        System.out.printf("%s has replied%n", HOST);
      }
    });
  }

  private void handleRequest(HttpServerRequest request) {
    MultiMap params = request.params();
    String address = params.get("address");
    if (address == null) {
      address = "foo";
    }
    String method = params.get("method");
    if (method == null) {
      method = "send";
    }
    BiConsumer<String, DeliveryOptions> handler = handlers.get(method);
    if (handler != null) {
      DeliveryOptions options = new DeliveryOptions().setSendTimeout(Long.MAX_VALUE).addHeader("method", method);
      handler.accept(address, options);
      request.response().end();
    } else {
      request.response().setStatusCode(500).end();
    }
  }

  private void send(String address, DeliveryOptions options) {
    eventBus.send(address, "toto", options);
    System.out.printf("%s sent message%n", HOST);
  }

  private void request(String address, DeliveryOptions options) {
    eventBus.request(address, "toto", options, ar -> {
      if (ar.succeeded()) {
        System.out.printf("%s received reply from %s%n", HOST, ar.result().body());
      } else {
        System.out.printf("%s received failure%n", HOST);
        ar.cause().printStackTrace();
      }
    });
    System.out.printf("%s sent request%n", HOST);
  }

  private void publish(String address, DeliveryOptions options) {
    eventBus.publish(address, "toto", options);
    System.out.printf("%s sent publish%n", HOST);
  }

  private void pingPong(String address, DeliveryOptions options) {
    eventBus.request(address, 0, options, this::pongHandler);
    System.out.printf("%s sent ping%n", HOST);
  }

  private void pongHandler(AsyncResult<Message<Integer>> ar) {
    if (ar.succeeded()) {
      Message<Integer> message = ar.result();
      int value = message.body();
      System.out.printf("%s received pong %d%n", HOST, value);
      if (value < 10) {
        message.replyAndRequest(value + 1, this::pongHandler);
      } else {
        message.reply(value + 1);
      }
    } else {
      System.out.printf("%s received failure%n", HOST);
      ar.cause().printStackTrace();
    }
  }
}
