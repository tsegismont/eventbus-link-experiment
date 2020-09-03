package io.github.tsegismont.eblink.vertx4;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServerRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class App extends AbstractVerticle {

  private static final String HOST = System.getenv().getOrDefault("VERTX_HOST", "127.0.0.1");
  private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("VERTX_PORT", "8080"));

  private final Map<String, BiConsumer<String, DeliveryOptions>> handlers;

  public App() {
    handlers = new HashMap<>();
    handlers.put("send", this::send);
    handlers.put("request", this::request);
    handlers.put("publish", this::publish);
  }

  @Override
  public void start() {
    consumer("foo");
    consumer(HOST);

    vertx.createHttpServer()
        .requestHandler(this::handleRequest)
        .listen(PORT, HOST);
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
    if (address==null) {
      address = "foo";
    }
    String method = params.get("method");
    if (method==null) {
      method = "send";
    }
    BiConsumer<String, DeliveryOptions> handler = handlers.get(method);
    if (handler!=null) {
      DeliveryOptions options = new DeliveryOptions().addHeader("method", method);
      handler.accept(address, options);
      request.response().end();
    } else {
      request.response().setStatusCode(500).end();
    }
  }

  private void send(String address, DeliveryOptions options) {
    vertx.eventBus().send(address, "toto", options);
    System.out.printf("%s sent message%n", HOST);
  }

  private void request(String address, DeliveryOptions options) {
    vertx.eventBus().request(address, "toto", options, ar -> {
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
    vertx.eventBus().publish(address, "toto", options);
    System.out.printf("%s sent publish%n", HOST);
  }
}
