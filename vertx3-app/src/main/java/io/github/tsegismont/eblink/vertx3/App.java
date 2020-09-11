package io.github.tsegismont.eblink.vertx3;

import io.netty.util.CharsetUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class App extends AbstractVerticle {

  private static final String HOST = System.getenv().getOrDefault("VERTX_HOST", "127.0.0.1");
  private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("VERTX_PORT", "8080"));
  private static final int LINK_PORT = Integer.parseInt(System.getenv().getOrDefault("VERTX_LINK_PORT", "43652"));
  public static final Buffer PONG = Buffer.buffer(new byte[]{(byte) 1});

  private final CodecManager codecManager;
  private final Map<String, BiConsumer<String, DeliveryOptions>> handlers;

  public App() {
    codecManager = new CodecManager();
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

    vertx.createNetServer()
        .connectHandler(this::handleLink)
        .listen(LINK_PORT, HOST);
  }

  private void handleLink(NetSocket socket) {
    RecordParser parser = RecordParser.newFixed(4);
    Handler<Buffer> handler = new Handler<Buffer>() {
      int size = -1;

      public void handle(Buffer buffer) {
        if (size == -1) {
          size = buffer.getInt(0);
          parser.fixedSizeMode(size);
        } else {

          int pos = 0;
          pos++; // Pass version
          byte systemCodecCode = buffer.getByte(pos);
          pos++;
          MessageCodec codec;
          if (systemCodecCode == -1) {
            // User codec
            int length = buffer.getInt(pos);
            pos += 4;
            byte[] bytes = buffer.getBytes(pos, pos + length);
            String codecName = new String(bytes, CharsetUtil.UTF_8);
            codec = codecManager.getCodec(codecName);
            if (codec == null) {
              throw new IllegalStateException("No message codec registered with name " + codecName);
            }
            pos += length;
          } else {
            codec = codecManager.systemCodecs()[systemCodecCode];
          }
          byte bsend = buffer.getByte(pos);
          boolean send = bsend == 0;
          pos++;
          int length = buffer.getInt(pos);
          pos += 4;
          byte[] bytes = buffer.getBytes(pos, pos + length);
          String address = new String(bytes, CharsetUtil.UTF_8);
          pos += length;
          length = buffer.getInt(pos);
          pos += 4;
          String replyAddress;
          if (length != 0) {
            bytes = buffer.getBytes(pos, pos + length);
            replyAddress = new String(bytes, CharsetUtil.UTF_8);
            pos += length;
          } else {
            replyAddress = null;
          }
          length = buffer.getInt(pos);
          pos += 4;
          pos += length; // Pass sender id
          MultiMap headers;
          int headersPos = pos;
          int headersLength = buffer.getInt(pos);
          if (headersLength != 4) {
            headersPos += 4;
            int numHeaders = buffer.getInt(headersPos);
            headersPos += 4;
            headers = MultiMap.caseInsensitiveMultiMap();
            for (int i = 0; i < numHeaders; i++) {
              int keyLength = buffer.getInt(headersPos);
              headersPos += 4;
              bytes = buffer.getBytes(headersPos, headersPos + keyLength);
              String key = new String(bytes, CharsetUtil.UTF_8);
              headersPos += keyLength;
              int valLength = buffer.getInt(headersPos);
              headersPos += 4;
              bytes = buffer.getBytes(headersPos, headersPos + valLength);
              String val = new String(bytes, CharsetUtil.UTF_8);
              headersPos += valLength;
              headers.add(key, val);
            }
          } else {
            headers = null;
          }
          pos += headersLength;
          int bodyPos = pos;
          Object body = codec.decodeFromWire(pos, buffer);

          parser.fixedSizeMode(4);
          size = -1;
          if (codec == CodecManager.PING_MESSAGE_CODEC) {
            // Just send back pong directly on connection
            socket.write(PONG);
          } else {
            if (headers == null) {
              headers = MultiMap.caseInsensitiveMultiMap();
            }
            if (send || !headers.contains("__vertx.link.relayed")) {
              headers.set("__vertx.link.relayed", "");
              DeliveryOptions options = new DeliveryOptions().setHeaders(headers);
              if (codec.systemCodecID() == -1) {
                options.setCodecName(codec.name());
              }
              if (send) {
                if (replyAddress == null) {
                  vertx.eventBus().send(address, body, options);
                } else {
                  // TODO
                }
              } else {
                vertx.eventBus().publish(address, body, options);
              }
            }
          }
        }
      }
    };
    parser.setOutput(handler);
    socket.handler(parser);
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
