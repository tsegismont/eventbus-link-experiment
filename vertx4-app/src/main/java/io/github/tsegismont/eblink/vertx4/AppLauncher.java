package io.github.tsegismont.eblink.vertx4;

import io.vertx.core.VertxOptions;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class AppLauncher extends io.vertx.core.Launcher {

  private static final String HOST = System.getenv("VERTX_HOST");
  private static final Set<String> ADDRESSES;
  private static final String LINK_HOST = System.getenv("VERTX_LINK_HOST");
  private static final int LINK_PORT = Integer.parseInt(System.getenv().getOrDefault("VERTX_LINK_PORT", "43652"));

  static {
    String addresses = System.getenv("VERTX_LINK_ADDRESSES");
    if (addresses!=null) {
      ADDRESSES = Arrays.stream(addresses.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(toSet());
    } else {
      ADDRESSES = Collections.emptySet();
    }
  }

  public static void main(String[] args) {
    new AppLauncher().dispatch(args);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    options.getEventBusOptions().setHost(HOST);
    InfinispanClusterManager clusterManager = new InfinispanClusterManager();
    options.setClusterManager(new EventBusLinkClusterManager(clusterManager, ADDRESSES, LINK_HOST, LINK_PORT));
  }
}
