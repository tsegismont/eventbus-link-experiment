package io.github.tsegismont.eblink.vertx3;

public class AppLauncher extends io.vertx.core.Launcher {

  public static void main(String[] args) {
    new AppLauncher().dispatch(args);
  }
}
