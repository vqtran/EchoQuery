import org.apache.log4j.BasicConfigurator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import com.amazon.speech.Sdk;
import com.amazon.speech.speechlet.Speechlet;
import com.amazon.speech.speechlet.servlet.SpeechletServlet;

import echoquery.EchoQuerySpeechlet;

/**
 * Shared launcher for executing all sample skills within a single servlet
 * container.
 */
public final class Launcher {

  private static final int PORT = 8888;
  private static final String HTTPS_SCHEME = "https";

  private Launcher() {}

  /**
   * Configures and sets up a Jetty server.
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    // Configure logging to output to the console with default level of
    // INFO.
    BasicConfigurator.configure();

    Server server = new Server();

    // Configure SSL from system properties.
    SslConnectionFactory sslConnectionFactory = new SslConnectionFactory();
    SslContextFactory sslContextFactory =
        sslConnectionFactory.getSslContextFactory();
    sslContextFactory.setKeyStorePath(
        System.getProperty("javax.net.ssl.keyStore"));
    sslContextFactory.setKeyStorePassword(
        System.getProperty("javax.net.ssl.keyStorePassword"));
    sslContextFactory.setIncludeCipherSuites(Sdk.SUPPORTED_CIPHER_SUITES);

    // Configure HTTPS server.
    HttpConfiguration httpConf = new HttpConfiguration();
    httpConf.setSecurePort(PORT);
    httpConf.setSecureScheme(HTTPS_SCHEME);
    httpConf.addCustomizer(new SecureRequestCustomizer());
    HttpConnectionFactory httpConnectionFactory =
        new HttpConnectionFactory(httpConf);

    // Set up the servlets.
    ServerConnector serverConnector = new ServerConnector(
        server, sslConnectionFactory, httpConnectionFactory);
    serverConnector.setPort(PORT);

    Connector[] connectors = new Connector[1];
    connectors[0] = serverConnector;
    server.setConnectors(connectors);

    ServletContextHandler context =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(
        createServlet(new EchoQuerySpeechlet())), "/echoquery");
    server.start();
    server.join();
  }

  private static SpeechletServlet createServlet(final Speechlet speechlet) {
    SpeechletServlet servlet = new SpeechletServlet();
    servlet.setSpeechlet(speechlet);
    return servlet;
  }
}
