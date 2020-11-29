package learn.masterclass.grpc.greeting.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

public class GreetingServer {

  public static void main(String[] args) throws Exception {
    System.out.println("Hello Grpc Server");

    Server server;
    server = ServerBuilder.forPort(50051)
        .addService(new GreetServiceImpl())
        .addService(ProtoReflectionService.newInstance())
        .build();

    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Received Shutdown Request");
      server.shutdown();
      System.out.println("Successfully stopped the server");
    }
    ));

    server.awaitTermination();
  }

}
