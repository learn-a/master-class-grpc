package learn.masterclass.grpc.greeting.client;

import com.proto.greet.GreetManyTimeResponse;
import com.proto.greet.GreetManyTimesRequest;
import com.proto.greet.GreetRequest;
import com.proto.greet.GreetResponse;
import com.proto.greet.Greeting;
import com.proto.greet.GreetingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Iterator;

public class GreetingClient {

  public static void main(String[] args) {
    System.out.println("Hello I am a Grpc Client");
    new GreetingClient().run();
  }

  private void run() {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
        .usePlaintext() // To byPass security for testing
        .build();

    doUnaryCall(channel);
    doServerStreamingCall(channel);
    doClientStreamingCall(channel);

    System.out.println("Shutting down the channel");
    channel.shutdown();
  }

  private void doUnaryCall(ManagedChannel channel) {
    GreetingServiceGrpc.GreetingServiceBlockingStub greetClient = GreetingServiceGrpc
        .newBlockingStub(channel);

    final Greeting greeting = Greeting.newBuilder()
        .setFirstName("Aziz")
        .setLastName("Hararwala")
        .build();

    final GreetRequest greetRequest = GreetRequest.newBuilder().setGreeting(greeting).build();

    final GreetResponse greetResponse = greetClient.greet(greetRequest);
    System.out.printf("Greet Response from server is: %s%n", greetResponse.getResult());
  }


  private void doServerStreamingCall(ManagedChannel channel) {
    GreetingServiceGrpc.GreetingServiceBlockingStub greetClient =
        GreetingServiceGrpc.newBlockingStub(channel);

    final Greeting greeting = Greeting.newBuilder()
        .setFirstName("Aziz")
        .setLastName("Hararwala")
        .build();

    final Iterator<GreetManyTimeResponse> greetManyTimes = greetClient
        .greetManyTimes(GreetManyTimesRequest.newBuilder().setGreeting(greeting).build());

    greetManyTimes.forEachRemaining(response -> System.out.printf("%s%n", response.getResult()));
  }


  private void doClientStreamingCall(ManagedChannel channel) {
  }

}
