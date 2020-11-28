package learn.masterclass.grpc.greeting.client;

import com.google.common.collect.Lists;
import com.proto.greet.GreetEveryOneRequest;
import com.proto.greet.GreetEveryOneResponse;
import com.proto.greet.GreetManyTimeResponse;
import com.proto.greet.GreetManyTimesRequest;
import com.proto.greet.GreetRequest;
import com.proto.greet.GreetResponse;
import com.proto.greet.GreetWithDeadlineRequest;
import com.proto.greet.GreetWithDeadlineResponse;
import com.proto.greet.Greeting;
import com.proto.greet.GreetingServiceGrpc;
import com.proto.greet.GreetingServiceGrpc.GreetingServiceBlockingStub;
import com.proto.greet.GreetingServiceGrpc.GreetingServiceStub;
import com.proto.greet.LongGreetRequest;
import com.proto.greet.LongGreetResponse;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GreetingClient {

  public static void main(String[] args) {
    System.out.println("Hello I am a Grpc Client");
    new GreetingClient().run();
  }

  private void run() {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
        .usePlaintext() // To byPass security for testing
        .build();

    try {
//      doUnaryCall(channel);
//      doServerStreamingCall(channel);
//      doClientStreamingCall(channel);
//      doBiDiStreamingCall(channel);
      doUnaryCallWithDeadline(channel);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("Shutting down the channel");
    channel.shutdown();
  }

  private void doUnaryCallWithDeadline(ManagedChannel channel) {
    final GreetingServiceBlockingStub blockingStub =
        GreetingServiceGrpc.newBlockingStub(channel);

    try {
      final GreetWithDeadlineResponse response = blockingStub
          .withDeadline(Deadline.after(2500, TimeUnit.MILLISECONDS))
          .greetWithDeadline(GreetWithDeadlineRequest.newBuilder()
              .setGreeting(Greeting.newBuilder().setFirstName("Aziz").build())
              .build());
      System.out.println(response.getResult());
    } catch (StatusRuntimeException e) {
      if (e.getStatus() == Status.DEADLINE_EXCEEDED) {
        System.out.println("Deadline has been exceeded, we don't want the response");
      } else {
        e.printStackTrace();
      }
    }
  }

  private void doBiDiStreamingCall(ManagedChannel channel) {
    final GreetingServiceStub aysncStub = GreetingServiceGrpc.newStub(channel);
    CountDownLatch latch = new CountDownLatch(1);

    final StreamObserver<GreetEveryOneRequest> requestObserver = aysncStub
        .greetEveryOne(new StreamObserver<GreetEveryOneResponse>() {
          @Override
          public void onNext(GreetEveryOneResponse value) {
            System.out.printf("Response from server=%s%n", value.getResult());
          }

          @Override
          public void onError(Throwable t) {
            latch.countDown();
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        });

    final List<String> firstNames = Lists.newArrayList("Aziz", "Maria", "Matt", "Arwa");

    for (String fn : firstNames) {
      requestObserver.onNext(GreetEveryOneRequest.newBuilder()
          .setGreeting(Greeting.newBuilder().setFirstName(fn).build()).build());
    }
    requestObserver.onCompleted();
    try {
      latch.await(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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


  private void doClientStreamingCall(ManagedChannel channel) throws InterruptedException {
    GreetingServiceGrpc.GreetingServiceStub asyncGreetingClient
        = GreetingServiceGrpc.newStub(channel);

    CountDownLatch latch = new CountDownLatch(1);

    final StreamObserver<LongGreetRequest> requestStreamObserver = asyncGreetingClient
        .longGreet(new StreamObserver<LongGreetResponse>() {
          @Override
          public void onNext(LongGreetResponse value) {
            System.out.println("Received response from server");
            System.out.printf("LongGreetResponse is: %s" + value.getResult());
          }

          @Override
          public void onError(Throwable t) {
            System.out.printf("error occurred: %s", t);
          }

          @Override
          public void onCompleted() {
            System.out.println("Server has completed sending us something");
            latch.countDown();
          }
        });

    System.out.println("Sending request 1");
    requestStreamObserver.onNext(LongGreetRequest.newBuilder()
        .setGreeting(Greeting.newBuilder()
            .setFirstName("Aziz")
            .build())
        .build());
    System.out.println("Sending request 2");
    requestStreamObserver.onNext(LongGreetRequest.newBuilder()
        .setGreeting(Greeting.newBuilder()
            .setFirstName("Jun")
            .build())
        .build());
    System.out.println("Sending request 3");
    requestStreamObserver.onNext(LongGreetRequest.newBuilder()
        .setGreeting(Greeting.newBuilder()
            .setFirstName("Sanyogeeta")
            .build())
        .build());
    // Tell server that the client has finished sending all its request, so that is sends the server response
    // This is how we have implemented out example
    requestStreamObserver.onCompleted();

    latch.await(3L, TimeUnit.SECONDS);

  }

}
