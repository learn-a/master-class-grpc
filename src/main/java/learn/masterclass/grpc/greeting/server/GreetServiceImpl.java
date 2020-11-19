package learn.masterclass.grpc.greeting.server;

import com.proto.greet.GreetManyTimeResponse;
import com.proto.greet.GreetManyTimesRequest;
import com.proto.greet.GreetRequest;
import com.proto.greet.GreetResponse;
import com.proto.greet.Greeting;
import com.proto.greet.GreetingServiceGrpc.GreetingServiceImplBase;
import com.proto.greet.LongGreetRequest;
import com.proto.greet.LongGreetResponse;
import io.grpc.stub.StreamObserver;
import java.util.stream.IntStream;

public class GreetServiceImpl extends GreetingServiceImplBase {

  // Example of Unary response (onNext() multiple times has no impact)
  @Override
  public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
    final Greeting greeting = request.getGreeting();
    final String firstName = greeting.getFirstName();

    String result = "Hello " + firstName;

    // Create response
    GreetResponse response = GreetResponse.newBuilder()
        .setResult(result).build();

    // Send the response
    responseObserver.onNext(response);
    // Complete the rpc call
    responseObserver.onCompleted();
  }

  // Example of Streaming response
  @Override
  public void greetManyTimes(GreetManyTimesRequest request,
      StreamObserver<GreetManyTimeResponse> responseObserver) {
    final Greeting greeting = request.getGreeting();
    final String firstName = greeting.getFirstName();

    IntStream.range(0, 11).forEach(num -> {
      try {
        String result = "Hello : " + firstName + ", response num: " + num;
        final GreetManyTimeResponse response = GreetManyTimeResponse.newBuilder().setResult(result)
            .build();
        responseObserver.onNext(response);
        Thread.sleep(1000l);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });

    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<LongGreetRequest> longGreet(
      StreamObserver<LongGreetResponse> responseObserver) {

    StreamObserver<LongGreetRequest> requestStreamObserver = new StreamObserver<LongGreetRequest>() {
      private String result = "";
      @Override
      public void onNext(LongGreetRequest value) {
        result += "Hello: " + value.getGreeting().getFirstName() + " \n";
      }

      @Override
      public void onError(Throwable t) {
        // Ignore for now
      }

      @Override
      public void onCompleted() {
        responseObserver.onNext(LongGreetResponse.newBuilder().setResult(result).build());
        responseObserver.onCompleted();
      }
    };

    return requestStreamObserver;
  }
}
