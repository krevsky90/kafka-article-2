package io.mitochondria.notification.adapter.grpc;

//import java.util.concurrent.TimeUnit;
import io.mitochondria.grpc.UserInfoServiceGrpc;
import io.mitochondria.grpc.UserRequest;
import io.mitochondria.grpc.UserResponse;
import io.mitochondria.notification.domain.UserInfo;
import io.mitochondria.notification.port.UserInfoClient;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;

@Service
public class GrpcUserInfoClient implements UserInfoClient {
    //NOTE: channel name (in the brackets) should be the same as grpc.client.user-service-channel-name.address in application.properties
    @GrpcClient("user-service-channel-name")
    private UserInfoServiceGrpc.UserInfoServiceBlockingStub stub;

    private final UserMapper userMapper;

    public GrpcUserInfoClient(UserMapper userMapper) {
        this.userMapper = userMapper;
    }

    @Override
    public UserInfo getUserInfo(String email) {
        UserRequest userRequest = UserRequest.newBuilder()
                .setEmail(email)
                .build();

        UserResponse userResponse = stub
//                .withDeadlineAfter(2, TimeUnit.SECONDS)
                .getUserInfo(userRequest);

        return userMapper.toDomain(userResponse);
    }
}
