package io.mitochondria.service;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.mitochondria.exception.UserNotFoundException;
import io.mitochondria.grpc.UserInfoServiceGrpc;
import io.mitochondria.grpc.UserRequest;
import io.mitochondria.grpc.UserResponse;
import io.mitochondria.mapper.UserMapper;
import io.mitochondria.model.UserInfo;
import net.devh.boot.grpc.server.service.GrpcService;

//transport that calls business logic
@GrpcService
public class UserInfoGrpcService extends UserInfoServiceGrpc.UserInfoServiceImplBase {
    private final UserInfoService userInfoService;
    private final UserMapper userMapper;

    public UserInfoGrpcService(UserInfoService userInfoService, UserMapper userMapper) {
        this.userInfoService = userInfoService;
        this.userMapper = userMapper;
    }

    @Override
    public void getUserInfo(UserRequest userRequest, StreamObserver<UserResponse> observer) {
        String email = userRequest.getEmail();
        try {
            UserInfo userInfo = userInfoService.getUserInfoByEmail(email);
            UserResponse response = userMapper.toProto(userInfo);

            observer.onNext(response);
            observer.onCompleted();
        } catch (UserNotFoundException ex) {
            //NOTE: instead of throwing of exception
            // best practice: Service throws domain exceptions
            // gRPC layer maps it to Status
            observer.onError(Status.NOT_FOUND
                    .withDescription("User not found: " + email)
                    .asRuntimeException());
        }
    }
}