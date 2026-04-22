package io.mitochondria.mapper;

import io.mitochondria.grpc.UserResponse;
import io.mitochondria.model.UserInfo;
import org.springframework.stereotype.Component;

@Component
public class UserMapper {
    public UserResponse toProto(UserInfo userInfo) {
        UserResponse.Builder builder = UserResponse.newBuilder()
                .setEmail(userInfo.getEmail())
                .setName(userInfo.getName());

        //for non-required fields:
        if (userInfo.getAge() != null) {
            builder.setAge(userInfo.getAge());
        }

        if (userInfo.getAddress() != null) {
            builder.setAddress(userInfo.getAddress());
        }

        return builder.build();
    }
}
