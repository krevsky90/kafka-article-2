package io.mitochondria.notification.adapter.grpc;

import io.mitochondria.grpc.UserResponse;
import io.mitochondria.notification.domain.UserInfo;
import org.springframework.stereotype.Component;

@Component
public class UserMapper {
    public UserInfo toDomain(UserResponse userResponse) {
        return new UserInfo(
                userResponse.getEmail(),
                userResponse.getName(),
                userResponse.getAge(),
                userResponse.getAddress()
        );
    }
}
