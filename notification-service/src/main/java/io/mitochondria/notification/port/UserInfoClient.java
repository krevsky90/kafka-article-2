package io.mitochondria.notification.port;

import io.mitochondria.notification.domain.UserInfo;

public interface UserInfoClient {
    UserInfo getUserInfo(String email);
}
