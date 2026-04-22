package io.mitochondria.service;

import io.mitochondria.exception.UserNotFoundException;
import io.mitochondria.model.UserInfo;
import io.mitochondria.repository.UserInfoRepository;
import org.springframework.stereotype.Service;

//business logic - does not depend on GRPC or any other transport!
@Service
public class UserInfoService {
    private final UserInfoRepository repository;

    public UserInfoService(UserInfoRepository repository) {
        this.repository = repository;
    }

    public UserInfo getUserInfoByEmail(String email) {
        return repository.findById(email).orElseThrow(() -> new UserNotFoundException("User not found: " + email));
    }
}
