package org.apache.streampark.console.system.service;

import org.apache.streampark.console.core.enums.UserTypeEnum;

public interface CorpUserService {

    UserTypeEnum getUserType(String email);

}
