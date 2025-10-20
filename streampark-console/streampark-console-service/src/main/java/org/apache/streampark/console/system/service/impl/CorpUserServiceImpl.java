/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.system.service.impl;

import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.system.service.CorpUserService;

import org.apache.shiro.authz.AuthorizationException;

import com.netease.music.da.mammut.sdk.service.RoleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
@Service
public class CorpUserServiceImpl implements CorpUserService {

    @Value("${mammut.host:http://mammut-gy.service.163.org}")
    private String mammutHost;
    @Value("${mammut.app.key:49810556-4c7a-40b4-a457-aa8dfdcc9cd9}")
    private String mammutAppKey;
    @Value("${mammut.master.key:63fcff76-1dc5-4c50-9aeb-a5785341714b}")
    private String mammutMasterKey;

    private String mammutProject = "music_das";

    private RoleService roleService;

    private Map<String, Set<String>> userRoleMap;

    @PostConstruct
    public void init() {
        try {
            roleService = new RoleService(mammutHost, mammutAppKey, mammutMasterKey);
            refreshUserRoleMap();
            Thread thread = new Thread(() -> {
                while (!Thread.interrupted()) {
                    try {
                        refreshUserRoleMap();
                        Thread.sleep(1000 * 60 * 10);
                    } catch (Exception e) {
                        log.warn("refresh user roles failed with exception: " + e.getMessage(), e);
                    }
                }
            });
            thread.setName("MammutUserService-RoleSync");
            thread.setDaemon(true);
            thread.start();
        } catch (Exception e) {
            throw new AuthorizationException(e);
        }
    }

    @Override
    public UserTypeEnum getUserType(String email) {
        Set<String> roleList = userRoleMap.get(email);
        if (roleList != null && roleList.contains("管理员")) {
            return UserTypeEnum.ADMIN;
        }
        return UserTypeEnum.USER;
    }

    private void refreshUserRoleMap() throws Exception {
        final Map<String, Set<String>> tmpRoleMap = new ConcurrentHashMap<>();
        roleService.listRolesByAccount(mammutProject, true).stream().forEach(roleDTO -> {
            String roleName = roleDTO.getRoleName();
            roleDTO.getUsers().stream().forEach(user -> {
                String userName = user.getUserEmail();
                tmpRoleMap.computeIfAbsent(userName, k -> new CopyOnWriteArraySet<>());
                tmpRoleMap.get(userName).add(roleName);
            });
        });
        this.userRoleMap = tmpRoleMap;
        log.debug("load user count: " + userRoleMap.size());
    }
}
