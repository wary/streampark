package org.apache.streampark.console.system.service.impl;

import com.netease.music.da.mammut.sdk.service.RoleService;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.system.service.CorpUserService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CorpUserServiceImpl implements CorpUserService {

    @Value("${mammut.host:http://mammut-gy.service.163.org}")
    private String mammutHost;
    @Value("${mammut.app.key:49810556-4c7a-40b4-a457-aa8dfdcc9cd9}")
    private String mammutAppKey;
    @Value("${mammut.master.key:63fcff76-1dc5-4c50-9aeb-a5785341714b}C")
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
