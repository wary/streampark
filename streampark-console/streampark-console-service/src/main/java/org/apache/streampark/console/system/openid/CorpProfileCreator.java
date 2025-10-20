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

package org.apache.streampark.console.system.openid;

import org.apache.streampark.console.system.service.CorpUserService;

import org.pac4j.core.context.WebContext;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.profile.OidcProfile;
import org.pac4j.oidc.profile.creator.OidcProfileCreator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

import static org.pac4j.core.profile.definition.CommonProfileDefinition.DISPLAY_NAME;
import static org.pac4j.oidc.profile.OidcProfileDefinition.NICKNAME;

public class CorpProfileCreator extends OidcProfileCreator {

    private CorpUserService corpUserService;

    public CorpProfileCreator(OidcConfiguration configuration,
                              OidcClient client,
                              CorpUserService corpUserService) {
        super(configuration, client);
        this.corpUserService = corpUserService;
    }

    @Override
    public Optional<UserProfile> create(OidcCredentials credentials, WebContext context) {
        Optional<UserProfile> userProfileOptional = super.create(credentials, context);
        if (userProfileOptional.isPresent()) {
            OidcProfile userProfile = (OidcProfile) userProfileOptional.get();
            String name = userProfile.getAttribute("name").toString();
            userProfile.addAttribute(NICKNAME, name);
            userProfile.addAttribute(DISPLAY_NAME, name);
            userProfile
                .setRoles(new HashSet<>(Arrays.asList(corpUserService.getUserType(userProfile.getEmail()).name())));
        }
        return userProfileOptional;
    }
}
