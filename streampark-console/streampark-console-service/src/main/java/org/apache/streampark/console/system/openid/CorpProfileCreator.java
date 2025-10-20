package org.apache.streampark.console.system.openid;


import static org.pac4j.core.profile.definition.CommonProfileDefinition.DISPLAY_NAME;
import static org.pac4j.oidc.profile.OidcProfileDefinition.NICKNAME;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import org.apache.streampark.console.system.service.CorpUserService;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.profile.OidcProfile;
import org.pac4j.oidc.profile.creator.OidcProfileCreator;

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
            userProfile.setRoles(new HashSet<>(Arrays.asList(corpUserService.getUserType(userProfile.getEmail()).name())));
        }
        return userProfileOptional;
    }
}
