package com.onkar.keycloak.demo.controller;


import org.keycloak.representations.AccessToken;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.logging.Logger;


@RestController
public class DemoController {

    @Autowired
    private AccessToken accessToken;

    private static final Logger LOGGER = Logger.getLogger(DemoController.class.getName());

    //@PreAuthorize("hasRole('admin')")
    @GetMapping("/admin/hello")
    //@Secured("admin")
    public String sayHelloToAdmin() {
        LOGGER.info("AccessToken : " + accessToken);
        return "Hello Admin";
    }

    //@PreAuthorize("hasRole('user')")
    @GetMapping("/user/hello")
    //@Secured("user")
    public String sayHelloToUser() {
        return "Hello User";
    }

    @GetMapping("/open")
    public String welcome() {
        return "It Works !!";
    }

}
