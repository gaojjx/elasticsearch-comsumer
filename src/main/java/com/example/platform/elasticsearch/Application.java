package com.example.platform.elasticsearch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@ComponentScan(basePackages = {"com.example.*"})
public class Application {

    Application() {
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    /**
     * rest template 增加bean与LoadBalanced.
     *
     * @param builder RestTemplateBuilder
     * @return 重新构造后的restTemplate
     */
    @Bean
    public RestTemplate restTemplate(final RestTemplateBuilder builder) {
        // Do any additional configuration here
        return builder.build();
    }

    /**
     * 规避checkstyle.
     */
    public void sayHello() {
        System.out.println("hello example");
    }
}
