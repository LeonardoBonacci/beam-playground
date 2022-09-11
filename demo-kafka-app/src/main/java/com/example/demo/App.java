package com.example.demo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class App {

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}
	
	@Bean
  CommandLineRunner demo(KafkaTemplate<String, String> kafkaTemplate) {
    return args -> {
      for (int i=0; i<10; i++) 
        kafkaTemplate.send("foo", "bar" + i).get();
      System.out.println("done");
      System.exit(0);
    };
  }
}
