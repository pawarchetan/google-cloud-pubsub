package com.gcloud.pubsub;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

@RestController
public class WebAppController {

  @Autowired
  private PubSubApplication.PubsubOutboundGateway messagingGateway;

  @PostMapping("/publishMessage")
  public RedirectView publishMessage(@RequestParam("input") String input) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(getClass().getClassLoader().getResource("ids.csv").getFile())));
    String message;
    while((message = reader.readLine()) !=null ) {
      messagingGateway.sendToPubsub(message);
    }

    return new RedirectView("/");
  }
}
