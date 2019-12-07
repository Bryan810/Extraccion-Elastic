package com.techprimers.elastic.standaloneelasticexample.resource;

import com.techprimers.elastic.standaloneelasticexample.service.ApisService;
import org.apache.wink.json4j.JSONException;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RestController
@RequestMapping("/rest/users")
public class UsersResource {

    TransportClient client;
    private ApisService apisService;
//    private CommentsRepository repository;

    public UsersResource(ApisService apisService) throws UnknownHostException {
        this.apisService = apisService;
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
//        this.repository = repository;
    }

    @GetMapping("/insert/{id}")
    public String insert(@PathVariable final String id) throws IOException {

        IndexResponse response = client.prepareIndex("employee", "id", id)
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "Ajay")
                        .field("salary", 1200)
                        .field("teamName", "Development")
                        .endObject()
                )
                .get();
        return response.getResult().toString();
    }


    @GetMapping("/view/{id}")
    public Map<String, Object> view(@PathVariable final String id) {
        GetResponse getResponse = client.prepareGet("employee", "id", id).get();
        System.out.println(getResponse.getSource());


        return getResponse.getSource();
    }

    @GetMapping("/update/{id}")
    public String update(@PathVariable final String id) throws IOException {

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("employee")
                .type("id")
                .id(id)
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject());
        try {
            UpdateResponse updateResponse = client.update(updateRequest).get();
            System.out.println(updateResponse.status());
            return updateResponse.status().toString();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
        }
        return "Exception";
    }

    @GetMapping("/delete/{id}")
    public String delete(@PathVariable final String id) {

        DeleteResponse deleteResponse = client.prepareDelete("employee", "id", id).get();

        System.out.println(deleteResponse.getResult().toString());
        return deleteResponse.getResult().toString();
    }

    @GetMapping("/comments")
    public void addComment() throws IOException, JSONException {
        int iteracion = 0;
        List<Map<String, Object>> list = apisService.getComments();
        System.out.println("Comentarios: " + list.size());
        if (list.size() > 50000) {
            iteracion = 500000;
        } else {
            List<Map<String, Object>> dups = list;
            for (int i = 0; i < dups.size(); i++) {
                list.add(dups.get(i));
            }
            if (list.size() < 50000) {
                for (int i = 0; i < dups.size(); i++) {
                    list.add(dups.get(i));
                }
            }
            iteracion = 50000;
        }
        System.out.println(list.size());
        if (list.size() > 50000) {
            iteracion = 50000;
        } else {
            iteracion = list.size();
        }
        for (int j = 0; j < 10; j++) {
            int comm = 0;
            LocalDateTime ini = LocalDateTime.now();
            Long init = System.currentTimeMillis();
//            for (int i = 0; i < list.size(); i++) {
            for (int i = 0; i < iteracion; i++) {
                comm++;
                try {
                    Random random = new Random();
                    IndexResponse response = client.prepareIndex("comments", "id", list.get(i).get("id").toString() + random.nextInt(150000))
                            .setSource(jsonBuilder()
                                    .startObject()
                                    .field("text", list.get(i).get("comment").toString())
                                    .field("likes", list.get(i).get("likes"))
                                    .endObject()
                            )
                            .get();
//                    System.out.println(response.status());
                } catch (Exception e) {
                }
            }
            Long end = System.currentTimeMillis();
            LocalDateTime ended = LocalDateTime.now();
            System.out.println("Medición " + (j + 1) + " " + (end - init) + " Comments " + comm);
            long seconds = ChronoUnit.SECONDS.between(ini, ended);
            System.out.println(seconds);
            apisService.deleteCreateIndex();
        }
    }
}
