package com.techprimers.elastic.standaloneelasticexample.service;

import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;


@Service
public class ApisService {

    TransportClient client;
    /**
     * Videos
     */
    private final String GET_VIDEOS_FROM_CHANNEL = "https://www.googleapis.com/youtube/v3/search?order=date&part=id,snippet&fields=items(id(videoId),snippet)&channelId=";
    //private final String MAX_RESULTS_50 = "&maxResults=1";
    private final String MAX_RESULTS_50 = "&maxResults=50";
    private final String KEY = "&key=";

    /**
     * Comments
     */
    private final String GET_COMMENTS_FROM_VIDEO = "https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId=";
    private final String MAX_RESULTS_100 = "&maxResults=100";
//    private final String MAX_RESULTS_100 = "&maxResults=50";


    public List<String> getVideos() throws JSONException {
        List<String> canales = new ArrayList<>();
        List<String> Tokens = new ArrayList<>();
        List<String> videosList = new ArrayList<>();
        canales.add("UCDIx8OQ3x4Guh0HXDdRPbvQ"); //----49 David Petit
        canales.add("UCYWOjHweP2V-8kGKmmAmQJQ"); //----49 PewDiePie
        canales.add("UCiBwOVBljEjcsLUjwI274Xw"); //----49 TopComics
        canales.add("UCAhgel1QD4p1aJYzstyQDpg"); //----49 VoxPopuli
        canales.add("UCns-8DssCBba7M4nu7wk7Aw"); //----49 Date un Voltio
        canales.add("UCH-Z8ya93m7_RD02WsCSZYA"); //----49 Derivando
        canales.add("UCo5HJNjfdSoPWsdAHLsvSxQ"); //----49 Shauntrack
        canales.add("UC6107grRI4m0o2-emgoDnAA"); //----49 SmarterEveryDay
        canales.add("UCS-4NlbHYyfL8z9FiZ7NQ0g"); //----49 LooperEspañol
        canales.add("UCDZsyOkn-WTiTwgAvZSQ_cg"); //----49 RinconGeorgio
        canales.add("UCcP6Vtkc2DtQ0IV9r_x6pCQ"); //----49 DicoveryChannel
        canales.add("UCdu8QrpJd6rdHU9fHl8J01A"); //----49 HopeForPawns
        canales.add("UCDZhJlgKdsdk_5bodS7Gm7A"); //----49 NancyRisol
        canales.add("UCaVPhFg-Ax873wvhbNitsrQ"); //----49 Robot De Platon
        canales.add("UCMqZR03pOWgKhckssbV8YZw"); //----49 Cinescondite
        canales.add("UCMb_jTYA_oFqJVJneNUH0Zg"); //----49 Locos Por el asado
        canales.add("UCECJDeK0MNapZbpaOzxrUPA"); //----49 Luisito COmunica
        canales.add("UCbdSYaPD-lr1kW27UJuk8Pw"); //----49 QuantumFracture
        canales.add("UCPRUgAl_MV9PajsrG_BmT9w"); //----49 BuzzFeed
        canales.add("UCsB1iUrIcnNZ9pUAHs3oArQ"); //----49 Cuby
        canales.add("UCsT0YIqwnpJCM-mx7-gSA4Q"); //----49 Tedx
        canales.add("UCEwl20VxZ3AwOgiKMZtI1GQ"); //----49 La capital
        canales.add("UCbrd1vu4_7qIE6IPV_dA-OA"); //----49 Genial
        canales.add("UCfsOfLvadg89Bx8Sv_6WERg"); //----49 NoobFromUa
        canales.add("UCqTVbsEw73sR26xyJQMmceQ"); //----49 DrainAddcit
        //25 canales
        Tokens.add("AIzaSyAUhAF6Uk3yzKgTR5xoRA8q2BEbsIOUOLI");
        Tokens.add("AIzaSyC5sXgzSiNduBvFMtGYUbKqWv5zyc03doc");
        Tokens.add("AIzaSyBGiE__yTrVkH5E3NJrsqXB-uLGwOWPbEE");
        Tokens.add("AIzaSyCuGPJoFt7lAj9Aq4Qr7hdr5uYWHFnUvH0");
        Tokens.add("AIzaSyCxclWh0xvULyiHtlnAI6VY-XgPFlJ58Qk");
        Tokens.add("AIzaSyBW_wSnT86DF5VHs3zXnTxsREZGrss83b8");
        Tokens.add("AIzaSyAYCobgwQq6esiOC0m0RWMWp9Asi2EBJV4");
        Tokens.add("AIzaSyADhOuZgmZyweNl6CaxWhKFtr7fg4R66xg");
        Tokens.add("AIzaSyCQhqa75JTGIayzlToLF_NLv80RT7kU_z8");
        Tokens.add("AIzaSyCCMOxivWjRihOVdCQPSAK1qejhhve5lAA");
        Tokens.add("AIzaSyCaii4uSPYdBwpSIZUPnYngR1o-Ap4nUPg");
        Tokens.add("AIzaSyCyWadqDDyn0LoTxpF8EHTJVAwhyvA8C7o");
        for (int k = 0; k < canales.size(); k++) {
            Random random = new Random();
            RestTemplate restTemplateVideos = new RestTemplate();
            String urlRequest = GET_VIDEOS_FROM_CHANNEL + canales.get(k) + MAX_RESULTS_50 + KEY + Tokens.get(random.nextInt(Tokens.size()));
            ResponseEntity<JSONObject> response = restTemplateVideos.getForEntity(urlRequest, JSONObject.class);
            JSONArray vids = response.getBody().getJSONArray("items");
            for (int i = 0; i < vids.length(); i++) {
                if (vids.getJSONObject(i).has("id")) {
                    videosList.add(vids.getJSONObject(i).getJSONObject("id").getString("videoId"));
                }
//                else {
//                    System.out.println("No habia ID");
//                }
            }
        }
        return videosList;
    }

    public List<Map<String, Object>> getComments() throws JSONException, IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        try {
            List<String> Tokens = new ArrayList<>();
            Tokens.add("AIzaSyAUhAF6Uk3yzKgTR5xoRA8q2BEbsIOUOLI");
            Tokens.add("AIzaSyC5sXgzSiNduBvFMtGYUbKqWv5zyc03doc");
            Tokens.add("AIzaSyBGiE__yTrVkH5E3NJrsqXB-uLGwOWPbEE");
            Tokens.add("AIzaSyCuGPJoFt7lAj9Aq4Qr7hdr5uYWHFnUvH0");
            Tokens.add("AIzaSyCxclWh0xvULyiHtlnAI6VY-XgPFlJ58Qk");
            Tokens.add("AIzaSyBW_wSnT86DF5VHs3zXnTxsREZGrss83b8");
            Tokens.add("AIzaSyAYCobgwQq6esiOC0m0RWMWp9Asi2EBJV4");
            Tokens.add("AIzaSyADhOuZgmZyweNl6CaxWhKFtr7fg4R66xg");
            Tokens.add("AIzaSyCQhqa75JTGIayzlToLF_NLv80RT7kU_z8");
            Tokens.add("AIzaSyCCMOxivWjRihOVdCQPSAK1qejhhve5lAA");
            Tokens.add("AIzaSyCaii4uSPYdBwpSIZUPnYngR1o-Ap4nUPg");
            Tokens.add("AIzaSyCyWadqDDyn0LoTxpF8EHTJVAwhyvA8C7o");

            List<String> videos = getVideos();
            System.out.println("Total Videos");
            System.out.println(videos.size());
            for (int i = 0; i < videos.size(); i++) {
                Random random = new Random();
                RestTemplate restTemplateUtube = new RestTemplate();
                String urlRequest = GET_COMMENTS_FROM_VIDEO + videos.get(i) + MAX_RESULTS_100 + KEY + Tokens.get(random.nextInt(Tokens.size()));
                ResponseEntity<JSONObject> response = restTemplateUtube.getForEntity(urlRequest, JSONObject.class);
                JSONArray comments = response.getBody().getJSONArray("items");

                for (int j = 0; j < comments.length(); j++) {
                    Map<String, Object> mapita = new HashMap<>();
                    mapita.put("id", comments.getJSONObject(j).getString("id"));
                    mapita.put("comment", comments.getJSONObject(j).getJSONObject("snippet").getJSONObject("topLevelComment").getJSONObject("snippet").getString("textDisplay"));
                    mapita.put("likes", Integer.parseInt(comments.getJSONObject(j).getJSONObject("snippet").getJSONObject("topLevelComment").getJSONObject("snippet").getString("likeCount")));
                    data.add(mapita);
                }

            }
//        System.out.println("data");
//        System.out.println(data);

        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
        return data;
    }

    public void deleteCreateIndex() {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.delete("http://localhost:9200/*", String.class);
        String requestJson = "{\n" +
                "    \"mappings\": {\n" +
                "        \"comments\": {\n" +
                "            \"properties\": {\n" +
                "                \"likes\": {\n" +
                "                    \"type\": \"integer\"\n" +
                "                },\n" +
                "                \"comment\": {\n" +
                "                    \"type\": \"text\"\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<String>(requestJson, headers);
        restTemplate.put("http://localhost:9200/comments", entity);
    }
}
