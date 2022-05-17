package org.cdlib.mrt.s3;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.cdlib.mrt.cloud.CloudList.CloudEntry;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.aws.AWSS3Cloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.service.CloudResponse.ResponseStatus;
import org.cdlib.mrt.s3.service.NodeIO.AccessNode;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

public class CloudIT {
        private int port = 9000;
        private int admin_port = 9001;

        public CloudIT() {
                try {
                        port = Integer.parseInt(System.getenv("minio-server.port"));
                } catch (NumberFormatException e) {
                        System.err.println("minio-server.port not set... using default value");
                }

                try {
                        admin_port = Integer.parseInt(System.getenv("minio-server.admin_port"));
                } catch (NumberFormatException e) {
                        System.err.println("minio-server.admin_port not set... using default value");
                }
        }

        private NodeIO nodeIO;
        private NodeIO.AccessNode primaryAccessNode;
        private NodeIO.AccessNode replicationAccessNode;
        public static final String key ="abc.txt";
        public static final String content = "abc";
        //macos: echo "abc" > abc.txt; md5 abc.txt
        public static final String content_md5 = "900150983cd24fb0d6963f7d28e17f72";
        //macos: echo "abc" > abc.txt; shasum -a 256 abc.txt
        public static final String content_sha256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

        @Before 
        public void readYaml() {
                String yamlName = "yaml:";

                try {
                        LoggerInf logger = new TFileLogger("test", 50, 50);
                        nodeIO = NodeIO.getNodeIOConfig(yamlName, logger);
                        primaryAccessNode = nodeIO.getAccessNode(7777);
                        replicationAccessNode = nodeIO.getAccessNode(8888);
                } catch(Exception e){
                        e.printStackTrace();
                }
                assertFalse(nodeIO == null);
                assertFalse(primaryAccessNode == null);
                assertFalse(replicationAccessNode == null);
        }

        public String getContentForUrl(String url) throws HttpResponseException, IOException {
                /*
                When using java11 libraries, the following classes can be used...
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .build();
                HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
                */
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpGet request = new HttpGet(url);
                        HttpResponse response = client.execute(request);
                        return new BasicResponseHandler().handleResponse(response);
                }
        }


        @Test
        public void connectToMinioDocker() throws HttpResponseException, IOException {
                String resp = getContentForUrl(String.format("http://localhost:%d", admin_port));
                assertFalse(resp.isEmpty());
        }

        @Test
        public void testNodeState() throws TException {
                try {
                        ArrayList<NodeIO.AccessNode> accessNodes = nodeIO.getAccessNodesList();  
                        assertEquals(2, accessNodes.size());

                        StateHandler.RetState retstate = primaryAccessNode.service.getState(primaryAccessNode.container);
                        assertTrue(retstate.getOk());
                        retstate = replicationAccessNode.service.getState(replicationAccessNode.container);
                        assertTrue(retstate.getOk());
                } catch(Exception e){
                        e.printStackTrace();
                }
        }

        public File createFile(String s) throws IOException {
                Path p = Files.createTempFile("junit", "txt");                
                Files.write(p, s.getBytes());
                return p.toFile();
        }

        public Identifier addObject(CloudStoreInf service, String bucket, String key, String content) throws IOException, TException {
                CloudResponse resp = ((AWSS3Cloud)service).putObject(
                        bucket, 
                        key, 
                        createFile(content)
                );
                assertEquals(ResponseStatus.ok, resp.getStatus());
                assertEquals(content_md5, resp.getMd5());
                assertEquals(content_sha256, resp.getSha256());

                return resp.getObjectID();
        }

        public void deleteObject(CloudStoreInf service, String bucket, String key) throws IOException, TException {
               CloudResponse resp = service.deleteObject(
                        bucket, 
                        key
                );
                assertEquals(ResponseStatus.ok, resp.getStatus());
                CloudResponse r = service.getObjectList(bucket);
                assertTrue(r.getCloudList().getList().isEmpty());
        }

        public String stream2string(InputStream is) {
                StringBuilder sb = new StringBuilder();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                        sb.append(br.readLine());
                } catch(Exception e) {
                        System.out.println(e.getMessage());
                }
                return sb.toString();
        }

        @Test
        public void addDataToNode() throws TException {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);
                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                } catch(Exception e){
                        e.printStackTrace();
                }
        }

        @Test
        public void addDataToNodeCheckMetadata() throws TException {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);

                        Properties prop = primaryAccessNode.service.getObjectMeta(primaryAccessNode.container, key);
                        assertEquals(Integer.toString(content.length()), prop.getProperty("size"));
                        assertEquals(content_sha256, prop.getProperty("sha256"));

                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                } catch(Exception e){
                        e.printStackTrace();
                }
        }

        @Test
        public void addDataToNodeCheckContent() throws TException {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);

                        InputStream is = primaryAccessNode.service.getObjectStreaming(primaryAccessNode.container, key, new CloudResponse());
                        assertEquals(content, stream2string(is));

                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                } catch(Exception e){
                        e.printStackTrace();
                }
        }

        @Test
        public void addDataToNodePresigned() throws TException {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);

                        CloudResponse resp = primaryAccessNode.service.getPreSigned(10, primaryAccessNode.container, key, "text/plain", "inline");
                        String url = resp.getReturnURL().toString();
                        assertFalse(url.isEmpty());
                        String presp = getContentForUrl(url);
                        assertEquals(content, presp);

                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                } catch(Exception e){
                        e.printStackTrace();
                }
        }

        @Test
        public void addManifest() throws TException {
                try {
                        Identifier objid = addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);
                        CloudResponse resp = primaryAccessNode.service.putManifest(
                                primaryAccessNode.container, 
                                objid, 
                                createFile("manifest")
                        );
                        assertEquals(ResponseStatus.ok, resp.getStatus());

                        CloudResponse r = new CloudResponse();
                        InputStream is = primaryAccessNode.service.getManifest(
                                primaryAccessNode.container, 
                                objid, 
                                r
                        );
                        assertEquals(ResponseStatus.ok, r.getStatus());                        
                        assertEquals("manifest", stream2string(is));

                        r = primaryAccessNode.service.getObjectList(primaryAccessNode.container);
                        assertEquals(2, r.getCloudList().getList().size());

                        primaryAccessNode.service.deleteManifest(primaryAccessNode.container, objid);
                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);

                } catch(Exception e){
                        e.printStackTrace();
                }
        }

}