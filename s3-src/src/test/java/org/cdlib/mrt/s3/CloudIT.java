package org.cdlib.mrt.s3;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.cdlib.mrt.cloud.CloudList.CloudEntry;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.service.CloudResponse.ResponseStatus;
import org.cdlib.mrt.s3.tools.CloudCloudCopy;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import java.io.IOException;

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
        //public static final String content_md5 = "900150983cd24fb0d6963f7d28e17f72";
        //public static final String content_md5 = "\"af5da9f45af7a300e3aded972f8ff687-1\"";
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
                        System.out.println("NodeIO AWS version:" + nodeIO.getAwsVersion());
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
        public void testNodeState() {
                try {
                        ArrayList<NodeIO.AccessNode> accessNodes = nodeIO.getAccessNodesList();  
                        assertEquals(2, accessNodes.size());

                        StateHandler.RetState retstate = primaryAccessNode.service.getState(primaryAccessNode.container);
                        assertTrue(retstate.getOk());
                        assertTrue(retstate.getError() == null);

                        assertEquals(primaryAccessNode.container, retstate.getBucket());

                        System.out.println(retstate.dump("dump"));
                        System.out.println(retstate.dumpline("dumpline"));
                        System.out.println(retstate.getDuration());
                        
                        assertEquals(primaryAccessNode.container, retstate.getBucket());
                        System.out.println(retstate.getKey());
                        retstate = replicationAccessNode.service.getState(replicationAccessNode.container);
                        assertTrue(retstate.getOk());

                        //Round out test coverage
                        retstate.setBucket("foo");
                        retstate.setKey("bar");

                        retstate = new StateHandler.RetState("foo", null, "error");
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

        public File createFile(String s) throws IOException {
                Path p = Files.createTempFile("junit", "txt");                
                Files.write(p, s.getBytes());
                return p.toFile();
        }

        public Identifier addObject(CloudStoreInf service, String bucket, String key, String content) throws IOException, TException {
                //CloudResponse resp = ((AWSS3V2Cloud)service).putObject(
                CloudResponse resp = service.putObject(
                        bucket, 
                        key, 
                        createFile(content)
                );
                assertEquals(ResponseStatus.ok, resp.getStatus());
                assertEquals(content.length(), resp.getStorageSize());
                assertEquals(content_sha256, resp.getSha256());

                return resp.getObjectID();
        }

        public void deleteObject(CloudStoreInf service, String bucket, String key) throws IOException, TException {
               CloudResponse resp = service.deleteObject(
                        bucket, 
                        key
                );
                assertEquals(ResponseStatus.ok, resp.getStatus());
        }

        public void checkEmpty(CloudStoreInf service, String bucket) throws TException {
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
        public void addDataToNode() {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);
                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                        checkEmpty(primaryAccessNode.service, primaryAccessNode.container);
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

        @Test
        public void addDataToNodeCheckMetadata() {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);

                        Properties prop = primaryAccessNode.service.getObjectMeta(primaryAccessNode.container, key);
                        assertEquals(Integer.toString(content.length()), prop.getProperty("size"));
                        assertEquals(content_sha256, prop.getProperty("sha256"));

                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                        checkEmpty(primaryAccessNode.service, primaryAccessNode.container);
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

        @Test
        public void addDataToNodeCheckContent() {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);

                        InputStream is = primaryAccessNode.service.getObjectStreaming(primaryAccessNode.container, key, new CloudResponse());
                        assertEquals(content, stream2string(is));

                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                        checkEmpty(primaryAccessNode.service, primaryAccessNode.container);
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

        @Test
        public void addDataToNodePresigned() {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);
   
                        CloudResponse resp = primaryAccessNode.service.getPreSigned(10, primaryAccessNode.container, key, "text/plain", "inline");
                        String url = resp.getReturnURL().toString();
                        assertFalse(url.isEmpty());
                        String presp = getContentForUrl(url);
                        assertEquals(content, presp);

                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                        checkEmpty(primaryAccessNode.service, primaryAccessNode.container);
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

        @Test
        public void addManifest() {
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

                        checkEmpty(primaryAccessNode.service, primaryAccessNode.container);
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

        @Test
        public void addMultipleDataToNode() {
                try {
                        CloudStoreInf service = primaryAccessNode.service;
                        String bucket = primaryAccessNode.container;

                        addObject(service, bucket, "test1.txt", content);
                        addObject(service, bucket, "test2.txt", content);
                        addObject(service, bucket, "test3.txt", content);
                        addObject(service, bucket, "test4.txt", content);

                        List<CloudEntry> list = getCloudList(service, bucket);
                        assertEquals(4, list.size());

                        String key = list.get(0).getKey();
                        assertEquals("test1.txt", key);

                        list = getCloudListAfter(service, bucket, key);
                        key = list.get(0).getKey();
                        assertEquals("test2.txt", key);

                        list = getCloudListAfter(service, bucket, key);
                        key = list.get(0).getKey();
                        assertEquals("test3.txt", key);

                        list = getCloudListAfter(service, bucket, key);
                        key = list.get(0).getKey();
                        assertEquals("test4.txt", key);

                        list = getCloudListAfter(service, bucket, key);
                        assertTrue(list.isEmpty());

                        deleteObject(service, bucket, "test1.txt");
                        deleteObject(service, bucket, "test2.txt");
                        deleteObject(service, bucket, "test3.txt");
                        deleteObject(service, bucket, "test4.txt");

                        checkEmpty(service, bucket);
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

        @Test
        public void checkDigest() {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);
                        //CloudResponse r = primaryAccessNode.service.validateMd5(primaryAccessNode.container, key, content_md5);
                        //assertEquals(ResponseStatus.ok, r.getStatus());

                        MessageDigest md = new MessageDigest(content_sha256, "sha256");
                        CloudResponse r = primaryAccessNode.service.validateDigest(primaryAccessNode.container, key, md, content.length());
                        assertEquals(ResponseStatus.ok, r.getStatus());
                        assertNull(r.getErrMsg());

                        md = new MessageDigest("0000000000000000000000000000000000000000000000000000000000000000", "sha256");
                        r = primaryAccessNode.service.validateDigest(primaryAccessNode.container, key, md, content.length());
                        assertNotNull(r.getErrMsg());
 
                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                        checkEmpty(primaryAccessNode.service, primaryAccessNode.container);
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

        List<CloudEntry> getCloudList(CloudStoreInf service, String bucket) throws TException {
                CloudResponse r = service.getObjectList(bucket);
                return r.getCloudList().getList();
        }

        List<CloudEntry> getCloudListAfter(CloudStoreInf service, String bucket, String key) throws TException {
                CloudResponse r = service.getObjectListAfter(bucket, key, 10);
                return r.getCloudList().getList();
        }

        @Test
        public void copyFile() {
                try {
                        addObject(primaryAccessNode.service, primaryAccessNode.container, key, content);
                        List<CloudEntry> list = getCloudList(primaryAccessNode.service, primaryAccessNode.container);
                        assertEquals(1, list.size());
                        List<CloudEntry> rlist = getCloudList(replicationAccessNode.service, replicationAccessNode.container);
                        assertEquals(0, rlist.size());

                        CloudCloudCopy ccc = new CloudCloudCopy(
                                primaryAccessNode.service, 
                                primaryAccessNode.container,
                                replicationAccessNode.service,
                                replicationAccessNode.container
                        );
                        ccc.copy(list.get(0));

                        rlist = getCloudList(replicationAccessNode.service, replicationAccessNode.container);
                        assertEquals(1, rlist.size());

                        deleteObject(primaryAccessNode.service, primaryAccessNode.container, key);
                        deleteObject(replicationAccessNode.service, replicationAccessNode.container, key);
                        checkEmpty(primaryAccessNode.service, primaryAccessNode.container);
                        checkEmpty(replicationAccessNode.service, replicationAccessNode.container);
                } catch(Exception e){
                        e.printStackTrace();
                        fail(e.getMessage());
                }
        }

}