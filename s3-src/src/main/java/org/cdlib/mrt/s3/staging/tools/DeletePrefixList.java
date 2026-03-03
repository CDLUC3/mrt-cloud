/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3.staging.tools;

/**
 *
 * @author loy
 */
import org.cdlib.mrt.s3.tools.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import org.cdlib.mrt.s3.service.NodeIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
//import static org.cdlib.mrt.s3v2.test.TestAWSPut.getSSM;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;
import org.json.JSONArray;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class DeletePrefixList {
    protected static final Logger log4j = LogManager.getLogger(); 
    protected static final long defaultNode = 8501;
    protected CloudStoreInf service = null;
    protected String bucket = null;
    //protected String prefix = null;
    protected int count = 0;
    protected int deleteCount = 0;
    protected DeleteStat listStat = null;
    protected ArrayList<DeleteStat> statArr = new ArrayList<>();
    protected ArrayList<CloudList.CloudEntry> entryList = null;
    
    public static JSONObject deleteList(
            String prefix)
        throws TException
    {
        DeletePrefixList dPL = getDeletePrefixListDefault();
        JSONObject responseJson = dPL.deleteRetrieveList(prefix);
        return responseJson;
    }
    
    public static DeletePrefixList getDeletePrefixListDefault()
        throws TException
    { 
        return getDeletePrefixList(defaultNode);
    }
    
    public static DeletePrefixList getDeletePrefixList(long retrieveNode)
        throws TException
    { 
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        return getDeletePrefixList(retrieveNode, logger);
    }
    
    
    public static DeletePrefixList getDeletePrefixList(long node, LoggerInf logger)
        throws TException
    {
        NodeIO nodeIO = NodeIO.getNodeIOConfig("yaml:2", logger) ;
        NodeIO.AccessNode retrievingAccessNode = nodeIO.getAccessNode(node);
        if (retrievingAccessNode == null) {
            throw new TException.INVALID_OR_MISSING_PARM("AccessNode not found for " + node);
        }
        CloudStoreInf service = retrievingAccessNode.service;
        String bucket = retrievingAccessNode.container;
        System.out.println(" - bucket=" + bucket);
        return new DeletePrefixList(service, bucket);
    }
    
    public DeletePrefixList  (
            CloudStoreInf service,
            String bucket)
        throws TException
    {
        this.service = service;
        this.bucket = bucket;
    }
    
    public static void main(String[] args) 
        throws TException
    {
        main_bigFile(args);
    }
    
    public static void main_noCollection(String[] args) 
        throws TException
    {
        
        String prefix = "ziptest";
        JSONObject responseJson = DeletePrefixList.deleteList(prefix);
        System.out.println("JSON:\n"  + responseJson.toString(2));
    }
    
    public static void main_bigFile(String[] args) 
        throws TException
    {
        
        String prefix = "MS323_AIP.zip";
        JSONObject responseJson = DeletePrefixList.deleteList(prefix);
        System.out.println("JSON:\n"  + responseJson.toString(2));
    }
    
    
    public static void main_test1(String[] args) 
        throws TException
    {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        
        NodeIO nodeIO = NodeIO.getNodeIOConfig("yaml:2", logger) ;
        NodeIO.AccessNode retrievingAccessNode = nodeIO.getAccessNode(8501);
        if (retrievingAccessNode == null) {
            throw new TException.INVALID_OR_MISSING_PARM("8501 accessNode not found");
        }
        
        CloudStoreInf service = retrievingAccessNode.service;
        String bucket = retrievingAccessNode.container;
        System.out.println(" - bucket=" + bucket);
        String prefix = "group.t1";
        DeletePrefixList deletePrefixList = DeletePrefixList.getDeletePrefixList(8501, logger);
        ArrayList<CloudList.CloudEntry> entries = deletePrefixList.getDeleteList(prefix);
    }
    
    public static void main_test2(String[] args) 
        throws TException
    {
        LoggerInf logger = new TFileLogger("jtest", 50, 50);
        
        NodeIO nodeIO = NodeIO.getNodeIOConfig("yaml:2", logger) ;
        NodeIO.AccessNode retrievingAccessNode = nodeIO.getAccessNode(8501);
        if (retrievingAccessNode == null) {
            throw new TException.INVALID_OR_MISSING_PARM("8501 accessNode not found");
        }
        
        CloudStoreInf service = retrievingAccessNode.service;
        String bucket = retrievingAccessNode.container;
        System.out.println(" - bucket=" + bucket);
        String prefix = "group.t1";
        DeletePrefixList deletePrefixList = DeletePrefixList.getDeletePrefixList(8501, logger);
        ArrayList<CloudList.CloudEntry> entries = deletePrefixList.getDeleteList(prefix);
        for (CloudList.CloudEntry entry: entries) {
            String msg = entry.dump("dumpit");
            System.out.println(msg);
        }
        
        deletePrefixList.deleteObjectPrefix(prefix);
        JSONObject responseJson = deletePrefixList.jsonResult(prefix);
        System.out.println("JSON:\n"  + responseJson.toString(2));
    }
    
    /**
     * Primary delete
     * @param prefix s3 key prefix for deletion list group
     * @return json response
     * @throws TException 
     */
    public JSONObject deleteRetrieveList (String prefix)
        throws TException
    {
        
        try {
            deleteObjectPrefix(prefix);
            return jsonResult(prefix);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public void deleteObjectPrefix (String prefix)
        throws TException
    {
        CloudResponse listResponse = null;
        try {
            entryList = getDeleteList(prefix);
            if (entryList == null) return;
            for (CloudList.CloudEntry entry : entryList) {
                String deleteKey = entry.getKey();
                DeleteStat deleteStat = delete(deleteKey);
                statArr.add(deleteStat);
            }
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    protected ArrayList<CloudList.CloudEntry> getDeleteList(String prefix)
        throws TException
    {
        try {
            listStat = new DeleteStat(DeleteStat.ResultType.list, prefix);
            CloudResponse listResponse = service.getObjectList (bucket, prefix);
            setStat(listResponse, listStat);
            if (listStat.result != DeleteStat.DeleteResult.ok) {
                return null;
            }
            CloudList cloudEntryList = listResponse.getCloudList();
            ArrayList<CloudList.CloudEntry> entryList = cloudEntryList.getList();
            if (false) { //test missing
                CloudList.CloudEntry badEntry = new CloudList.CloudEntry();
                badEntry.key = "zzz";
                badEntry.container = bucket;
                badEntry.size = 666;
                entryList.add(badEntry);
            }
            return entryList;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected DeleteStat delete(String deleteKey)
        throws TException
    {
        try {
            log4j.debug("delete:" + deleteKey);
            DeleteStat stat = new DeleteStat(DeleteStat.ResultType.object, deleteKey);
            Properties prop = service.getObjectMeta(bucket, deleteKey);
            if ((prop != null) && prop.isEmpty()) {
                setStatResult("missing", stat);
                return stat;
            }
            CloudResponse deleteResponse  = service.deleteObject (bucket, deleteKey);
            
            setStat(deleteResponse, stat);
            deleteCount++;
            return stat;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    } 
    
    protected DeleteStat setStatResult(String statResult, DeleteStat stat)
        throws TException
    {
        stat.result = DeleteStat.DeleteResult.valueOf(statResult);
        return stat;
    }
    
    protected DeleteStat setStat(CloudResponse response, DeleteStat stat)
        throws TException
    {
        try {
            stat.ex = response.getException();
            if (stat.ex != null) {
                if (stat.ex instanceof TException.REQUESTED_ITEM_NOT_FOUND) {
                    stat.result = DeleteStat.DeleteResult.missing;
                    
                } else {
                    stat.result = DeleteStat.DeleteResult.error;
                }
                
            } else {
                stat.result = DeleteStat.DeleteResult.ok;
            }
            return stat;
            
        }  catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public JSONObject jsonResult(String prefix)
        throws TException
    {
        log4j.debug("SIZE statArr:" + statArr.size());
        JSONObject listJson = new JSONObject();
        listJson.put("bucket", bucket);
        listJson.put("prefix", prefix);
        listJson.put("listResult", listStat.result.toString());
        if (listStat.result == DeleteStat.DeleteResult.ok) {
            listJson.put("count", entryList.size());
        } else {
            listJson.put("exception", listStat.ex.toString());
            return listJson;
        }
        listJson.put("deleteCount", deleteCount);
        HashMap<DeleteStat.DeleteResult,Integer> statMap = new HashMap<>();
        for (DeleteStat stat : statArr) {
            DeleteStat.DeleteResult result = stat.result;
            Integer cnt = statMap.get(result);
            if (cnt == null) cnt = 1;
            else cnt++;       
            log4j.debug("stat.result:" + stat.result.toString() 
                    + " - cnt:" + cnt 
                    );
            statMap.put(result, cnt);
        }
        Set<DeleteStat.DeleteResult> keys = statMap.keySet();
        JSONArray listCnts = new JSONArray();
        for (DeleteStat.DeleteResult key : keys) {
            JSONObject entryJson = new JSONObject();
            Integer cnt = statMap.get(key);
            entryJson.put(key.toString(), cnt);
            listCnts.put(entryJson);
        }
        listJson.put("deleteRsults", listCnts);
        Integer errorCnt = statMap.get(DeleteStat.DeleteResult.error);
        if (errorCnt == null) errorCnt = 0;
        String runStatus = "ok";
        if ((errorCnt != 0) || (listStat.result != DeleteStat.DeleteResult.ok)) {
            runStatus = "fail";
        }
        listJson.put("runStatus", runStatus);
        return listJson;
    }
    
    public static class DeleteStat {
        public enum DeleteResult {none, ok, missing, error};
        public enum ResultType {list, object};
        public ResultType type = null;
        public DeleteResult result = null;
        public String deleteKey = null;
        public Exception ex = null;
        public DeleteStat(ResultType type, String deleteKey) {
            this.type = type;
            this.deleteKey = deleteKey;
        }
    }
}