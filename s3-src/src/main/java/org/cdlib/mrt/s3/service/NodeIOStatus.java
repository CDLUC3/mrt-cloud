/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.service;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;

import org.json.JSONObject;
import org.json.JSONArray;

public class NodeIOStatus {
    private static int DEFAULT_TIMEOUT = 5; // seconds
    private final int timeout;
    
    private static final Logger log4j = LogManager.getLogger();
    
    public static void main(String[] argv) {

    	try {
             main_status(argv);
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
    public static void main_state_form(String[] argv) {

    	try {
            
            LoggerInf logger = new TFileLogger("test", 50, 50);
            
            //String yamlName = "jar:nodes-stagedry";
            String yamlName = "yaml:";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            NodeIOStatus nodeStatus = new NodeIOStatus(10);
            JSONObject stateNodeIO = nodeStatus.runNodeStatus(nodeIO);
            System.out.println(stateNodeIO.toString(2));
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
    
    public static void main_status(String[] argv) {

    	try {
            
            LoggerInf logger = new TFileLogger("test", 50, 50);
            
            //String yamlName = "jar:nodes-stagedry";
            String yamlName = "yaml:";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            JSONObject stateNodeIO = NodeIOStatus.runStatus(nodeIO);
            System.out.println(stateNodeIO.toString(2));
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
    
    public static JSONObject runStatus(NodeIO nodeIO)
    {
        NodeIOStatus nodeIOStatus = new NodeIOStatus(DEFAULT_TIMEOUT);
        return nodeIOStatus.runNodeStatus(nodeIO);
    }
    
    public static JSONObject runStatus(NodeIO nodeIO, int timeout)
    {
        NodeIOStatus nodeIOStatus = new NodeIOStatus(timeout);
        return nodeIOStatus.runNodeStatus(nodeIO);
    }
     
    public NodeIOStatus (int timeout)
    {
        this.timeout = timeout;
    }

    
    public JSONObject runNodeStatus(NodeIO nodeIO) 
    {
    	try {
            JSONObject stateNodeIO = new JSONObject();
            JSONArray stateArray = new JSONArray();
            stateNodeIO.put("NodesStatus", stateArray);
            ArrayList<NodeIO.AccessNode> accessNodes = nodeIO.getAccessNodesList();
            for (NodeIO.AccessNode accessNode : accessNodes)
            {
                
                StateHandler.RetState retstate = doTask(accessNode);
                JSONObject nodeJson = new JSONObject();
                CloudStoreInf service = accessNode.service;
                String container = accessNode.container;
                nodeJson.put("node", "" + accessNode.nodeNumber);
                nodeJson.put("bucket", container);
                nodeJson.put("running", retstate.getOk());
                nodeJson.put("mode", accessNode.accessMode);
                nodeJson.put("description", accessNode.nodeDescription);
                nodeJson.put("durationMs", retstate.getDuration());
                if (retstate.getError() != null) {
                    nodeJson.put("error", retstate.getError());
                }
                stateArray.put(nodeJson);
            }
            return stateNodeIO;
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
                return null;
        }
    }
    
    protected StateHandler.RetState doTask(NodeIO.AccessNode accessNode)
    {
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        long startTask = System.currentTimeMillis();
        Exception executerError = null;
        try {
            Task task = new Task(accessNode);
            executor.submit(task);
            log4j.debug("Shutdown executor");
            executor.shutdown();
            executor.awaitTermination(timeout, TimeUnit.SECONDS);
            StateHandler.RetState retStat = task.getRetstate();
            return retStat;
            
        } catch (InterruptedException e) {
           executerError = e;
           System.err.println("tasks interrupted");
           return null;
           
        } catch (Exception e) {
           executerError = e;
           System.err.println("Task exception:" + e);
           return null;
           
        } finally {
           
           if (!executor.isTerminated()) {
              long endTask = System.currentTimeMillis() - startTask;
              StateHandler.RetState errStat = new StateHandler.RetState(accessNode.container, null, "" + executerError);
              errStat.setDuration(endTask);
              errStat.setError("forced termination");
              executor.shutdownNow();
              if (false) log4j.info("Forced termination:"
                    + " - node=" + accessNode.nodeNumber
                    + " - container=" + accessNode.container
                    + " - timeout=" + timeout
              );
              return errStat;
           }
           executor.shutdownNow();
           //System.out.println("shutdown finished");
        }
    }
    
    static class Task implements Runnable 
    {
        private final NodeIO.AccessNode accessNode;
        private Exception ex = null;
        private StateHandler.RetState retstate = null;

        public Task (NodeIO.AccessNode accessNode)
        {
            this.accessNode = accessNode;
        }

        public void run() 
        {

            try {
                   CloudStoreInf service = accessNode.service;
                   String container = accessNode.container;
                   retstate = service.getState(container);
                   if (false && (accessNode.nodeNumber == 2001)) { // test
                       Thread.sleep(20000);
                   }
            } catch (Exception e) {
               ex = e;
               System.out.println("***In Task:" + e);
            }
        }

        public Exception getEx() {
            return ex;
        }

        public StateHandler.RetState getRetstate() {
            return retstate;
        }
   }
}