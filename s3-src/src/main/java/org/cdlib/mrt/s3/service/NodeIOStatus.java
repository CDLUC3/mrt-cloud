/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.service;

import java.util.ArrayList;
import org.cdlib.mrt.cloud.object.StatusHandler;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;

import org.json.JSONObject;
import org.json.JSONArray;

public class NodeIOStatus {
    
     public static void main(String[] argv) {

    	try {
            
            LoggerInf logger = new TFileLogger("test", 50, 50);
            
            //String yamlName = "jar:nodes-stagedry";
            String yamlName = "yaml:";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            JSONObject stateNodeIO = runStatus(nodeIO);
            System.out.println(stateNodeIO.toString(2));
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
    
    public static JSONObject runStatus(NodeIO nodeIO) 
    {
    	try {
            JSONObject stateNodeIO = new JSONObject();
            JSONArray stateArray = new JSONArray();
            stateNodeIO.put("NodesStatus", stateArray);
            ArrayList<NodeIO.AccessNode> accessNodes = nodeIO.getAccessNodesList();
            for (NodeIO.AccessNode accessNode : accessNodes)
            {
                JSONObject nodeJson = new JSONObject();
                CloudStoreInf service = accessNode.service;
                String container = accessNode.container;
                StatusHandler.RetStatus retstate = StatusHandler.runStatusHandler(accessNode.nodeNumber,service,container, 1, false);
                nodeJson.put("node", "" + accessNode.nodeNumber);
                nodeJson.put("bucket", retstate.getBucket());
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
}