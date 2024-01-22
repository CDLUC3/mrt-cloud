/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test;


import java.util.ArrayList;
import java.lang.reflect.Field;
import java.util.Map;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.cloud.object.StatusHandler;

public class TestNodeIOStatusReset {
    
     public static void main(String[] argv) {

    	try {
            
            LoggerInf logger = new TFileLogger("test", 50, 50);
            
            updateEnv("NODE_TABLE", "nodes-stage") ;
            //updateEnv("NODE_TABLE", "nodes-stage-post-dryad") ;
            //updateEnv("NODE_TABLE", "nodes-prod-post-dryad") ;
            //updateEnv("NODE_TABLE", "nodes-prod") ;
            String yamlName = "yaml:";
            //String yamlName = "jar:nodes-stagedefbad";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            ArrayList<NodeIO.AccessNode> accessNodes = nodeIO.getAccessNodesList();
            int keyCnt = 1;
            boolean failCnt = false;
            for (NodeIO.AccessNode accessNode : accessNodes)
            {
                CloudStoreInf service = accessNode.service;
                String container = accessNode.container;
                //StateHandler.RetState retstate = service.getState(container);
                //System.out.println(retstate.dumpline("Node:" + accessNode.nodeNumber)testScan(
                System.out.println("***Test node:" + accessNode.nodeNumber);
                StatusHandler.RetStatus status = testScan(accessNode.nodeNumber, service, container, keyCnt, failCnt);
                System.out.append(status.dump("out"));
            }
            
    
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
    
    
    
    public static StatusHandler.RetStatus testScan(
            long nodeNumber,
            CloudStoreInf service,
            String bucket,
            int keyCnt,
            boolean failCnt)
    {
        StatusHandler.RetStatus status = StatusHandler.runStatusHandler(nodeNumber,service,bucket, keyCnt, failCnt);
        return status;
    }
    
    @SuppressWarnings({ "unchecked" })
  public static void updateEnv(String name, String val) 
          throws ReflectiveOperationException 
  {
    Map<String, String> env = System.getenv();
    Field field = env.getClass().getDeclaredField("m");
    field.setAccessible(true);
    ((Map<String, String>) field.get(env)).put(name, val);
  }
}