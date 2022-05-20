/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.test;

import java.util.ArrayList;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;

public class TestNodeIOState {
    
     public static void main(String[] argv) {

    	try {
            
            LoggerInf logger = new TFileLogger("test", 50, 50);
            
            //String yamlName = "jar:nodes-stagedry";
            String yamlName = "yaml:";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            ArrayList<NodeIO.AccessNode> accessNodes = nodeIO.getAccessNodesList();
            for (NodeIO.AccessNode accessNode : accessNodes)
            {
                CloudStoreInf service = accessNode.service;
                String container = accessNode.container;
                StateHandler.RetState retstate = service.getState(container);
                System.out.println(retstate.dumpline("Node:" + accessNode.nodeNumber) + " - mode:" + accessNode.accessMode
                );
            }
            
    
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
}