/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.tools;
import java.io.File;
import java.util.ArrayList;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
/**
 *
 * @author replic
 */
public class TestStateHandler {
	public static final String KEY = "ark:/99999/test|1|prod/test";
        //public static final String nodeName = "nodes-dev";
        //public static final long nodeNumber = 910;
        public static final String nodeName = "nodes-ch-ucdn";
        public static final long nodeNumber = 7013;
            
    public static void main(String[] args) {
            
        NodeIO nodeIO = null;
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        try {
            ArrayList<Test> list = new ArrayList<>();
            list.add(test("nodes-dev", 910, logger));
            list.add(test("nodes-dev", 9001, logger));
            list.add(test("nodes-dev", 5001, logger));
            list.add(test("nodes-ch-ucdn", 7013, logger));
            list.add(test("nodes-ch-ucdn", 7032, logger));
            for (Test result : list) {
                System.out.println(result.state.dump("Node: " + result.node));
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
    }
    
               
    public static Test test(String nodeName, long nodeNumber, LoggerInf logger) {
            
        NodeIO nodeIO = null;
        try {
            System.out.println("\n***Test>" + nodeNumber);
            nodeIO = NodeIO.getNodeIO(nodeName, logger);
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(nodeNumber);
            CloudStoreInf service = accessNode.service;
            String container = accessNode.container;
            StateHandler stateHandler = StateHandler.getStateHandler(service, container, KEY, logger);
            StateHandler.RetState result =  stateHandler.process();
            return new Test(nodeNumber, result);
            
        } catch (TException tex) {
            tex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
        return null;
    }
    
    public static File format(StateInf responseState, LoggerInf logger)
        throws TException
    {
           String str = formatANVL(responseState, logger);
           System.out.println("*** ANVL ***\n" + str);
           File tmp = FileUtil.getTempFile("anvl", ".properties");
           FileUtil.string2File(tmp, str);
           return tmp;
    }
    
    public static String formatXML(StateInf responseState, LoggerInf logger)
        throws TException
    {
        
           FormatterInf xml = FormatterAbs.getXMLFormatter(logger);
           return formatIt(xml, responseState);
    }
    
    public static String formatJSON(StateInf responseState, LoggerInf logger)
        throws TException
    {
        
           FormatterInf xml = FormatterAbs.getJSONFormatter(logger);
           return formatIt(xml, responseState);
    }
    
    public static String formatANVL(StateInf responseState, LoggerInf logger)
        throws TException
    {
        
           FormatterInf xml = FormatterAbs.getANVLFormatter(logger);
           return formatIt(xml, responseState);
    }

    public static String formatIt(
            FormatterInf formatter,
            StateInf responseState)
    {
        try {
           ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
           PrintStream  stream = new PrintStream(outStream, true, "utf-8");
           formatter.format(responseState, stream);
           stream.close();
           byte [] bytes = outStream.toByteArray();
           String retString = new String(bytes, "UTF-8");
           return retString;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
            return null;
        }
    }
    
    public static class Test {
        public Long node = null;
        public StateHandler.RetState state = null;
        public Test(Long node, StateHandler.RetState state) {
            this.node = node;
            this.state = state;
        }
    }
}
