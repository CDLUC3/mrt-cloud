/*
Copyright (c) 2005-2010, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*********************************************************************/

package org.cdlib.mrt.s3.service;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;      
import java.util.List;      
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.cdlib.mrt.s3.service.NodeIO.MESSAGE;
import org.cdlib.mrt.tools.SSMConfigResolver;
import org.cdlib.mrt.tools.YamlParser;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONArray;
import org.json.JSONObject;

public class NodeIOConf 
{
    
    protected static final String NAME = "NodeIOConf";
    protected static final String MESSAGE = NAME + ": ";
    private static boolean DEBUG = false;
    private static boolean DEBUG_ACCESS = false;
    private static Pattern pConfig = Pattern.compile("(jar|yaml):[\\s]*([^\\s]*)[\\s]*");
    private static Pattern p2Config = Pattern.compile("([^\\s]*):[\\s]*([^\\s]*)[\\s]*");
    
    protected String nodeName = null;
    protected LoggerInf logger = null;
    protected ConfigType configType = ConfigType.jar;
    
    public enum ConfigType {jar, yaml};
    
    public static void main(String[] args) throws Exception {
        //main_ssm(args);
        main_yaml(args);
        //main_file(args);
        //main_jar(args);
    }
    
    public static void main_yaml(String[] args) throws Exception {

        LoggerInf logger = new TFileLogger("lockFile", 10, 10);
        String ssmBase = "yaml:";
        NodeIO nodeIO = NodeIOConf.getNodeIOConfig(ssmBase, logger) ;
        nodeIO.printNodes("main dump");
    }
    
    public static void main_jar(String[] args) throws Exception {

        LoggerInf logger = new TFileLogger("lockFile", 10, 10);
        String jarBase = "jar:nodes-stage";
        NodeIO nodeIO = NodeIOConf.getNodeIOConfig(jarBase, logger) ;
    }
    
    
    public static NodeIO getNodeIOConfig(String config, LoggerInf logger) 
        throws TException
    {
        ConfigType configType = ConfigType.jar;
        
        String test = config.toLowerCase();
        String type = "jar";
        String nodeIOName = config;
        Matcher m = pConfig.matcher(test);
        if (m.matches()) {
            type = m.group(1);
        }
        Matcher m2 = p2Config.matcher(config);
        if (m2.matches()) {
            nodeIOName = m2.group(2);
        }
        System.out.println("getNodeIOConfig"
                + " - type:" + type
                + " - nodeIOName:" + nodeIOName
                        );
        configType = ConfigType.valueOf(type);
        try {
            
            switch (configType) {
                case jar:  return getNodeIOJar(nodeIOName, logger);
                case yaml: return getNodeIOYaml(nodeIOName,  logger);
            default: 
                throw new TException.INVALID_OR_MISSING_PARM("NodeIO configuration switch not found:" + configType);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static NodeIO getNodeIOJar(String nodeName, LoggerInf logger) 
        throws TException
    {
        
        try {
            
            if (nodeName== null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeName required and missing");
            }
            return new NodeIO(nodeName,logger);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static NodeIO getNodeIOYaml(String yamlName, LoggerInf logger) 
        throws TException
    {
        try {
            return getNodeIOYaml(logger);
            
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public static NodeIO getNodeIOYaml(LoggerInf logger) 
        throws TException
    {
        
        try {
            NodeIO.AccessNode test = new NodeIO.AccessNode();
            String propName = "yaml/cloudConfig.yml";
            InputStream propStream =  test.getClass().getClassLoader().
                    getResourceAsStream(propName);
            if (propStream == null) {
                throw new TException.INVALID_OR_MISSING_PARM("yaml/cloudConfig.yml not found");
            }
            String inYml = StringUtil.streamToString(propStream, "utf8");
            if (DEBUG) System.out.println("***inYml:\n" + inYml);
            
            
            List<DefNode> defNodes = new ArrayList<DefNode>();
            SSMConfigResolver ssmResolver = new SSMConfigResolver();
            YamlParser yamlParser = new YamlParser(ssmResolver);
            yamlParser.parseString(inYml);
            try {
                yamlParser.resolveValues();
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM("resolve on yalParser fails: - Exception:" + ex);
            }
            String jsonout = yamlParser.dumpJson();

            JSONObject jsonBase = new JSONObject(jsonout);
            if (DEBUG) System.out.println("***jsonBase\n" + jsonBase.toString());
            
            String nodeTable = null;
            try {
                nodeTable = jsonBase.getString("node-table");
            } catch (Exception ex) {
                nodeTable = null;
            }
            String envName = nodeTable;
            if (envName == null) {
                throw new TException.INVALID_OR_MISSING_PARM("Yaml envName missing");
            }
            
            JSONObject nodestables = jsonBase.getJSONObject("nodes-tables");
            JSONArray envTables = nodestables.getJSONArray(envName);
            for (int i=0; i<envTables.length(); i++) {
                JSONObject entry = envTables.optJSONObject(i);
                DefNode defNode = getNodeYaml(entry, logger);
                if (DEBUG)System.out.println(defNode.dump("***DUMP***"));
                defNodes.add(defNode);
                        
            }
            NodeIO nodeIO = new NodeIO(defNodes, logger);
            
            nodeIO.setNodeName("yaml:");
            return nodeIO;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static Properties setPropSSM(Properties cloudProp)
        throws TException
    {
        SSMConfigResolver ssmResolver = new SSMConfigResolver();
        try {
            
            Properties retProp = new Properties();
            Set<String> keys = cloudProp.stringPropertyNames();
            for (String key : keys) {
                String value = cloudProp.getProperty(key);
                if (value.startsWith("{!")) {
                    String resolveValue = ssmResolver.resolveConfigValue(value);
                    if (resolveValue.equals("SSMFAIL")) {
                        throw new TException.INVALID_CONFIGURATION("Unable to locate SSM:" + value);
                    }
                    retProp.setProperty(key, resolveValue);
                } else {
                    retProp.setProperty(key, value);
                }
            }
            return retProp;
            
        } catch (TException tex) {
            System.out.println(MESSAGE + "Exception:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public static DefNode getNodeYaml(JSONObject yamlEntry, LoggerInf logger) 
        throws TException
    {
        
        try {
            
            if (yamlEntry == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fileBase required and missing");
            }
           
            long nodeNumber = yamlEntry.getLong("identifier");
            String nodeDescription = null;
            try {
               nodeDescription = yamlEntry.getString("desc");
            } catch (Exception ex) { }
            String bucket = yamlEntry.getString("bucket");
            JSONObject jsonProp = yamlEntry.getJSONObject("service-properties");
            
            DefNode defNode = jsonDefNode(nodeNumber, nodeDescription, bucket, jsonProp);
            return defNode;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    private static DefNode jsonDefNode(Long node, String nodeDescription, String bucket, JSONObject jsonNode)
        throws TException
    {
        try {
            String container = bucket;
            Properties serviceProp = getServiceProp(jsonNode);
            if (container == null) {
                container = serviceProp.getProperty("bucket");
            }
            if (container == null) {
                throw new TException.INVALID_ARCHITECTURE("addMapEntry - no bucket found");
            }
            DefNode defNode = new DefNode(node, container, nodeDescription, serviceProp);
            return defNode;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
     
    private static Properties getServiceProp(JSONObject serviceJSONProp)
    {
        Properties serviceProp = new Properties();
        String [] names = JSONObject.getNames(serviceJSONProp);
        for (String name : names) {
            add(serviceJSONProp, serviceProp, name);
        }
        return serviceProp;
    }
    
    private static Properties add(JSONObject serviceJSONProp, Properties serviceProp, String key)
    {
        try {
            String value = serviceJSONProp.getString(key);
            if (value != null) {
                serviceProp.setProperty(key, value);
            }
            return serviceProp;
            
        } catch (Exception ex) {
            return serviceProp;
        }
    }
}
