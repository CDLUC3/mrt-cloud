/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.service;

import java.util.Properties;
import java.util.Map;
import java.util.Set;
import org.cdlib.mrt.utility.StringUtil;
import org.json.JSONObject;

/**
 *
 * @author replic
 */
public class DefNode {
    
        public Long nodeNumber = null;
        public String nodeDescription = null;
        public String bucket = null;
        public Properties propNodeDef = null;
        public DefNode() { }
        public DefNode(
            Long nodeNumber,
            String bucket,
            String nodeDescription,
            Properties propNodeDef
        ) {
            this.nodeNumber = nodeNumber;
            this.propNodeDef = propNodeDef;
            this.nodeDescription = nodeDescription;
            this.bucket = bucket;
            //System.out.println("echo '" + jsonNodeDef.toString() + "' > " + nodeNumber);
        }
        
        public String dump(String header)
        {
            StringBuffer buf = new StringBuffer();
            if (!StringUtil.isAllBlank(header)) {
                buf.append(header);
            }
            buf.append(" JsonNode:"
                    + " - nodeNumber:" + nodeNumber
                    + " - propNodeDef:" + propNodeDef
                    + " - nodeDescription:" + nodeDescription
            );
            return buf.toString();
        }
        
        public boolean isFail() 
        {
            if ((propNodeDef == null) 
                    || (propNodeDef.size() == 0))
            {
                return true;
            }
            Map<String, String> map = (Map)propNodeDef;
            Set<String> keys = map.keySet();
            for(String k:keys){
                String value = map.get(k);
                if (value.equals("SSMFAIL")) {
                    return true;
                }
            }
            return false;
        }
}
