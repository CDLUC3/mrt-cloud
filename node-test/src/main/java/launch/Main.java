/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package launch;
import org.cdlib.mrt.s3.tools.CloudNodeList;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
public class Main {
    public static void main(String args[])
        throws TException
    {
        LoggerInf logger = new TFileLogger("tcloud", 0, 0);
        boolean debug = false;
        String testDirS = ".";
        if (args.length == 0) {
            throw new TException.INVALID_OR_MISSING_PARM("name test not provided");
        }
        String testName = args[0];
        if (args.length == 2)  {
            debug = true;
        }
        
        CloudNodeList cloudNodeList = CloudNodeList.getCloudNodeList(
            testDirS, 
            testName,
            logger);
        cloudNodeList.setCloudNodeTestDebug(debug);
        try {
            if (true) cloudNodeList.run();

        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        }
    
    }
}