/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3v2.tools;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;
import software.amazon.awssdk.services.ssm.model.SsmException;

public class GetParameter {

    public static void main(String[] args) {
        String paraName = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-accessKey";
        String result = getssmv2(paraName);
        System.out.println(result);
    }
    
    public static String getExpand(String paraName) {

        final String USAGE = "\n" +
                "Usage:\n" +
                "    GetParameter <paraName>\n\n" +
                "Where:\n" +
                "    paraName - the name of the parameter\n";

        // Get args
       

        Region region = Region.US_WEST_2;
        SsmClient ssmClient = SsmClient.builder()
                .region(region)
                .build();

        try {
            
            
            GetParameterRequest parameterRequest = GetParameterRequest.builder()
                .name(paraName)
                .build();

            GetParameterResponse parameterResponse = ssmClient.getParameter(parameterRequest);
            String response = parameterResponse.parameter().value();
            System.out.println("The parameter value is "+response);
            return response;

        } catch (SsmException e) {
            System.err.println(e.getMessage());
            return null;
        }
   }
    
   public static String getssmv2(String name)
   {
        Region region = Region.US_WEST_2;
        SsmClient ssmClient = SsmClient.builder()
                .region(region)
                .build();

        try {

            GetParameterRequest request = GetParameterRequest.builder().
                     name(name).
                     withDecryption(Boolean.TRUE).
                     build();
             GetParameterResponse response = ssmClient.getParameter(request);
             return response.parameter().value();
             
        } catch (SsmException e) {
            System.err.println(e.getMessage());
            return null;
        }
   }
}