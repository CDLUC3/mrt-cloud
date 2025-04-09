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

public class GetSSM {
    public static Region region = Region.US_WEST_2;
    protected final SsmClient ssmClient;
    
    public GetSSM() 
    {
        ssmClient = SsmClient.builder()
                .region(region)
                .build();
    }
    public static void main(String[] args) {
        String paraName = "/uc3/mrt/stg/cloud/nodes/sdsc-s3-accessKey";
        GetSSM getSSM = new GetSSM();
        String result = getSSM.getssmv2(paraName);
        System.out.println(result);
    }
    
    
    
   public String getssmv2(String name)
   {
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