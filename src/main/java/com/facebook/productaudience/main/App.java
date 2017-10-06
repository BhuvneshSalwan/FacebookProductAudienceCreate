package com.facebook.productaudience.main;

import java.util.ArrayList;
import java.util.List;

import com.facebook.productaudience.common.ProductAudienceCreate;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.bigquery.main.Authenticate;
import com.google.bigquery.main.TableResults;

public class App {
	
	public static ArrayList<Rows> logChunk = new ArrayList<Rows>();
	
	public static void main( String[] args )
    {
        
    	Bigquery bigquery;
    	
    	if((bigquery = Authenticate.getAuthenticated()) != null){
    		
    		System.out.println(bigquery);
    		
    		if(TableResults.ListDataSet(bigquery)){
    		
    			if(TableResults.ListTables(bigquery)){
    				
    				List<TableRow> listData = TableResults.getResults(bigquery);
    				
    				if(null != listData && listData.size() > 0){	
    					
    					for(int arr_i = 0; arr_i < listData.size(); arr_i++){
    			
    						TableRow row = listData.get(arr_i);
    						
    						if(ProductAudienceCreate.createProductAudience(row)){
    							System.out.println("Audience is created Successfully : " + row.getF().get(0).getV());
    						}
    						else{
    							System.out.println("Response Message : Page Engagement Audience Creation Failed.");
    						}
    						
    					}
    					
    					if(null !=  logChunk && logChunk.size() > 0){
    			    		
    			    		if(TableResults.insertDataRows(bigquery, logChunk)){
    			    			System.out.println("Response Message : Logs Added Successfully.");
    			    		}else{
    			    			System.out.println("Response Message : Error while saving Logs.");
    			    		}
    			    		
    			    	}
    					
    				}
    				else{
    					System.out.println("Response Message : Some Error while retrieving data from Table.");
    				}
    				
    			}
    			else{
    				System.out.println("Response Message : Error while Listing Tables.");
    			}
    			
    		}
    		else{
    			System.out.println("Response Message : Error while Listing Datasets.");
    		}
    		
    	}
    	else{
    		
    		System.out.println("Response Message : Didn't got the object of Big Query from get Authenticated Method.");
    		System.exit(0);
    		
    	}
    	
    }
	
}