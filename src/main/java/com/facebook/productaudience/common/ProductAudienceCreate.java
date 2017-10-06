package com.facebook.productaudience.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;

import com.facebook.productaudience.main.App;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableRow;

public class ProductAudienceCreate {

	public static final String URL = "https://graph.facebook.com";
	public static final String VERSION = "v2.9";
	private static final String SESSION_TOKEN = "CAAWXmQeQZAmcBANADF6ew1ZBXAAifj7REIcHmbTVjkAR5q6GAnRjrpcuVhhV435LHMXpb8HzUKzQaUU4uwkxIl5xpYSgzUNog43JX4qxe0pqVBvjHZCsPfgIpRRGY7xfFC2hb1Hi1s9EH0IhQu4KlnTGcsdgIq5FN2ufeNHOeEB9YGck36aah1rPHrdi10ZD";

	public static Boolean createProductAudience(TableRow row){
			
		try{
				
			String audience_name;
			String retention_days;
			String account_id;
			String product_set_id;
			String hostname;
			String events;
			String parse_client_id;
			
			try{ audience_name = String.valueOf(row.getF().get(0).getV()); } catch(Exception e){ System.out.println(e); System.out.println("Response Message : Couldn't find the Audience Name in Table."); return false;}
			try{ retention_days = String.valueOf(row.getF().get(1).getV()); } catch(Exception e){ System.out.println(e); return false;}
			try{ account_id = String.valueOf(row.getF().get(2).getV()); } catch(Exception e){ System.out.println(e); return false; }
			try{ product_set_id = String.valueOf(row.getF().get(3).getV()); } catch(Exception e){ System.out.println(e); return false; }
			try{ hostname = String.valueOf(row.getF().get(4).getV()); } catch(Exception e){ System.out.println(e); return false; }
			try{ events = String.valueOf(row.getF().get(5).getV()); } catch(Exception e){ System.out.println(e); return false; }
			try{ parse_client_id = String.valueOf(row.getF().get(6).getV()); } catch(Exception e){ System.out.println(e); parse_client_id = "NULL"; }
			
			String custom_url = URL + "/" + VERSION + "/act_" + account_id + "/product_audiences";
				
			HttpClient reqClient = new DefaultHttpClient();
			HttpPost reqpost = new HttpPost(custom_url);
				
			ArrayList<NameValuePair> urlparameters = new ArrayList<NameValuePair>();
			
			int retention_seconds = Integer.parseInt(retention_days) * 60 * 60 * 24;
				
			urlparameters.add(new BasicNameValuePair("access_token", SESSION_TOKEN));				
			urlparameters.add(new BasicNameValuePair("name", audience_name));
			urlparameters.add(new BasicNameValuePair("product_set_id", product_set_id));
			urlparameters.add(new BasicNameValuePair("inclusions","[{\"retention_seconds\":"+ retention_seconds +",\"rule\":{\"event\":{\"eq\":\""+ events +"\"}}}]"));
			reqpost.setEntity(new UrlEncodedFormEntity(urlparameters));
		
			System.out.println("Sending POST Request : " + custom_url);
			System.out.println("POST Parameters : " + urlparameters.toString());
				
			HttpResponse response = reqClient.execute(reqpost);
				
			System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
				
			StringBuffer buffer = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line = null;
				
			while((line = reader.readLine()) != null){
				buffer.append(line);
			}
				
			System.out.println("Response Content : " + buffer.toString());
				
			Rows logsRow = new Rows();
				
			HashMap<String, Object> logsMap = new HashMap<String, Object>();
				
			logsMap.put("hostname", hostname);
			logsMap.put("parse_client_id", parse_client_id);
			logsMap.put("account_id", account_id);
			logsMap.put("operation", "CREATE");
			logsMap.put("table_name", "PRODUCT_AUDIENCE_CREATE");
			logsMap.put("audience_name", audience_name);
			logsMap.put("product_set_id", product_set_id);
			logsMap.put("status_code", response.getStatusLine().getStatusCode());
			logsMap.put("response_message", buffer.toString());
			logsMap.put("created_at", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()));
				
			logsRow.setJson(logsMap);
			App.logChunk.add(logsRow);
				
			if(response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300){
					
				JSONObject responseObj = new JSONObject(buffer.toString());
					
				if(responseObj.has("id")){
					System.out.println("Response Message : Audience Created Successfully with Audience ID : " + responseObj.getString("id"));
					return true;
				}
				else{
					System.out.println("Response Message : Please check the response. Wasn't able to find ID for the audience.");
					return false;
				}
					
			}
			else{
				System.out.println("Response Message : Request for Facebook Product Audience Creation Failed.");
				return false;
			}
				
		}
		catch(Exception e){
				System.out.println("Exception : ProductAudienceCreate - createProductAudience Method");
				System.out.println(e);
				return false;
		}
		
	}
		
}