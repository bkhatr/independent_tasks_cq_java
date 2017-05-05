package com.standard.core.scheduler;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.InvalidQueryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.net.ssl.HttpsURLConnection;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.scr.annotations.Property;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@Component(immediate = true, metatype = true, label = "Price Update Scheduler")
@Service(value = Runnable.class)
@Property( name = "scheduler.period", longValue = 10)
public class PriceUpdateScheduler implements Runnable {
    /** Default log. */
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
 

    private static final String GET_PRICE_URL = "https://mkonnect.havells.com:8082/xmwgw/rfctojson?aclclientid=ECOMM&IM_FLAG=R&IM_PROJECT=ECOMM&bapiname=ZBAPI_CHANGE_MRP_UPDATE";
   
    private static final String POST_PRICE_UPDATE_URL = "https://mkonnect.havells.com:8082/xmwgw/rfctojson";
    
    private static final String POST_PRICE_UPDATE_PARAMETER = "aclclientid=ECOMM&IM_FLAG=U&IM_PROJECT=ECOMM&bapiname=ZBAPI_CHANGE_MRP_UPDATE";
    
    private static final String SHOP_HAVELLS_URL = "https://shop.havells.com/ServiceM/processProduct";
    
    private static final String CONTENT_TYPE_PROPERTY = "Content-Type";
    
    private static final String CONTENT_TYPE_VALUE = "application/x-www-form-urlencoded";
    
    private static final String CLIENT_ID_PROPERTY = "client_id";
    
    private static final String CLIENT_ID_VALUE = "bca13773e8aa97af817dd877fb4af867";
    
    private static final String SECRET_PROPERTY = "secret";
    
    private static final String SECRET_VALUE = "df7ecdc4964b5a1670958444b822be39";
    
   
    @Reference
    private ResourceResolverFactory resolverFactory;
    
    private ResourceResolver resourceResolver;
    
    private Session session;
    
   
	
    public ResourceResolver getAdminResourceResolver() {
        ResourceResolver resourceResolver = null;
        try {
            Map<String,Object> authInfo = new HashMap<String,Object>();
            authInfo.put(ResourceResolverFactory.SUBSERVICE, "adminResourceResolver");
            resourceResolver = resolverFactory.getServiceResourceResolver(authInfo);
        } catch(LoginException exe) {
        	log.error("Exception while getting resource resolver." + exe);
        }
        return resourceResolver;
    }
    
    public void run() {
    	resourceResolver = getAdminResourceResolver();
    	session = resourceResolver.adaptTo(Session.class);
    	
    	JSONArray priceUpdateJson = getPriceUpdateData();
    	log.info("Executing a cron job to update product price");
    	boolean serverUpdated = false;
    	
    	if(priceUpdateJson != null) {
    		
    		for(int i=0;i<priceUpdateJson.length();i++) {
    			String sku =  null;
    			String price = null;
    			try {
					JSONObject jsonObject = priceUpdateJson.getJSONObject(i);
					if(jsonObject.has("SKU")) {
						sku = jsonObject.getString("SKU");
					}
					if(jsonObject.has("PRICE")) {
						price = jsonObject.getString("PRICE");
					}
					if(sku != null && price != null) {
						String shopUrl = updateAndGetShopURL(sku);
						updateProductPrices(sku, price, shopUrl);
					}
				} catch (JSONException exception) {
					 log.error("Excpetion updating product price",exception);
				}
    		}
    		serverUpdated = updateServerForSuccess();
    	}
    	
    	if(serverUpdated) {
    		log.info("Server is updated after fetching sku prices successfully");
    	}
       
    }
    
    public JSONArray getPriceUpdateData() {
    	JSONObject jsonObject = null;
    	JSONArray priceJsonArray = null;
    	try {
	        URL myurl = new URL(GET_PRICE_URL);
	        HttpsURLConnection con = (HttpsURLConnection)myurl.openConnection();
	        InputStream ins = con.getInputStream();
	        InputStreamReader isr = new InputStreamReader(ins);
	        BufferedReader in = new BufferedReader(isr);
	
	        String inputLine;
	        String jsonString = "";
	        while ((inputLine = in.readLine()) != null)
	        {
	          jsonString+=inputLine;
	        }
	        in.close();
			
	       
	        if(!jsonString.equals("")) {
	        	jsonObject = new JSONObject(jsonString);
	        	Object object = jsonObject.get("LT_MRP_DETAILS");
	        	
	        	if(object instanceof JSONArray) {
	        		priceJsonArray = (JSONArray) object;
	        	}
	        	
	        }
    	} catch (JSONException exception) {
    		log.error("Exception while creating json object." + exception);
		} catch (IOException exception) {
			log.error("Exception while calling REST Service for price update." + exception);
		} catch (Exception exception) {
			log.error("Exception while calling REST Service for price update." + exception);
		}
        return priceJsonArray;
    }
    
    public boolean updateServerForSuccess() {
    	boolean success = false;
    	try {
	        URL myurl = new URL(POST_PRICE_UPDATE_URL);
	        HttpsURLConnection con = (HttpsURLConnection)myurl.openConnection();
	        con.setRequestMethod("POST");
	        
	        // Send post request
			con.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.writeBytes(POST_PRICE_UPDATE_PARAMETER);
			wr.flush();
			wr.close();
			
			int responseCode = con.getResponseCode();
			
	        if(responseCode == 200) {
	        	success=true;
	        }
    	} catch (IOException exception) {
			log.error("Exception while calling REST Service for price update." + exception);
		} catch (Exception exception) {
			log.error("Exception while calling REST Service for price update." + exception);
		}
        return success;
    }
    
    public String updateAndGetShopURL(String sku) {
    	String shopUrl= "";
    	try {
	        URL myurl = new URL(SHOP_HAVELLS_URL);
	        HttpsURLConnection con = (HttpsURLConnection)myurl.openConnection();
	        con.setRequestMethod("POST");
	        con.setRequestProperty(CONTENT_TYPE_PROPERTY,CONTENT_TYPE_VALUE);
			con.setRequestProperty(CLIENT_ID_PROPERTY, CLIENT_ID_VALUE);
			con.setRequestProperty(SECRET_PROPERTY, SECRET_VALUE);
	        
	        // Send post request
			con.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.writeBytes("data={"+"\"sku\":\""+sku+"\"|");
			wr.flush();
			wr.close();
			
		
			InputStream ins = con.getInputStream();
	        InputStreamReader isr = new InputStreamReader(ins);
	        BufferedReader in = new BufferedReader(isr);
	
	        String inputLine;
	        String jsonString = "";
	        while ((inputLine = in.readLine()) != null)
	        {
	          jsonString+=inputLine;
	        }
	        in.close();
			
	       
	        if(!jsonString.equals("")) {
	        	JSONObject jsonObject = new JSONObject(jsonString);
	        	
	        	if(jsonObject.has("url")) {
	        		shopUrl = jsonObject.getString("url");
	        	}

	        }
    	} catch (IOException exception) {
			log.error("Exception while calling REST Service for price update." + exception);
		} catch (Exception exception) {
			log.error("Exception while calling REST Service for price update." + exception);
		}
        return shopUrl;
    }
    
    public void updateProductPrices(final String sku, final String price, final String shopURL) {
    
    	String queryString = "SELECT * FROM [nt:unstructured] AS node WHERE ISDESCENDANTNODE(node, '/etc/commerce/products/havells/') AND NAME() = '"+sku.toLowerCase()+"'";
 	    
 	    Query query=null;
 	    String productPath = "";
			try {
				query = session.getWorkspace().getQueryManager().createQuery(queryString,Query.JCR_SQL2);
				QueryResult results = query.execute();
				
				if(results != null) {
					Row r = results.getRows().nextRow();
					productPath = r.getPath();
					Resource productResource = resourceResolver.getResource(productPath);
					if(productResource != null) {
						Node productNode = productResource.adaptTo(Node.class);
						String oldPrice = "";
						if(productNode.hasProperty("price")) {
							oldPrice = productNode.getProperty("price").getString();
						}
						productNode.setProperty("price", price);
						log.info("Product SKU : " + sku +", Product new price: " + price + ", Old Price: "+ oldPrice +", Product Path: " + productPath );
						
						String oldShopUrl = "";
						if(productNode.hasProperty("shopurl")) {
							oldShopUrl = productNode.getProperty("shopurl").getString();
						}
						productNode.setProperty("shopurl", shopURL);
						log.info("Product SKU : " + sku +", Product new shop url: " + shopURL + ", Old Shop Url: "+ oldShopUrl +", Product Path: " + productPath );
						
						
						session.save();
					}
				}
			} catch (InvalidQueryException e) {
				log.error("Invalid Query Exception", e.getMessage());
			} catch (RepositoryException e) {
				log.error("Repository Exception", e.getMessage());
			} catch (Exception exception) {
				log.error("Exception while storing information in product node." + exception);
			}
    }
 
}