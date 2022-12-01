from dotenv import load_dotenv
load_dotenv()
import os
import ftplib
import json
import csv
import logging
import logzero
import numpy as np
import pandas as pd
import pyodbc
from datetime import date
import requests
import json
from pandas.io.json import json_normalize


def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    print(response)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    return response     

def markWrikeTaskComplete (taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/"
    querystring = {
        'status':'Completed'
        }     
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    response = requests.request("PUT", url, headers=headers, params=querystring)
    return response         

def makeWrikeTaskSubtask (taskid, parenttaskid):
    
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/" + "?addSuperTasks=['" + parenttaskid + "']"

    payload={}
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    response = requests.request("PUT", url, headers=headers, data=payload)
    return response    

def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out    

if __name__ == '__main__':
    logzero.loglevel(logging.WARN)

    ihadanerror = ''
        
    conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";") 

    cnxn = pyodbc.connect(conn_str, autocommit=True)

    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        sys.path.append("..")
        from akeneo_api_client.client import Client

    #logf = open("GoogleFeedError.log", "w")
    try:
        print("connnected")

        AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
        AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
        AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
        AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
        AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")


        akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,
                        AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)
        google_feed_attributes = akeneo.attribute_groups.fetch_item('google_feed')['attributes'] # The attributes you'd like to pull from the system

        google_feed_attributes.append('Title70')
        google_feed_attributes.append('Title150')
        google_feed_attributes.append('AdditionalImages')
        google_feed_attributes.append('DisplayName')
        google_feed_attributes.append('ImageUrl')
        google_feed_attributes.append('ProductUrl')
        google_feed_attributes.append('Brand')
        google_feed_attributes.append('visibility')
        

        akeneo_att_string = ','.join(google_feed_attributes)

        google_feed_attributes.append('identifier')        
        google_feed_attributes.append('Brand_linked_labels_en_US')        
        google_feed_attributes.append('visibility_linked_labels_en_US')   
        
        print(google_feed_attributes)
        pandaObject = pd.DataFrame(data=None, columns=google_feed_attributes)
        pandaObject.set_index('identifier',inplace=True)

        searchparams = """
        {
            "limit": 100,
            "locales": "en_US",
            "scope": "Google",
            "with_count": true,
            "with_attribute_options": true,
            "attributes": "search_atts",
            "search": {
                "completeness": [{
                    "operator": "=",
                    "value": 100,
                    "scope": "Google"
                }],
                "groups": [{
                    "operator": "NOT IN",
                    "value": ["google_ChannelExclusion"]
                }],        
                "ImageUrl": [{
                    "operator": "DOES NOT CONTAIN",
                    "value": "no-image"
                }],  
                "ProductUrl":[{
                    "operator": "DOES NOT CONTAIN",
                    "value": "pricelist"
                }],                                                                
                "enabled": [{
                    "operator": "=",
                    "value": true
                }]
            }
        }
        """.replace('search_atts',akeneo_att_string)    

        result = akeneo.products.fetch_list(json.loads(searchparams))

        go_on = True
        count = 0
        #for i in range(1,8):
        while go_on:
            count += 1
            try:
                print(str(count) + ": normalizing")
                page = result.get_page_items()
                #print(page)
                pagedf = pd.DataFrame([flatten_json(x,['scope','locale','currency','unit']) for x in page])
                pagedf.columns = pagedf.columns.str.replace('values_','')
                pagedf.columns = pagedf.columns.str.replace('_0','')
                pagedf.columns = pagedf.columns.str.replace('_data','')
                pagedf.columns = pagedf.columns.str.replace('_amount','')
                pagedf.drop(pagedf.columns.difference(google_feed_attributes), 1, inplace=True)
                pandaObject = pandaObject.append(pagedf, sort=False)
            except:
                #print(item)
                go_on = False
                break
            go_on = result.fetch_next_page()

        
        pandaObject.columns = pandaObject.columns.str.replace('google_','')
        pandaObject.columns = pandaObject.columns.str.replace('identifier','ItemCode')
        pandaObject.columns = pandaObject.columns.str.replace('product_category','google_product_category')

        pandaObject.to_csv('\\\\FOT00WEB\\Alt Team\\Akeneo\\API Testing\\RawGoogleFeedsViaAkeneo.txt', header=True, sep='\t', index=False)

        print("SqL")
        sql = """SELECT CI_Item.ItemCode, CI_Item.SuggestedRetailPrice, CI_Item.StandardUnitPrice, CI_Item.UDF_SPECIALORDER, CI_Item.UDF_ON_CLEARANCE,
                 CI_Item.ShipWeight, CI_Item.PrimaryVendorNo, CI_Item.ProductType, CI_Item.ProductLine, CI_Item.UDF_UPC, CI_Item.UDF_GTIN14, CI_Item.UDF_MANUFACTURER, 
                 CI_Item.UDF_WEB_DISPLAY_MODEL_NUMBER, CI_Item.UDF_MAP_PRICE, CI_Item.UDF_CALL, CI_Item.UDF_WEB_DISABLED
                 FROM CI_Item CI_Item 
                 WHERE CI_Item.InactiveItem <> 'Y'
                 """
        #Pull SQL
        SageQueryDF = pd.read_sql(sql,cnxn,index_col='ItemCode')
        #Merge Akeneo Data with Sage Data
        pandaObject = pandaObject.merge(SageQueryDF,on='ItemCode')
        pandaObject = pandaObject.replace(r'^\s*$', np.nan, regex=True)
        #Column Renames
        pandaObject.columns = pandaObject.columns.str.replace('StandardUnitPrice','sale_price')
        pandaObject.columns = pandaObject.columns.str.replace('SuggestedRetailPrice','price')  
        #Remove items not ready for the feed
        pandaObject = pandaObject.query("UDF_SPECIALORDER != 'Y'")
        pandaObject = pandaObject.query("sale_price != 0")
        pandaObject = pandaObject.query("UDF_CALL != 'Y'")
        print(pandaObject)

        #Data Transformations
        pandaObject.loc[pandaObject["ProductLine"].str.startswith('U', na=False), 'PrimaryVendorNo'] = "Refurbished"

        pandaObject.loc[pandaObject["ProductLine"].str.startswith('U', na=False), 'condition'] = "Refurbished"
        pandaObject.loc[pandaObject["ProductLine"].str.startswith('N', na=False), 'condition'] = "New"

        pandaObject.loc[pandaObject["ProductLine"].str.startswith('U', na=False), 'PrimaryVendorNo'] = "NOF"
        pandaObject.loc[pandaObject["ProductLine"].str.startswith('DD', na=False), 'PrimaryVendorNo'] = "NOF"
    
        pandaObject.loc[((pandaObject['UDF_ON_CLEARANCE'] == 'Y') & pandaObject['custom_label_0'].isna()), 'custom_label_0'] = "Clearance"

        pandaObject.loc[(pandaObject['sale_price'] > 500), 'custom_label_1'] = ">500"
        pandaObject.loc[(pandaObject['sale_price'] <= 500), 'custom_label_1'] = "200-500"
        pandaObject.loc[(pandaObject['sale_price'] <= 200), 'custom_label_1'] = "100-200"
        pandaObject.loc[(pandaObject['sale_price'] <= 100), 'custom_label_1'] = "50-100"
        pandaObject.loc[(pandaObject['sale_price'] <= 50), 'custom_label_1'] = "25-50"
        pandaObject.loc[(pandaObject['sale_price'] < 25), 'custom_label_1'] = "<25"

        pandaObject.loc[(pandaObject['sale_price'] > 50), 'custom_label_3'] = "Over50"
        pandaObject.loc[(pandaObject['sale_price'] <= 50), 'custom_label_3'] = "Under50"        

        pandaObject.loc[(pandaObject['sale_price'] > 50), 'adwords_labels'] = "Over50"
        pandaObject.loc[(pandaObject['sale_price'] <= 50), 'adwords_labels'] = "Under50"

        pandaObject['shipping_weight'] = pandaObject['ShipWeight'] 

        pandaObject['image_link'] = pandaObject['ImageUrl'] 

        pandaObject['availability'] = 'In Stock'

        pandaObject['link'] = pandaObject['ProductUrl'] + "?ref=gbase"    
        
        pandaObject['description'] = pandaObject['Title150'] 

        pandaObject['title'] = pandaObject['Title70'] 

        pandaObject['gtin'].fillna(pandaObject['UDF_UPC'],inplace=True)

        pandaObject['gtin'].fillna(pandaObject['UDF_GTIN14'],inplace=True)

        pandaObject['id'].fillna(pandaObject['ItemCode'],inplace=True)

        pandaObject['brand'].fillna(pandaObject['Brand_linked_labels_en_US'],inplace=True)     

        pandaObject['product_type'].fillna(pandaObject['google_product_category'].str.split(' > ',expand=True)[0],inplace=True)

        #Backfill missing data
        pandaObject['mpn'].fillna(pandaObject['UDF_WEB_DISPLAY_MODEL_NUMBER'],inplace=True)
        pandaObject['mpn'].fillna(pandaObject['DisplayName'],inplace=True)        

        #Remove items not ready for the feed
        pandaObject = pandaObject.dropna(subset=['PrimaryVendorNo'])
        pandaObject = pandaObject.dropna(subset=['title'])
        pandaObject = pandaObject.dropna(subset=['link'])        
        pandaObject = pandaObject.dropna(subset=['brand'])    

        #Megger MAP price snowflake situation (MAP Violating)
        pandaObject.loc[((pandaObject['ProductLine'] == 'NMEG') & (pandaObject['price'] >= 500)),'sale_price'] = pandaObject['price']

        #If List price is less then our current price...set both to our current price (as it shows on the website)
        pandaObject.loc[(pandaObject['price'] < pandaObject['sale_price']),'price'] = pandaObject['sale_price']

        #If Sale Price = List price...make 'Sale' blank
        pandaObject.loc[(pandaObject['price'] == pandaObject['sale_price']),'sale_price'] = np.nan

        #Rounding :)
        pandaObject['price'] = pandaObject['price'].round(2)
        pandaObject['sale_price'] = pandaObject['sale_price'].round(2)

        #For Google Feed
        pandaObject.set_index('id',inplace=True)

        #Make text file dump for debugging
        pandaObject.replace(r'\n',' ', regex=True).to_csv('\\\\FOT00WEB\\Alt Team\\Akeneo\\API Testing\\RawGoogleFeedsViaAkeneo2.txt', header=True, sep='\t', index=True, quoting=csv.QUOTE_ALL)

        #Make text file dump for FTPing
        pandaObject.replace(r'\n',' ', regex=True).to_csv('\\\\FOT00WEB\\Alt Team\\Akeneo\\API Testing\\testequipmentdepot1.txt', header=True, sep='\t', index=True, quoting=csv.QUOTE_ALL,
            columns = ['title','description','google_product_category','product_type','link','image_link','condition','availability','brand','gtin','mpn','adwords_labels','item_group_id','custom_label_0','custom_label_1','custom_label_2','custom_label_3','custom_label_4','promotion_id','additional_image_link','is_bundle','shipping_weight','price','sale_price'])

        #FTP that Bad Boy
        session = ftplib.FTP('uploads.google.com',os.environ.get("GOOGLE_FEED_LOGIN"),os.environ.get("GOOGLE_FEED_PW"))
        file = open('\\\\FOT00WEB\\Alt Team\\Akeneo\\API Testing\\testequipmentdepot1.txt','rb')
        session.storbinary('STOR testequipmentdepot1.txt', file)
        file.close()
        session.quit()        

        print('GoogleFeedSent!')

        #BING FEED SHEEEEEET
        #we try because bing ftp will reject if attempt multiple upload in a short time frame (which is very unclear)
        try:
            bingFeedDF = pandaObject.rename(columns = {'ItemCode':'SKU'})

            print("Bingads_grouping")
            bingFeedDF['Bingads_grouping'] = np.nan
            bingFeedDF.loc[(bingFeedDF['UDF_ON_CLEARANCE'] == 'Y'),'Bingads_grouping'] = 'clearance'
            bingFeedDF['Bingads_grouping'].fillna('Standard', inplace=True)

            print("Bingads_label")
            bingFeedDF['Bingads_label'] = ''
            
            print("Bingads_label2")
            bingFeedDF.loc[(bingFeedDF['price'] > 500), 'Bingads_label'] = "Over500"
            bingFeedDF.loc[(bingFeedDF['price'] <= 500), 'Bingads_label'] = "200to500"
            bingFeedDF.loc[(bingFeedDF['price'] <= 200), 'Bingads_label'] = "100to200"
            bingFeedDF.loc[(bingFeedDF['price'] <= 100), 'Bingads_label'] = "50to100"
            bingFeedDF.loc[(bingFeedDF['price'] <= 50), 'Bingads_label'] = "25to50"
            bingFeedDF.loc[(bingFeedDF['price'] < 25), 'Bingads_label'] = "Under25"

            print("customlabel0")
            bingFeedDF['customlabel0'] = bingFeedDF['Bingads_grouping']
            bingFeedDF['customlabel1'] = bingFeedDF['Bingads_label']
            bingFeedDF['customlabel2'] = 'regular'
            bingFeedDF['customlabel3'] = ''
            bingFeedDF['customlabel4'] = ''

            print("link")
            bingFeedDF['link'] = "https://" + bingFeedDF['link'].str.replace(r'&ref=gbase', '')

            #Making bing feed csv
            bingFeedDF.replace(r'\n',' ', regex=True).to_csv('\\\\FOT00WEB\\Alt Team\\Akeneo\\API Testing\\TestEquipmentDepot.txt', header=True, sep='\t', index=True, quoting=csv.QUOTE_ALL, 
                                columns=['title','description','google_product_category','product_type','link','image_link','condition','availability','price',
                                'sale_price','brand','gtin','mpn','SKU','customlabel0','customlabel1','customlabel2','customlabel3','customlabel4'])
        
            #FTP to Bing
            session = ftplib.FTP('feeds.adcenter.microsoft.com',os.environ.get("BING_FEED_LOGIN"),os.environ.get("BING_FEED_PW"))
            file = open('\\\\FOT00WEB\\Alt Team\\Akeneo\\API Testing\\TestEquipmentDepot.txt','rb')
            session.storbinary('STOR TestEquipmentDepot.txt', file)
            file.close()
            session.quit()   

            print('bing is done!')
        except:
            print('silly BING')


        #octopsart...we only send what we have on hand
        sql = """SELECT ItemCode, QuantityOnHand, ReorderPointQty
                 FROM IM_ItemWarehouse 
                 WHERE WarehouseCode = '000' AND QuantityOnHand > 0 AND ReorderPointQty > 0
                 """
        SageQueryDF = pd.read_sql(sql,cnxn,index_col='ItemCode')
        print(SageQueryDF)
        octopartFeedDF = pandaObject.merge(SageQueryDF, on='ItemCode', how='left')
        print('merged')
        octopartFeedDF = octopartFeedDF[~octopartFeedDF.link.astype(str).str.contains(r'&ref=gbase')]
        print('gbase')
        
        print('link')
        octopartFeedDF['link'] = 'https://' + octopartFeedDF['link'] + '?utm_source=octopart&utm_campaign=' + octopartFeedDF['ItemCode'] + '&utm_medium=cpc'
        print('QuantityOnHand')

        octopartFeedDF.loc[(octopartFeedDF['QuantityOnHand'] <= 0),'QuantityOnHand'] = -2
        octopartFeedDF['QuantityOnHand'].fillna(-2, inplace=True)
        octopartFeedDF['QuantityOnHand'] = octopartFeedDF['QuantityOnHand'].round(0)
        print('US')
        octopartFeedDF['eligible-region'] = 'US'
        octopartFeedDF['price-break-1'] = 1
        print('price')
        octopartFeedDF['price-usd-1'] = octopartFeedDF['price'].round(2)
        octopartFeedDF.loc[(octopartFeedDF['sale_price'] < octopartFeedDF['price-usd-1']),'price-usd-1'] = octopartFeedDF['sale_price'].round(2)

        octopartFeedDF = octopartFeedDF.rename(columns = {'ItemCode':'sku','brand':'manufacturer','link':'distributor-url','image_link':'image-url','QuantityOnHand':'quantity'})
        
        octopartFeedDF = octopartFeedDF.dropna(subset=['image-url'])
        octopartFeedDF = octopartFeedDF.dropna(subset=['distributor-url'])
        octopartFeedDF = octopartFeedDF.dropna(subset=['ReorderPointQty'])

        octopartFeedDF.replace(r'\n',' ', regex=True).to_csv('\\\\FOT00WEB\\Alt Team\\Akeneo\\API Testing\\OctopartFeed.tsv', header=True, sep='\t', index=False, quoting=csv.QUOTE_ALL, 
                            columns=['manufacturer','eligible-region','mpn','sku','distributor-url','description','image-url','quantity','price-break-1','price-usd-1'])

        session = ftplib.FTP('feeds.octopart.com',os.environ.get("OCTOPART_FEED_LOGIN"),os.environ.get("OCTOPART_FEED_PW"))
        file = open(r'\\FOT00WEB\Alt Team\Akeneo\API Testing\OctopartFeed.tsv','rb')
        session.storbinary('STOR OctopartFeed.tsv', file)
        file.close()
        session.quit()   
        assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]'
        description = "Telling u about the Google Feed "        

    except Exception as e: 
        print(e)
        ihadanerror = 'Error'
        folderid = 'IEAAJKV3I4KM3YOP' 
        assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]'
        wrikedescription = "Telling u about the Advertising Feeds"
        wriketitle = date.today().strftime('%Y-%m-%d')+ " - Google, Bing, Octopart Feeds ERRORED"         
        response = makeWrikeTask(title = wriketitle, description = wrikedescription, assignees = assignees, folderid = folderid)
        response_dict = json.loads(response.text)
        print('wrike task made!')
        taskid = response_dict['data'][0]['id']        
        print('error :(')     

    finally:
        print('Making Wrike notication of completed Google Feed...') 
        folderid = 'IEAAJKV3I4KM3YOP' 
        assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]'
        wrikedescription = "Telling u about the Advertising Feeds"
        wriketitle = date.today().strftime('%Y-%m-%d')+ " - Google, Bing, Octopart Feeds successful!"        
        response = makeWrikeTask(title = wriketitle, description = wrikedescription, assignees = assignees, folderid = folderid)
        response_dict = json.loads(response.text)
        print('wrike task made!')
        taskid = response_dict['data'][0]['id']        
        markWrikeTaskComplete (taskid)
        print('sent')    
            