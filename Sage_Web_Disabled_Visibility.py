from dotenv import load_dotenv
load_dotenv()
import os
from datetime import date
from subprocess import Popen
import time
import pandas as pd
import pyodbc
import numpy as np
import requests
import logzero
import logging
import json
import datetime
from sqlalchemy import column

def printVisibilities(reporting = "", printdf = pd.DataFrame):
    print('/n')
    print('/n')
    print(reporting)
    print("Total Disabled = " + str(sum(printdf.UDF_WEB_DISABLED_Calc == 'Y'))) 
    print("Total Active = " + str(sum(printdf.UDF_WEB_DISABLED_Calc == 'N'))) 
    print("Total not Visibile = " + str(sum(printdf.Visibility_Calc == 1))) 
    print("Total Catalog = " + str(sum(printdf.Visibility_Calc == 2)))
    print("Total Search = " + str(sum(printdf.Visibility_Calc == 3)))
    print("Total Search, Catalog = " + str(sum(printdf.Visibility_Calc == 4)))

def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object
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

def make_json_attribute_data_nest(row, column_name, unit, currency):
    if row[column_name] is None or row[column_name] is np.nan or str(row[column_name]) == 'nan':
        # or str(row[column_name]) == ''
        row[column_name] = np.nan  
    elif type(row[column_name]) != list:
        if isinstance(row[column_name], bool):
            d = row[column_name]
        elif not isinstance(row[column_name], str):
            d = str(row[column_name]).encode().decode()
        else:
            d = row[column_name].encode().decode()
        if unit is not None and currency is None:
            if row[column_name] == '':
                row[column_name] = np.nan
                return row
            else:
                d = np.array({"amount":d,"unit":unit}).tolist()
        elif unit is None and currency is not None:
            d = [np.array({"amount":d,"currency":currency}).tolist()]
        d = {"data":d,"locale":None,"scope":None}
        row[column_name] = [d]
    return row        

#This is the basic info for making wrike tasks.

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUALCDZR", folderid = "IEAAJKV3I4JBAOZD"):
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


if __name__ == '__main__':

    print("Connecting to Akeneo...")
    pd.options.display.max_colwidth = 9999

    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        sys.path.append("..")
        from akeneo_api_client.client import Client

    #Stored in .env
    AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
    AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")

    #Establish akeneo API client
    akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,
                    AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)    

    logzero.loglevel(logging.WARN)


    AKattributes = [
        'visibility',
        'magento2_visibility_override',
        'magento2_web_disabled_override',
        'URL_Key',
        'DisplayName'
    ]

    # Repurpose for the Akeneo attributes required
    akeneo_df = pd.DataFrame(data=None, columns=AKattributes + ['identifier'])

    akeneo_att_string = ','.join(AKattributes)  

    searchparams = """
    {
        "limit": 100,
        "scope": "ecommerce",
        "with_count": true,
        "attributes": "search_atts"
    }
    """.replace('search_atts',akeneo_att_string)      

    result = akeneo.products.fetch_list(json.loads(searchparams))   

    go_on = True
    
    akeneopagecounter = 1
    #for i in range(1,3):
    while go_on:
        try:
            page = result.get_page_items()
            print(akeneopagecounter)
            page_df = pd.DataFrame([flatten_json(x,['scope','locale','currency','unit']) for x in page])
            page_df.columns = page_df.columns.str.replace('values_','')
            page_df.columns = page_df.columns.str.replace('_0','')
            page_df.columns = page_df.columns.str.replace('_data','')
            page_df.columns = page_df.columns.str.replace('_amount','')
            page_df.drop(page_df.columns.difference(AKattributes + ['identifier']), 1, inplace=True)
            akeneo_df = akeneo_df.append(page_df, sort=False)
            akeneopagecounter +=1
        except:
            # print(item)
            go_on = False
            break
        go_on = result.fetch_next_page()
    
    print(akeneo_df)
    akeneo_df.to_csv(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\web-disabled (Sage to Sage)\AkeneoWebDisabledPull.csv', header=True, sep=',', index=True) 

    akeneo_df = pd.read_csv(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\web-disabled (Sage to Sage)\AkeneoWebDisabledPull.csv') 
    print(akeneo_df)


    #Get Sage Data
    #This is the connection string to Sage.
    conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";") 
            
    #This makes the connection to Sage based on the string above.
    cnxn = pyodbc.connect(conn_str, autocommit=True)

    #This is responsible for selecting what data to pull from Sage.
    sql = """SELECT 
                CI_Item.ItemCode, 
                CI_Item.UDF_REPLACEMENT_ITEM, 
                CI_Item.UDF_VENDOR_PRICE_DATE, 
                CI_Item.InactiveItem, 
                CI_Item.ProductType, 
                CI_Item.ProductLine, 
                CI_Item.PrimaryVendorNo, 
                CI_Item.UDF_DISCONTINUED_STATUS, 
                CI_Item.UDF_SPECIALORDER, 
                CI_Item.UDF_WEB_DISABLED,
                CI_Item.LastSoldDate,
                CI_Item.LastReceiptDate
            FROM 
                CI_Item CI_Item
    """
    df = pd.read_sql(sql,cnxn)
    print(df)

    #Goal Posts 
    Six_Months_ago = datetime.datetime.now() - datetime.timedelta(days=182)
    One_Year_Ago = datetime.datetime.now() - datetime.timedelta(days=365)
    Two_Years_Ago = datetime.datetime.now() - datetime.timedelta(days=2*365)
    Three_Years_Ago = datetime.datetime.now() - datetime.timedelta(days=3*365)
    Five_Years_Ago = datetime.datetime.now() - datetime.timedelta(days=5*365)
    Ten_Years_ago = datetime.datetime.now() - datetime.timedelta(days=10*365)

    ##############
    #Dataframe Prepping
    #Fill nulls with 'N' for consistancy
    for x in ['InactiveItem','UDF_SPECIALORDER','UDF_WEB_DISABLED']:
        df[x].fillna('N', inplace=True)

    #Fill in defaults
    df['UDF_DISCONTINUED_STATUS'].fillna('', inplace=True)
    df['LastSoldDate'].fillna(Ten_Years_ago, inplace=True)
    df['LastReceiptDate'].fillna(Ten_Years_ago, inplace=True)

    only_wanted_certain_columns_list = ['ReplacementStatus']

    #Figure Out Replacement's Status
    replacementlist = list(set(df['UDF_REPLACEMENT_ITEM']))
    ReplacementStatusDF = df.copy()
    ReplacementStatusDF = ReplacementStatusDF.reset_index(drop=True).set_index(['ItemCode'])
    ReplacementStatusDF = ReplacementStatusDF[ReplacementStatusDF['UDF_REPLACEMENT_ITEM'].isin(replacementlist)] 
    ReplacementStatusDF = ReplacementStatusDF.rename(columns={'InactiveItem':'ReplacementStatus'})  
    ReplacementStatusDF.drop(ReplacementStatusDF.columns.difference(only_wanted_certain_columns_list), 1, inplace=True)

    #Merge with Akeneo Data
    df = pd.merge(df,akeneo_df,how='outer',left_on='ItemCode',right_on='identifier')
    print(df)

    #print(ReplacementStatusDF)
    df = df.reset_index().set_index(['UDF_REPLACEMENT_ITEM'])
    df['ReplacementStatus'] = np.nan
    df.update(ReplacementStatusDF)
    df = df.reset_index()#.set_index(['ItemCode'])      

    #everything defaults as not disabled and visibility = 
    #   #1 = Not Visible Individually
    #   #2 = Catalog
    #   #3 = Search
    #   #4 = Catalog, Search
    df['UDF_WEB_DISABLED_Calc'] = 'N'
    df['Visibility_Calc'] = 4 
    printVisibilities('Starts', df)

    #Inactive
    #   -Under year Catalog, Search
    #   -1-3 year Search
    #   -Over 3 Year Invisible    
    df.loc[(df['InactiveItem'] == 'Y'), 'Visibility_Calc'] = 1
    df.loc[((df['InactiveItem'] == 'Y') & ((pd.to_datetime(df['LastSoldDate']) >= One_Year_Ago) | (pd.to_datetime(df['LastReceiptDate']) >= One_Year_Ago) | (pd.to_datetime(df['UDF_VENDOR_PRICE_DATE']) > One_Year_Ago))), 'Visibility_Calc'] = 4
    df.loc[((df['InactiveItem'] == 'Y') & ((pd.to_datetime(df['LastSoldDate']) < One_Year_Ago) | (pd.to_datetime(df['LastReceiptDate']) < One_Year_Ago) | (pd.to_datetime(df['UDF_VENDOR_PRICE_DATE']) > One_Year_Ago))), 'Visibility_Calc'] = 3
    df.loc[((df['InactiveItem'] == 'Y') & ((pd.to_datetime(df['LastSoldDate']) < Three_Years_Ago) | (pd.to_datetime(df['LastReceiptDate']) < Three_Years_Ago) | (pd.to_datetime(df['UDF_VENDOR_PRICE_DATE']) > Three_Years_Ago))), 'Visibility_Calc'] = 1
    printVisibilities('Default', df)

    #Inactive, Has Replacement - 
    #   -Under 2 years Catalog, Search
    #   -2-4 year Search
    #   -Over 5 Year Invisible
    df.loc[((~df['UDF_REPLACEMENT_ITEM'].isna()) & (df['InactiveItem'] == 'Y') & ((pd.to_datetime(df['LastSoldDate']) < Two_Years_Ago) | (pd.to_datetime(df['LastReceiptDate']) < Two_Years_Ago) | (pd.to_datetime(df['UDF_VENDOR_PRICE_DATE']) > Two_Years_Ago))), 'Visibility_Calc'] = 3
    df.loc[((~df['UDF_REPLACEMENT_ITEM'].isna())  & (df['InactiveItem'] == 'Y') & ((pd.to_datetime(df['LastSoldDate']) < Five_Years_Ago) | (pd.to_datetime(df['LastReceiptDate']) < Five_Years_Ago) | (pd.to_datetime(df['UDF_VENDOR_PRICE_DATE']) > Five_Years_Ago))), 'Visibility_Calc'] = 1
    printVisibilities('Inactive, Has active Replacement', df)

    #Makes anything invisible with temp disco searchable (will be in catalog too if replacement is active[below])
    df.loc[(((df['UDF_DISCONTINUED_STATUS'].str.contains("Temporarily")) | (df['UDF_DISCONTINUED_STATUS'].str.contains("Temporary"))) & (df['Visibility_Calc'] == 1)), 'Visibility_Calc'] = 3
    printVisibilities('Temporary', df)

    #Inactive, with page, has active replacement and activity within last 6 months increases visibility
    df.loc[((~df['URL_Key'].isna()) & (~df['UDF_REPLACEMENT_ITEM'].isna()) & (df['ReplacementStatus'] == 'N') & (df['Visibility_Calc'] == 3) & ((pd.to_datetime(df['LastSoldDate']) > One_Year_Ago) | (pd.to_datetime(df['LastReceiptDate']) > One_Year_Ago) | (pd.to_datetime(df['UDF_VENDOR_PRICE_DATE']) > Six_Months_ago))), 'Visibility_Calc'] = 4
    df.loc[((~df['URL_Key'].isna()) & (~df['UDF_REPLACEMENT_ITEM'].isna()) & (df['ReplacementStatus'] == 'N') & (df['Visibility_Calc'] == 1) & ((pd.to_datetime(df['LastSoldDate']) > One_Year_Ago) | (pd.to_datetime(df['LastReceiptDate']) > One_Year_Ago) | (pd.to_datetime(df['UDF_VENDOR_PRICE_DATE']) > Six_Months_ago))), 'Visibility_Calc'] = 3    
    printVisibilities('Replacements with Pages', df)

    #Stuff we never want to see
    df.loc[(df['UDF_SPECIALORDER'] == 'Y'), 'Visibility_Calc'] = 1
    #df.loc[(df['DisplayName'].isna()), 'Visibility_Calc'] = 1
    df.loc[df['UDF_DISCONTINUED_STATUS'].str.contains("Error") == True, 'Visibility_Calc'] = 1
    df.loc[df['ItemCode'].str.contains("/") == True, 'Visibility_Calc'] = 1
    df.loc[df['identifier'].str.contains("/") == True, 'Visibility_Calc'] = 1
    df.loc[df['identifier'].str.contains("-EBY") == True, 'Visibility_Calc'] = 1
    df.loc[df['identifier'].str.contains("-BVA") == True, 'Visibility_Calc'] = 1
    df.loc[df['identifier'].str.contains("-NOB") == True, 'Visibility_Calc'] = 1
    printVisibilities('Non-TED', df)

    #Do overrrides
    df.loc[((df['magento2_visibility_override'] != df['Visibility_Calc']) & (~df['magento2_visibility_override'].isna())), 'Visibility_Calc'] = df['magento2_visibility_override']
    printVisibilities('Visibility Overrides', df)

    #Anything Invisible is disabled
    df.loc[(df['Visibility_Calc'] == 1), 'UDF_WEB_DISABLED_Calc'] = 'Y'
    printVisibilities('Final Web Disabled', df)

    df.loc[((df['magento2_web_disabled_override'] == True) & (df['UDF_WEB_DISABLED_Calc'] == 'N') & (~df['magento2_web_disabled_override'].isna())), 'UDF_WEB_DISABLED_Calc'] = 'Y'
    df.loc[((df['magento2_web_disabled_override'] == False) & (df['UDF_WEB_DISABLED_Calc'] == 'Y') & (~df['magento2_web_disabled_override'].isna())), 'UDF_WEB_DISABLED_Calc'] = 'N'
    printVisibilities('Disabled Overrides', df)

    #dump full list for ad-hoc auditing
    df.to_csv(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\web-disabled (Sage to Sage)\WebDisabled_Visibility.csv', header=True, sep=',', index=True) 

    #Only stuff that's changing
    WebStatusCorrectionsDF = df.loc[((df['UDF_WEB_DISABLED'] != df['UDF_WEB_DISABLED_Calc']) & ~df['ItemCode'].isna())]#.rename(columns={"identifier": "ItemCode"})#.set_index('ItemCode', drop=True)    
    WebStatusCorrectionsDF = WebStatusCorrectionsDF.set_index('ItemCode', drop=True)  
    WebStatusCorrectionsDF.drop(labels=WebStatusCorrectionsDF.columns.difference(['UDF_WEB_DISABLED_Calc']), axis=1, inplace=True)

    print("Total Disabled Corrections")
    print(WebStatusCorrectionsDF.shape[0])

    if WebStatusCorrectionsDF.shape[0] > 0:
        #sage data batch file
        WebStatusCorrectionsDF.to_csv(r'\\FOT00WEB\Alt Team\Qarl\Automatic VI Jobs\Maintenance\CSVs\AA_WEB_STATUS_VIWI79.csv', header=False, sep=',', index=True) 
        print('to csv')
        time.sleep(20) 
        p = Popen('Auto_Web_Status_VIWI79.bat', cwd= 'Y:\\Qarl\\Automatic VI Jobs\\Maintenance', shell = True)
        stdout, stderr = p.communicate()   
        print('to sage done')
    else:
        print('nothing to sync')
    print('done!')    

    #Only stuff that's changing
    VisibilityCorrectionsDF = df.loc[((df['visibility'] != df['Visibility_Calc']) & ~df['identifier'].isna())]  
    VisibilityCorrectionsDF.drop(labels=VisibilityCorrectionsDF.columns.difference(['Visibility_Calc','identifier']), axis=1, inplace=True)
    VisibilityCorrectionsDF = VisibilityCorrectionsDF.rename({'Visibility_Calc':'visibility'}, axis=1)

    print("Total Visibility Corrections")
    print(VisibilityCorrectionsDF.shape[0])

    if VisibilityCorrectionsDF.shape[0] > 0:

        VisibilityCorrectionsDF = VisibilityCorrectionsDF.apply(make_json_attribute_data_nest, column_name = 'visibility', currency = None, unit = None, axis = 1)       
        valuesCols = ['visibility']

        #This is the real JSON magic
        #We make a data frame essentially 3 columns, and an Index [ Identifier, enabled, and all the json values (as the Akenoe API needs it)]
        jsonDF = (VisibilityCorrectionsDF.groupby(['identifier'], as_index=False)
                    .apply(lambda x: x[valuesCols].dropna(axis=1).to_dict('records'))
                    .reset_index()
                    .rename(columns={'':'values'}))    
        jsonDF.rename(columns={jsonDF.columns[2]: "values" }, inplace = True)                    
        jsonDF.drop(jsonDF.columns.difference(['values','identifier']), 1, inplace=True)    
        values_for_json = jsonDF.loc[:, ['identifier','values']].dropna(how='all',subset=['values']).to_dict(orient='records')    

        try:
            #we are sending the JSON now :D
            data_results = akeneo.products.update_create_list(values_for_json)   
            print("data results...")               
            print(data_results)   
        #if we failed to create JSON and/or send we log error and send it 
        except requests.exceptions.RequestException as api_error:
            print('ERROR!')
            print('ERROR!')
            print('ERROR!')
            load_failure = True
            #api_errors_file.write(str(api_error))    

        data_reponse_df = pd.DataFrame.from_dict(data_results)  
        print(data_reponse_df)    
        #Anything not in the 200 range is some sort of issue
        errordf = data_reponse_df.loc[(data_reponse_df["status_code"] > 299) | (data_reponse_df["status_code"] < 200)]

        if errordf.shape[0] > 0:
            print('some items did not pass akeneo validation')
            #shouldn't be a ton...so excel should be fine
            errordf.to_excel('errordf.xlsx')
            assignees = '[KUACOUUA]'#,KUALCDZR,KUAEL7RV]' # Andrew, Anthony
            folderid = 'IEAAJKV3I4JEW3BI' #Web Requests IEAAJKV3I4GOVKOA
            wrikedescription = ""
            wriketitle = "M2 Visibility Sync Error - " + date.today().strftime('%y/%m/%d') + "(" + str(errordf.shape[0]) + ")"
            response = makeWrikeTask(title = wriketitle, description = wrikedescription, assignees = assignees, folderid = folderid)
            response_dict = json.loads(response.text)
            taskid = response_dict['data'][0]['id']
            filetoattachpath = 'errordf.xlsx'
            print('Attaching file')
            attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
            print('File attached!')   
        else:
            print('no api data errors....Yay!')
    else:
        print('nothing to sync')
    print('done!')    