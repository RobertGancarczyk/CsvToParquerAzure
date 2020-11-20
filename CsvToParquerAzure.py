import uuid
from datetime import date, datetime, timedelta
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions, BlobServiceClient
import pandas as pd
import snappy
import sys

try:
#DATA INPUT
    account_name = "rcmsdwh"
    connect_str = "DefaultEndpointsProtocol=https;AccountName=rcmsdwh;AccountKey=omcjHeDdTpe+fR8e5q2ZuKUO47Db/T3/YCWheMlgVC14BaRAUimL93xmO51CkdQ+KoG8iR6VfuQ8VMbuREtRBQ==;EndpointSuffix=core.windows.net"
    primary_key = "omcjHeDdTpe+fR8e5q2ZuKUO47Db/T3/YCWheMlgVC14BaRAUimL93xmO51CkdQ+KoG8iR6VfuQ8VMbuREtRBQ=="
    container = "testcontainer"

#INPUT
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container)
    for blob in container_client.list_blobs():
        if '/' in blob.name:
            pass
        elif blob.name[-4:] == '.csv':
            file_name = blob.name
            break
        else:
            blob_client = blob_service_client.get_blob_client(container=container, blob='wrong_file_type/'+blob.name)
            blob_client.upload_blob(blob_client.url, overwrite=True)
            blob_client = blob_service_client.get_blob_client(container=container, blob=blob.name)
            blob_client.delete_blob()
            sys.exit()
    blob_client = blob_service_client.get_blob_client(container=container, blob=file_name)

#BACKUP
    date_id = "_" + str(date.today()) + "_" + str(uuid.uuid4()) + "."
    backup_file_name = 'backup/' + str.replace(file_name ,'.', date_id)
    backup_blob_client = blob_service_client.get_blob_client(container=container, blob=backup_file_name)
    backup_blob_client.upload_blob(blob_client.url)

#CONVERSION
    #Get SAS URL for input file
    def get_blob_sas(account_name,account_key, container_name, blob_name):
        sas_blob = generate_blob_sas(account_name=account_name, 
                                     container_name=container,
                                     blob_name=file_name,
                                     account_key=primary_key,
                                     permission=BlobSasPermissions(read=True),
                                     expiry=datetime.utcnow() + timedelta(hours=1))
        return sas_blob
    blob = get_blob_sas(account_name,primary_key, container, file_name)
    url = 'https://'+account_name+'.blob.core.windows.net/'+container+'/'+file_name+'?'+blob
    #Load csv and convert it to parquet
    df = pd.read_csv(url, sep=';')
    df['date'] = datetime.today().strftime('%Y-%m-%d') #add date colume
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns] #strip columes names nad removes spaces
    df.to_parquet('df.parquet.snappy')

#OUTPUT
    n = 1
    output_file_name = 'output/'+file_name
    while True:
        try:
            output_blob_client = blob_service_client.get_blob_client(container=container, 
                                                                     blob=str.replace(output_file_name, 
                                                                                      output_file_name[-4:],
                                                                                     '.parquet'))
            with open('df.parquet.snappy', 'rb') as data:
                output_blob_client.upload_blob(data)
            blob_client.delete_blob()
            break
        except Exception:
            output_file_name = 'output/'+str.replace(file_name, 
                                                     file_name[-4:], 
                                                     '(' + str(n) + ')' + '.csv')
            n += 1
            pass

except Exception as ex:
    print('Exception:')
    print(ex)
