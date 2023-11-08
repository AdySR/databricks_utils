def authenticate_azure_adlsgen2_SP(storagename, service_principal_id, service_principal_secret, tenantid):
    spark.conf.set(f"fs.azure.account.auth.type.{storagename}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storagename}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storagename}.dfs.core.windows.net", f"{service_principal_id}")
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storagename}.dfs.core.windows.net", f"{service_principal_secret}")
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storagename}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")



def get_config_authenticate_azure_adlsgen2_SP (storagename, service_principal_id, service_principal_secret, tenantid):
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": f"{service_principal_id}",
            "fs.azure.account.oauth2.client.secret": f"{service_principal_secret}",
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantid}/oauth2/token"}

    
    return configs
    # Optionally, you can add <directory-name> to the source URI of your mount point.




storagename='irissapoc'
service_principal_id='51b7b1bd-472b-4602-b6f7-452cffa4628e'
service_principal_secret='aIj8Q~fROWsSm.~5ZEbT1VHHu.qdvtvosX-kWbE3'
tenantid='73bffe2b-9041-4754-aaf0-3ef61cde7559'
container='data-ingestion-poc'


authenticate_azure_adlsgen2(storagename, service_principal_id, service_principal_secret, tenantid)




dbutils.fs.mount(
source = f"abfss://{container}@{storagename}.dfs.core.windows.net/",
mount_point = f"/mnt/azure__{container}_{storagename}",
extra_configs = get_config_authenticate_azure_adlsgen2_SP(storagename, service_principal_id, service_principal_secret, tenantid))
