from tableau_tools import tableau_documents
from tableau_tools import tableau_rest_api
from tableau_tools import *

#file details
log_file = u'datasource.log'
datasource_name = u'datasource.tds'
###################

#datasource details
ds_version = u'10.5'
ds_type = u'mysql'
ds_server_name = u''
db_name = u''
table_name = u''
ds_username = u''
ds_password = u''
port_number = u'3306'
####################

#server details#
tableau_server_name = u'http://'
tableau_username = u''
tableau_password = u''
tableau_site_name = u'Default'
tableau_project_name = u'Default'
####################

logger = Logger(log_file)

###Creating file and adding connection XML
tableau_file = tableau_documents.TableauFile(datasource_name, create_new=True, logger_obj=logger, ds_version=ds_version)
tableau_document = tableau_file.tableau_document

dses = tableau_document.datasources
ds = dses[0]
ds.add_new_connection(ds_type=ds_type, server=ds_server_name, db_or_schema_name=db_name)
ds.set_first_table(db_table_name=table_name, table_alias=table_name, connection=ds.connections[0].connection_name)

###Updating username and port number
conn = ds.connections[0]
conn.username = ds_username
conn.port = port_number

###Saving datasource file
tableau_document.save_file(datasource_name)

###Publishing the datasource
o = tableau_rest_api.TableauRestApiConnection(server=tableau_server_name, username=tableau_username, password=tableau_password,
                               site_content_url=tableau_site_name)
o.signin()
proj_obj = o.query_project (tableau_project_name)
o.publish_datasource (ds_filename=datasource_name, ds_name=datasource_name, project_obj=proj_obj, overwrite=True, connection_username = ds_username, connection_password=ds_password)
