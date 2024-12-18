import tableauserverclient as TSC

class tableau_api():
    def __init__(self, server, user, token, pagesize = 1000):
        self.tableau_user = user
        self.tableau_token = token
        self.tableau_server = server
        self.tableau_auth = TSC.PersonalAccessTokenAuth(self.tableau_user, self.tableau_token)
        self.server = TSC.Server(self.tableau_server, use_server_version=True)
        self.pagesize = pagesize
        self.request_options = TSC.RequestOptions(pagesize=self.pagesize)
        self.server.auth.sign_in(self.tableau_auth)
        

    def datasource_list(self, search=None):
        ds_list=[]
        server=self.server
        all_datasources = list(TSC.Pager(server.datasources.get, self.request_options))

        for datasource in all_datasources:
            if search is None or search.lower() in datasource.name.lower():
                print(datasource.name)
                ds_list.append([datasource.name, datasource.id])
        return ds_list

    def datasource_download_no_extract(self, filepath):
            server=self.server
            datasource_id=input('\nEnter datasource id: ')
            server.datasources.download(datasource_id, filepath=filepath, include_extract=False, no_extract=None)

    def datasource_download_id_no_extract(self, ds_id, filepath):
                server=self.server
                datasource_id=ds_id
                return server.datasources.download(datasource_id, filepath=filepath, include_extract=False, no_extract=None)

    def users_list(self, search=None):
        #tableau_auth=self.tableau_auth
        server=self.server
        all_users = list(TSC.Pager(server.users.get, self.request_options))
        for user in all_users:
            if search is None or search.lower() in user.name.lower():
                print(user.name)

    def projects_list(self, search=None):
        projects_list = []
        server = self.server
        all_projects = list(TSC.Pager(server.projects.get, self.request_options))
        for project in all_projects:
            if search is None or search.lower() in project.name.lower():
                #print(project.name)
                projects_list.append([project.name, project.id])
        return projects_list

    def ds_publish(self):
        print('\nPublishing Datasource...\n') 
        server = self.server
        publish_mode = TSC.Server.PublishMode.Overwrite
        project_id ='XXXXXX'
        new_datasource = TSC.DatasourceItem(project_id) #, name='sheet_test')
        new_conn_creds = None
        file_path = '/xxxxx/xxxx.tds'
        return server.datasources.publish(datasource_item=new_datasource, 
                                            file=file_path, 
                                            mode=publish_mode,
                                            connection_credentials=new_conn_creds)#, as_job=True)

    def ds_refresh(self):
        server = self.server
        datasource = 'XXXXX'
        server.datasources.refresh(datasource)

    def wb_publish(self):
        print('\nPublishing Workbook...\n') 
        server = self.server
        new_conn_creds = None
        publish_mode = TSC.Server.PublishMode.Overwrite
        project_id ='xxxxxx'
        wb_item = TSC.WorkbookItem(project_id=project_id, name='xxxxx')
        file_path = '/xxxxx/xxxx.tds'
        return server.workbooks.publish(wb_item, file_path, mode=publish_mode,skip_connection_check=True)