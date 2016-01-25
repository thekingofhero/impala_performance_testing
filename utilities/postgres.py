import psycopg2

class mydb:
    def __init__(self,conn_dic={'db':'postgres','port':5432,'host':'localhost','user':'postgres','pwd':None}):
        self.conn = psycopg2.connect(database = conn_dic['db'],\
                                    port = conn_dic['port'],\
                                    host = conn_dic['host'],\
                                    user = conn_dic['user'],\
                                    password = conn_dic['pwd'])
        self.cursor = self.conn.cursor()

    def runsql(self,sql):
        self.cursor.execute(sql)

    def get_all(self):
        return self.cursor.fetchall()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()        
