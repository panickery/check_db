import jaydebeapi
# import logging
import sys
import os
import pandas as pd
import numpy as np
# from datetime import datetime, timedelta
from pyreadline import Readline
import configparser
import platform

# 서비스로 DB 구조 확인 하는 스크립트
# 서비스명을 입력하면 쿼리를 날라고 구조를 명령어로 사용할수 있도록?
# ex) show, 일단 판다스 구현 

# conf_dict에 들어갈 내용중 jar 파일 패스를 모두 잡아줘야 하는 부분이 있어서 추가
class Conn_Info :
    config_name, jdbc_driver_library, jdbc_driver_class, jdbc_connection_string, default_port = 'default.ini', '', '', '', ''

    def __init__(self, sys_info) :
        try :
            config = configparser.ConfigParser()
            config.read(os.path.join(sys_info.folder_path, self.config_name))
            self.jdbc_driver_library    = config[sys_info.db_info.db_type]['jdbc_driver_library']
            self.jdbc_driver_class      = config[sys_info.db_info.db_type]['jdbc_driver_class']
            self.jdbc_connection_string = config[sys_info.db_info.db_type]['jdbc_connection_string'].replace('{host_ip}', sys_info.db_info.host_ip)
            self.jdbc_connection_string = self.jdbc_connection_string.replace('{host_ip}', sys_info.db_info.host_ip)
            self.jdbc_connection_string = self.jdbc_connection_string.replace('{port}', str(sys_info.db_info.port))
            self.jdbc_connection_string = self.jdbc_connection_string.replace('{database}', sys_info.db_info.database)
            self.default_table_query = None
            
            if 'default_table_query' in config[sys_info.db_info.db_type] :
                print('default_table_query exist!')
                self.default_table_query = config[sys_info.db_info.db_type]['default_table_query'].replace('{user_id}', sys_info.db_info.user_id)
            else :
                print("default_table_query doesn't exist!")

            if self.jdbc_driver_class.upper().startswith('JAVA') : 
                self.jdbc_driver_class = self.jdbc_driver_class[6:]

        except Exception as e :
            print("Failed While Forming Conn Info!! :: {}".format(e))
            sys.exit()

class Db_Info : 
    host_ip, port, db_type, user_id, user_pw, database = '', '', '', '', '', ''

class Sys_Info : 
    detected_os, folder_path, config_path, config_name, config_section = '', '', '', 'db_config.ini', ''
    db_info = Db_Info()

    def get(self, section) : 
        try :
            config = configparser.ConfigParser()
            config.read(os.path.join(self.folder_path, self.config_name))

            self.db_info.host_ip = config[section]['host_ip']
            self.db_info.port = int(config[section]['port'])
            self.db_info.db_type = config[section]['db_type'].lower()
            self.db_info.user_id = config[section]['user_id']
            self.db_info.user_pw = config[section]['user_pw']
            self.db_info.database = config[section]['database']

            return True

        except Exception as e :
            print("FAIL WHILE GET_DB_INFO!! REASON :: {}".format(e))
            return False
    
def execute_query(conn, query) : 
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    
    columns = [desc[0].lower() for desc in cur.description]
    print('가져온 컬럼 : {}'.format(columns))
    df = pd.DataFrame(rows, columns=columns)

    print('결과 길이 :: {}'.format(len(rows)))
    print(df)
    cur.close()
#    conn.close()

    return df

def check_ini(ini_section) : 
    sys.exit()

# 시스템의 정보를 확인합니다.
def system_check() : 
    try : 
        sys_info = Sys_Info()

        # 리눅스/윈도우 여부
        sys_info.detected_os = platform.platform()

        # ini 파일 존재 여부
        sys_info.folder_path = os.path.dirname(os.path.realpath(__file__))
        config = configparser.ConfigParser()
        sys_info.config_path = config.read(os.path.join(sys_info.folder_path, sys_info.config_name))
        sys_info.config_section = config.sections()

        print('Detected OS :: {}'.format(sys_info.detected_os))
        print('Detected ini :: {}'.format(sys_info.config_path))
        print('Detected ini Sections :: {}'.format(sys_info.config_section))

        return sys_info

    except Exception as e :
        print("FAIL WHILE SYSTEM_CHECK!! REASON :: {}".format(e))
        sys.exit(1)

def connect_db(sys_info) : 
    print('START CONNECT_DB!!')

    conn_info = Conn_Info(sys_info)

    try :
        print('{} :: {}'.format(sys_info.db_info.user_id, sys_info.db_info.user_pw))

        lib_path = os.path.join(sys_info.folder_path, 'jars', conn_info.jdbc_driver_library)
        print('lib_path :: {}'.format(lib_path))

        conn = jaydebeapi.connect(
            conn_info.jdbc_driver_class,
            conn_info.jdbc_connection_string,
            [sys_info.db_info.user_id, sys_info.db_info.user_pw],
            lib_path)

        if conn_info.default_table_query :
            execute_query(conn, conn_info.default_table_query)
            print('TABLE PEEP QUERY :: {}'.format(conn_info.default_table_query))

        return conn
    
    except Exception as e:
        print('FAIL While Connect DB :: {}'.format(e))
        return None

if __name__ == '__main__' : 
    # description = '''
    #     지정된 서비스의 count 쿼리를 날린다.
    #     '''
    # parser = argparse.ArgumentParser(description=description)
    # parser.add_argument('--services', help='서비스를 service_nm_eng 값으로 입력한다.')
    # args = parser.parse_args()
    sys_info = system_check()

    if len(sys_info.config_section) == 0 :
        print('No INI section!!')
        exit(0)
    
    announce  = "line interpreter :: (quit for 'q', help for '\h' or 'help')"

    while True :
        print("Type INI section :: ")
        ini_section = input()
        
        if ini_section in sys_info.config_section : 
            if sys_info.get(ini_section) : 
                # 제대로 INI 파일 내용 가져옴
                break
            else :
                print('Bad INI section')
                # 제대로 INI 파일 내용 못가져옴
        else : 
            print('Try Again!!')
    
    conn = connect_db(sys_info)
    
    if conn == None : 
        print('연결이 잘못 되었습니다.')
        sys.exit()
    else :
        print('연결이 성공했습니다.')

    while True :
        print(announce)
        inp = input()
    
        if inp == 'q' :
            print("line interpreter end!")
            print('BYE!')
            break
        
        if inp == 'query' : 
            print("give me query! - only select :: (quit for 'q')")
            query = input()
            if query == 'q' : 
                continue
            
            else :
                print('''you enter query like this :: {} '''.format(query))
                df = execute_query(conn, query)
                print('date success')
                if not df.empty :
                    continue  
                else :
                    print('Wrong query or Not Select query!')
                    continue
            
        elif inp == 'help' or inp == '\h' :
            print('---------help---------')
            print('----- q for quit -----')
            print('- query for query ----')
            print('-- show for show -----')
            print('-- help for help -----')
            print('----------------------')
            continue

        # elif inp == 'show' :
        #     print('I will show you the tables!')
        #     table_query = 
        #     df = execute_query(conn, query)

    print('END')
