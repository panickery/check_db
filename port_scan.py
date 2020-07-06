# # 포트스캔 포트 하나에 1초 이상
# # 쓰레드?

import socket
import sys

def port_scan(host, port) :
    try :
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("Try to Connect %-16s :: %-5d" %(host, port), end='')
        client_socket.connect((host, port))
        print(" - Connect OK")
        client_socket.close();
    except ConnectionRefusedError as cr_e :
        print(" - Connection Refused :: {}".format(port))
        client_socket.close();
    except OSError as os_e :
        print(" - OS Error - err_no :: {}".format(os_e.args[0]))
        client_socket.close();
    except ConnectionAbortedError as ca_e :
        print(" - Connection Aborted :: {}".format(os_e.args[0]))
        client_socket.close();

if __name__ == '__main__' :
    hosts = ["0.0.0.0"]
    ports = [8080]

    for host in hosts :
        for port in ports : 
            port_scan(host, port)
            



