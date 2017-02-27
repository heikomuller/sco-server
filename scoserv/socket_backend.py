"""Standard Cortical Observer - Workflow Engine API. Default backend server.
Listens to given port for model run requests.
"""
import getopt
import json
from multiprocessing import Process
import os
import socket
import sys

from workflow import sco_run
from socket_client import RESPONSE_MESSAGE, RESPONSE_STATUS


# ------------------------------------------------------------------------------
#
# Constants
#
# ------------------------------------------------------------------------------

# Local folder for data files
DB_DIR = os.path.abspath('../resources/data')

# Local folder for SCO subject files
ENV_DIR = os.path.abspath('../resources/env/subjects')

# SCO Engine server
ENGINE_HOST = 'localhost'
ENGINE_PORT = 5001


# ------------------------------------------------------------------------------
#
# Helper methods
#
# ------------------------------------------------------------------------------

def run_sco(conn, db_dir):
    """Execute the default SCO predictive model for a given experiment.

    Parameters
    ----------
    conn : socket.Connection
        Connection to SCO Engine client
    """
    data = conn.recv(1024)
    if not data:
        conn.sendall(json.dumps({RESPONSE_STATUS : 400, RESPONSE_MESSAGE : 'no data send'}))
        conn.close()
        return
    json_obj = json.loads(data)
    # Signal to client that request was received successfully
    conn.sendall(json.dumps({RESPONSE_STATUS : 200}))
    conn.close()
    # Run predictive model for run with given identifier
    sco_run(json_obj)

# ------------------------------------------------------------------------------
#
# Main
#
# ------------------------------------------------------------------------------

if __name__ == '__main__':
    # Get command line options for SCO subjects, and engine server host and port
    options, args = getopt.getopt(sys.argv[1:], 'd:e:h:p:', ['dbdir=""''env=','host=', 'port='])
    for opt, arg in options:
        if opt in ('-d', '--dbdir'):
            DB_DIR = arg
        elif opt in ('-e', '--env'):
            ENV_DIR = arg
        elif opt in ('-h', '--host'):
            ENGINE_HOST = arg
        elif opt in ('-p', '--port'):
            ENGINE_PORT = int(arg)
            add_subject_path(ENV_DIR)
    # Add subject path
    # Open socket and listen
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error, msg:
        print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
        sys.exit();
    try:
        s.bind((ENGINE_HOST , ENGINE_PORT))
    except socket.error , msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()
    s.listen(10)
    print 'SCO Engine Backend listening on ' + ENGINE_HOST + ':' + str(ENGINE_PORT)
    while 1:
        conn, addr = s.accept()
        Process(target=run_sco, args=(conn, DB_DIR)).start()
