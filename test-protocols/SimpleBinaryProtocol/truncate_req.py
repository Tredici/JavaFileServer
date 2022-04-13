#!/usr/bin/python3

import socket
import time
from utils import *


def serialize_truncate_message(path):
    version = 1
    cmd = 4
    category = 0

    return version.to_bytes(4,byteorder='big')+\
        cmd.to_bytes(2,byteorder='big')+\
        category.to_bytes(2,byteorder='big')+\
        serialize_string(path)


def recv_ans(sck):
    print("Receving response from socket")
    # message version
    v = sck.recv(4, socket.MSG_WAITALL)
    if len(v) != 4:
        raise Exception("Bad read "+ str(len(v)))
    version = int.from_bytes(v, byteorder='big')
    if version != 1:
        raise Exception("Bad version, found:", version)
    # message type
    t = sck.recv(2, socket.MSG_WAITALL)
    if len(t) != 2:
        raise Exception("Bad read")
    typem = int.from_bytes(t, byteorder='big')
    if typem != 4:
        raise Exception("Bad message type, found:", typem)
    # message category
    cat = sck.recv(2, socket.MSG_WAITALL)
    if len(cat) != 2:
        raise Exception("Bad read")
    category = int.from_bytes(cat, byteorder='big')
    if category != 1:
        raise Exception("Bad category, found:", category)
    # message status
    s = sck.recv(1, socket.MSG_WAITALL)
    if len(s) != 1:
        raise Exception("Bad read")
    success = int.from_bytes(s, byteorder='big')
    # check padding
    p = sck.recv(3, socket.MSG_WAITALL)
    if len(p) != 3:
        raise Exception("Bad read")
    padding = int.from_bytes(p, byteorder='big')
    if padding != 0:
        raise Exception("Bad padding, found:", padding)
    print("File truncated: ", success == 1)

port = 5050
path = "file/base"

if __name__ == "__main__":
    print("Test WRITE request")
    s = "Ciao panino al caffééé"
    b = serialize_string(s)
    print(s, '=>', b)
    print('decoded: ', deserialize_string(b))
    cmd = serialize_truncate_message(path)
    print("Write MSG =>", cmd)
    sck = send_cmd(port, cmd)
    sck.settimeout(1)
    #time.sleep(1)
    recv_ans(sck)