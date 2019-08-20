
import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#s.settimeout(10)
succ=s.connect_ex(('127.0.0.1', 9092))
print(succ)
print("\n")
b = b'\x00\x12\x00\x02\x00\x00\x00\x00\x00\x01\x61'
print("send " + str(b, 'utf-8'))
len = s.send(b)
print(len)
print("...")
response = s.recv(128)
print("---")
print(response)
print("---") 
