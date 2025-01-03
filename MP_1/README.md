# CS425_MP1

### Server host names: 
fa24-cs425-70XX.cs.illinois.edu


### To test: 

server 1: 
python3 start_server.py [machine number, i.e. 01] [Optional --is-test]

example: 
python3 start_server.py 01 (this will grep from machine.01.log, if --is-test, grep from unittest.01.log)

client: 
python3 client.py [Optional --show-times] -- [Optional flags ...] "search pattern"

example: 
python3 client.py --show-times -- -H "PATTERN"

...

### To kill server: 

ctrl + c in terminal while server is running
##### OR
lsof -i :9999 to find pid after server stops.


### To skip credential when git pull on vm
git config --global credential.helper "cache --timeout=3600"
##### To do so in vscode
git config --global credential.helper store

These will store as plain text


### To move machine.i.log to remote server:
scp machine.xx.log wenjies3@fa24-cs425-70xx.cs.illinois.edu:/home/netid/g70_mp1