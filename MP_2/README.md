PingAck 

# How to run
#### Introducer
python3 start_server.py --node-id=01 --is-introducer 
#### Other machines
python3 start_server.py --node-id=XX

# TODO: measure the bandwidth (total bytes / total time)
Add start_time, end_time, and num_bytes variable. In send_message and recieve_messge functions, num_bytes += len(message)

# suspicion_part 
enable/disable suspicion (enable_sus / disable_sus)

suspicion on/off status (status_sus)



# SUS logic:
##### Each node in membership list: 
node_id: {host, port timestamp, sus, incarnation}
##### sus: true/false, incarnation: 0
##### When node_i suspect node_j: 
broadcast sus_node_j, update own membership, start sus_timeout timer
##### When node_l recieves sus_node_j: 
if incarnation override, update own membership, start sus_timeout timer(do nothing if timer already started)
##### When node_j recieves sus_node_j:
 if incarnation override, incarnation ++, share_alive
##### Ping now includes whether the target_node is being suspected. 
##### When node_j recieves ping that contain sus: 
incarnation ++
##### When node_l recieves ack from node_j after sus_node_j: 
if incarnation override: update incarnation from node_j, no longer sus_node_j. share_alive for node_j
##### When other nodes recieves node_j alive: 
if incarnation override: end timer, no longer sus_node_j


# Measure bandwidth
##### In each terminal: 
sudo dnf install iftop
##### Run:
sudo iftop in a seperate terminal of the same vm


sudo iftop -f "port 5000"
