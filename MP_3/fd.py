#!/usr/bin/env python3

# MP_2
# Implement SWIM with time-bound completeness and suspicion (optional) that uses broadcasting for dissemination.
# Hard coded vm01 to be the introducer.

import threading
import time
import random
import socket
import argparse
import sys
import json

PING_INTERVAL = 1     # Time per cycle (seconds)
PING_TIMEOUT = 2         # Timeout for ping (seconds)
INTRODUCER_PORT = 5000     # Default port for the introducer
SUS_TIMEOUT = 1         # Suspicion timeout (seconds)

class Node:
    def __init__(self, node_id, host, introducer_addr, port, is_introducer=False):
        self.node_id = node_id
        self.host = host
        self.introducer_addr = introducer_addr  # (host, port)
        self.port = port
        self.is_introducer = is_introducer
        self.membership_list = {}  # node_id: {'host', 'port', 'timestamp', 'sus', 'incarnation'}
        self.lock = threading.RLock()
        self.alive = True
        self.sock = None
        self.ping_targets = []
        self.ping_index = -1
        self.received_acks = {}
        self.hdfs = None
        self.enable_sus = True
        self.drop_ack = False
        self.add_member_callback = None
        # Used for benchmark in report, disregard
        self.bytes_sent = 0
        self.failure_callback = None
        self.bytes_received = 0
        self.bandwidth_lock = threading.Lock()
        self.p = 1
        self.benchmark_detection_time = False
        self.benchmark_detection_time_list = []

    def start(self):
        """Start the node and request introducer to join network"""
        # Start server
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', self.port))
        print(f"[{self.node_id}] Listening on port {self.port}")
        
        threading.Thread(target=self.receive_messages, daemon=True).start() 
        threading.Thread(target=self.ping_cycle, daemon=True).start()
        # threading.Thread(target=self.input_loop, daemon=True).start()

        if self.is_introducer: # Introducer is expected to be the first to join a network
            print(f"[{self.node_id}] Acting as introducer")
            self.add_member(self.node_id, self.host, self.port, timestamp=time.time())
        else:
            self.join_system()

    def join_system(self):
        """Helper function to send join request to introducer"""
        introducer_host, introducer_port = self.introducer_addr
        message = {
            'type': 'JOIN',
            'node_id': self.node_id,
            'host': self.host,
            'port': self.port,
            'timestamp': time.time(),  # Include the node's own timestamp
            'sus': False,
            'incarnation': 0
        }
        self.send_message(introducer_host, introducer_port, message)
        print(f"[{self.node_id}] Sent JOIN request to introducer {introducer_host}:{introducer_port}")

    def send_message(self, host, port, message):
        """Message marshalling and send message to host"""
        data = json.dumps(message).encode('utf-8')
        self.sock.sendto(data, (host, port))
        # One way to measure bandwidth (in report, this is done using external tools)
        with self.bandwidth_lock:
            self.bytes_sent += len(data)

    def receive_messages(self):
        """Recieve message, decode and refer to message handler"""
        while self.alive:
            try:
                data, addr = self.sock.recvfrom(4096)
                ##update bandwidth
                with self.bandwidth_lock:
                    self.bytes_received += len(data)
                message = json.loads(data.decode('utf-8'))
                # Handle the received message
                self.handle_message(message, addr)
            except Exception as e:
                if self.alive:
                    print(f"[{self.node_id}] Error receiving message: {e}")
                
    def report_bandwidth(self):
        """A way to report bandwidth, disregard"""
        with self.bandwidth_lock:
            sent = self.bytes_sent
            received = self.bytes_received
            self.bytes_sent = 0
            self.bytes_received = 0
        print(f"[{self.node_id}] Bandwidth Usage - Sent: {sent} bytes, Received: {received} bytes")


    def handle_message(self, message, addr):
        """Message handler that reacts to incoming messages based on their type"""
        msg_type = message.get('type')
        if msg_type == 'JOIN':
            self.handle_join(message)
        elif msg_type == 'JOIN_MEMBERSHIPLIST':
            self.new_node_update_membership_list(message)
        elif msg_type == 'PING':
            sus = message.get('sus')
            incarnation = message.get('incarnation')
            self.send_ack(sus, incarnation, addr, self.p)
        elif msg_type == 'ACK':
            source_id = message.get('source_id')
            incarnation = message.get('incarnation')
            self.receive_ack(source_id, incarnation)
        elif msg_type == 'LIST_MEM':
            self.list_membership()
        elif msg_type == 'LIST_SELF':
            self.list_self()
        elif msg_type == 'LEAVE':
            leaving_node_id = message.get('node_id')
            with self.lock: 
                if leaving_node_id in self.membership_list:
                    print(f"[{self.node_id}] Node {leaving_node_id} is leaving")
            self.mark_node_as_failed(leaving_node_id)
        elif msg_type == 'FAIL':
            failed_node_id = message.get('node_id')
            if failed_node_id == self.node_id:
                self.stop()
            else:
                with self.lock:
                    if failed_node_id in self.membership_list:
                        print(f"[{self.node_id}] Node {failed_node_id} has failed")
                self.mark_node_as_failed(failed_node_id)
        elif msg_type == 'SUS':
            node_id = message.get('node_id')
            incarnation = message.get('incarnation')
            self.recieve_sus(node_id, incarnation)
        elif msg_type == 'ALIVE':
            node_id = message.get('node_id')
            incarnation = message.get('incarnation')
            self.recieve_alive(node_id, incarnation)
        elif msg_type == 'ENABLE_SUS':
            with self.lock:
                self.enable_sus = True
                PING_TIMEOUT = 1
        elif msg_type == 'DISABLE_SUS':
            with self.lock:
                self.enable_sus = False
                PING_TIMEOUT = 2
        elif msg_type == 'DETECTION':
            end_time = message.get('time')
            with self.lock:
                self.benchmark_detection_time_list.append(end_time)
        elif msg_type == 'ENABLE_DETECTION':
            with self.lock:
                self.benchmark_detection_time = True
        elif msg_type == 'DISABLE_DETECTION':
            with self.lock:
                self.benchmark_detection_time = False
        elif msg_type == 'DROPRATE':
            p = message.get('p')
            with self.lock:
                self.p = p
        else:
            print(f"[{self.node_id}] Unknown message type: {msg_type}")

    def handle_join(self, message):
        """Introducer code to handle a new node join request. Update membership list for self and new node. New node is broadcasted to existing nodes"""
        node_id = message.get('node_id')
        host = message.get('host')
        port = message.get('port')
        timestamp = message.get('timestamp') 
        sus = message.get('sus', False)
        incarnation = message.get('incarnation', 0)

        self.add_member(node_id, host, port, timestamp, sus, incarnation)
        if self.is_introducer:
            self.share_new_member(message)
            # Send existing membership list to the new node
            with self.lock:
                for member_id, info in self.membership_list.items():
                    existing_member_message = {
                        'type': 'JOIN_MEMBERSHIPLIST',
                        'node_id': member_id,
                        'host': info['host'],
                        'port': info['port'],
                        'timestamp': info['timestamp'], 
                        'sus': info['sus'],
                        'incarnation': info['incarnation']
                    }
                    self.send_message(host, port, existing_member_message)
                    
    def new_node_update_membership_list(self, message):
        """Introducer code to handle a new node join request. Update membership list for self and new node. New node is broadcasted to existing nodes"""
        node_id = message.get('node_id')
        host = message.get('host')
        port = message.get('port')
        timestamp = message.get('timestamp') 
        sus = message.get('sus', False)
        incarnation = message.get('incarnation', 0)

        self.add_member(node_id, host, port, timestamp, sus, incarnation, callback=False)

    def add_member(self, node_id, host, port, timestamp=None, sus=False, incarnation=0, callback=True):
        """Add given node to self's membership list. Append to the end of the current ping iteration"""
        if timestamp is None:
            timestamp = time.time()
        with self.lock:
            self.membership_list[node_id] = {
                'host': host,
                'port': port,
                'timestamp': timestamp,
                'sus': sus,
                'incarnation': incarnation
            }
            # Append to ping_targets if not already present and not self
            if node_id != self.node_id and node_id not in self.ping_targets:
                self.ping_targets.append(node_id)
        if callback and self.add_member_callback and node_id != self.node_id: # callback to remap file
            self.add_member_callback(node_id)

    def share_new_member(self, message):
        """Introducer code to share information of new node joining to its members"""
        node_id = message.get('node_id')
        with self.lock:
            for member_id, info in self.membership_list.items():
                if member_id != self.node_id and member_id != node_id:
                    self.send_message(info['host'], info['port'], message)

    def broadcast(self, message):
        """Send to every node in the membership list"""
        with self.lock:
            for member_id, info in self.membership_list.items():
                if member_id != self.node_id:
                    self.send_message(info['host'], info['port'], message)

    def send_ack(self, sus, incarnation, addr, p):
        """Acknowledge a ping. Increment incarnation if being suspected"""
        with self.lock:
            if self.enable_sus and sus:
                # Suspect_i overrides Alive_j if i>=j
                if self.membership_list[self.node_id]['incarnation'] <= incarnation: 
                    self.membership_list[self.node_id]['incarnation'] = incarnation + 1
            
        message = {
            'type': 'ACK',
            'source_id': self.node_id,
            'incarnation': self.membership_list[self.node_id]['incarnation']
        }
        # Send the message with probability p
        if random.random() < p:
            self.send_message(addr[0], addr[1], message)

    def receive_ack(self, source_id, incarnation):
        """Recieve an ack after ping. In suspicion mode, also repsonsible for sharing the alive status of ack node"""
        # Store ACK receipt
        with self.lock:
            self.received_acks[source_id] = True
            
            if source_id in self.membership_list:
                # Alive_i overrides Suspect_j if i > j
                if self.enable_sus and self.membership_list[source_id]['sus'] and self.membership_list[source_id]['incarnation'] < incarnation:
                    self.membership_list[source_id]['sus'] = False
                    self.membership_list[source_id]['incarnation'] = incarnation
                    self.share_alive(source_id, incarnation)

    def ping_cycle(self):
        """Logic in each ping cycle. Timeouts are concurrent to fullfill time-bound completeness"""
        while self.alive:
            start_time = time.time()

            # Reshuffle the membership list and reset ping index if needed
            with self.lock:
                if self.ping_index >= len(self.ping_targets) - 1:
                    self.ping_targets = list(self.membership_list.keys())
                    random.shuffle(self.ping_targets)
                    if self.node_id in self.ping_targets:
                        self.ping_targets.remove(self.node_id)
                    self.ping_index = -1

            # Select next node to ping
            target_id = self.select_next_node()

            # Pinging a node and waiting concurrently for timeout/suspect
            threading.Thread(target=self.ping_logic, args=(target_id,), daemon=True).start()

            # Sleep until next ping cycle
            elapsed = time.time() - start_time
            time.sleep(max(0, PING_INTERVAL - elapsed))

    def ping_logic(self, target_id):
        """Logic for pinging each node. If direct_ping failed, handle based on whether suspicion is enabled"""
        if target_id:
            ack_received = self.direct_ping(target_id)
            if not ack_received:
                self.lock.acquire()
                if self.enable_sus: 
                    self.lock.release()
                    self.suspecting_node(target_id)
                    self.share_sus(target_id)
                    self.wait_for_sus(target_id) # Since ping_logic is already running concurrently, no need to run a new thread for sus timeout
                else: 
                    self.lock.release()
                    self.handle_failed(target_id)
        else:
            print(f"[{self.node_id}] No nodes to ping")
            
    def suspecting_node(self, target_id):
        """Mark suspected node and print log message to stdout"""
        with self.lock:
            if target_id in self.membership_list:
                self.membership_list[target_id]['sus'] = True
                print(f"[{self.node_id}] Suspecting node: [{target_id}]")

            
    def handle_failed(self, target_id):
        """Broadcast failure to members and call mark_node_as_failed to clean up""" 
        fail_message = {
            'type': 'FAIL',
            'node_id': target_id
        }
        # Broadcast includes the failed node so if false positive that node can self destruct gracefully
        self.broadcast(fail_message)
        self.mark_node_as_failed(target_id)
    
    def wait_for_sus(self, target_id):
        """Wait for suspicion timeout. If timeout, handle failure just like disable_sus"""
        start_time = time.time()
        while time.time() - start_time < SUS_TIMEOUT:
            with self.lock:
                if target_id not in self.membership_list:
                    return
                if not self.membership_list[target_id]['sus']:
                    return
            time.sleep(0.1)
        with self.lock:
            if target_id in self.membership_list:
                self.handle_failed(target_id)

        
    def share_sus(self, target_id):
        """Share suspicion of node with other members"""
        with self.lock:
            if target_id in self.membership_list:
                incarnation = self.membership_list[target_id]['incarnation']
                sus_message = {
                    'type': 'SUS',
                    'node_id': target_id,
                    'incarnation': incarnation
                }
                self.broadcast(sus_message)
    
    def recieve_sus(self, node_id, incarnation):
        """When a node_i recieve suspicion of another node_j, it should update it's own suspicion information (based on SWIM paper override rules) and wait concurrently for sus timeout"""
        with self.lock: 
            if node_id in self.membership_list:     
                if node_id != self.node_id:
                    if self.membership_list[node_id]['sus'] == True: # Suspect_i overrides Suspect_j if i > j
                        if self.membership_list[node_id]['incarnation'] < incarnation: 
                            self.membership_list[node_id]['incarnation'] = incarnation
                    else: # Suspect_j overrides Alive_j if i >= j
                        if self.membership_list[node_id]['incarnation'] <= incarnation:
                            self.membership_list[node_id]['incarnation'] = incarnation
                            self.suspecting_node(node_id)
                            threading.Thread(target=self.wait_for_sus, args=(node_id,), daemon=True).start()
                            
                else: # self is being suspected, update incarnation and tell other nodes that self is alive
                    if self.membership_list[node_id]['incarnation'] <= incarnation:
                        self.membership_list[node_id]['incarnation'] = incarnation + 1
                        self.share_alive(self.node_id, self.membership_list[node_id]['incarnation'])
    
    def recieve_alive(self, node_id, incarnation):
        """Update suspicion of node using override rules"""
        with self.lock:
            if node_id in self.membership_list and node_id != self.node_id: # Alive_i overrides ANYTHING_j if i > j
                if self.membership_list[node_id]['incarnation'] < incarnation: 
                    self.membership_list[node_id]['sus'] = False
                    self.membership_list[node_id]['incarnation'] = incarnation
                    
    def share_alive(self, node_id, incarnation):
        """Broadcast to other nodes that node_id is alive. Either done by self or the pinging node"""
        alive_message = {
            'type': 'ALIVE',
            'node_id': node_id,
            'incarnation': incarnation
        }
        self.broadcast(alive_message)
                    
    def select_next_node(self):
        """Select next node in the ping target list"""
        with self.lock:
            self.ping_index += 1
            if self.ping_index < len(self.ping_targets):
                return self.ping_targets[self.ping_index]
            else:
                return None

    def direct_ping(self, target_id):
        """Send ping request to target and wait for ack. In suspicion mode, also share target's suspicion status"""
        with self.lock:
            target_info = self.membership_list.get(target_id)
            if not target_info:
                return False
        host, port, sus, incarnation = target_info['host'], target_info['port'], target_info['sus'], target_info['incarnation']
        message = {
            'type': 'PING',
            'source_id': self.node_id,
            'sus': sus,
            'incarnation': incarnation
        }
        self.send_message(host, port, message)
        return self.wait_for_ack(target_id)

    def wait_for_ack(self, target_id):
        """Timeout if ack is not recieved in PING_TIMEOUT"""
        self.received_acks = {}
        start_time = time.time()
        while time.time() - start_time < PING_TIMEOUT:
            with self.lock:
                if self.received_acks.get(target_id):
                    return True
            time.sleep(0.1)
        return False

    def mark_node_as_failed(self, target_id):
        """Remove failed nodes from membership list and ping target list."""
        with self.lock:
            if target_id in self.membership_list:
                print(f"[{self.node_id}] Removing {target_id} from membership list")
                del self.membership_list[target_id]
                if self.failure_callback:
                    self.failure_callback(target_id)  # Notify hdfs of the failure
                # Used for report, ignore
                if self.benchmark_detection_time:
                    self.benchmark_send_detection_time()
            # Remove from ping targets
            if target_id in self.ping_targets:
                self.ping_targets.remove(target_id)
                
    def benchmark_send_detection_time(self):
        """Benchmark code that sends detection end time to introducer for processing"""
        end_time = time.time()
        benchmark_message = {
            'type': 'DETECTION',
            'time': end_time
        }
        self.send_message(self.introducer_addr[0], self.introducer_addr[1], benchmark_message)
        
    def calculate_detection_time(self):
        """Take timestamp from crash nodes as start time and remove from membership timestamps from regular nodes as end time. Calculate elaspe"""
        with self.lock:
            end_time = max(self.benchmark_detection_time_list)
            start_time = min(self.benchmark_detection_time_list)
        detection_time = end_time - start_time
        print(f"[{self.node_id}] Final Detection Time: {detection_time}s")

    def list_membership(self):
        """Pretty print membership list"""
        with self.lock:
            print(f"[{self.node_id}] Membership List:")
            for member_id, info in self.membership_list.items():
                print(f"  Node ID: {member_id}, Host: {info['host']}, Port: {info['port']}, Timestamp: {info['timestamp']}, Sus: {info['sus']}, Incarnation: {info['incarnation']}")

    def list_self(self):
        """Demo instruction to list node_id"""
        print(f"[{self.node_id}] Self ID: {self.node_id}")
        
    def crash_nodes(self, crash_list):
        """Benchmark code that crash mutiple nodes in the crash_list at once (or as fast as messages are send and recieved)"""
        crash_self = False
        for node_id in crash_list:
            if node_id == self.node_id:
                crash_self = True
            else: 
                with self.lock:
                    if node_id in self.membership_list:
                        crash_command = {
                            'type': 'FAIL',
                            'node_id': node_id
                        }
                        info = self.membership_list[node_id]
                        self.send_message(info['host'], info['port'], crash_command)
        if crash_self:
            self.stop()
            
    def set_add_member_callback(self, callback):
        """Set a callback function to handle new node additions."""
        self.add_member_callback = callback

    def input_loop(self):
        """Handle commands that user can input at program runtime"""
        while self.alive:
            try:
                user_input = input()
                if user_input == 'list_mem':
                    self.list_membership()
                    self.broadcast({'type': 'LIST_MEM'})
                elif user_input == 'list_self':
                    self.list_self()
                    self.broadcast({'type': 'LIST_SELF'})
                elif user_input == 'leave':
                    print(f"[{self.node_id}] Leaving the network")
                    leave_message = {
                        'type': 'LEAVE',
                        'node_id': self.node_id
                    }
                    self.broadcast(leave_message)
                    self.stop()
                    break  # Exit the input loop
                elif user_input == 'enable_sus':
                    with self.lock:
                        self.enable_sus = True
                    message = {'type': 'ENABLE_SUS'}
                    self.broadcast(message)
                elif user_input == 'disable_sus':
                    with self.lock:
                        self.enable_sus = False
                    message = {'type': 'DISABLE_SUS'}
                    self.broadcast(message)
                elif user_input == 'drop_rate':
                    drop_rate_input = input()
                    with self.lock:
                        self.p = float(drop_rate_input)
                    message = {'type': 'DROPRATE', 'p': self.p}
                    self.broadcast(message)
                elif user_input == 'report_bandwidth':
                    self.report_bandwidth()
                elif user_input == 'crash':
                    crash_list = input().split() # [01, 02, 03, ...]
                    self.crash_nodes(crash_list)
                elif user_input == 'enable_detection_time':
                    with self.lock:
                        self.benchmark_detection_time = True
                    message = {'type': "ENABLE_DETECTION"}
                    self.broadcast(message)
                elif user_input == 'disable_detection_time':
                    with self.lock:
                        self.benchmark_detection_time = False
                    message = {'type': "DISABLE_DETECTION"}
                    self.broadcast(message)
                elif user_input == 'clear_detection_time':
                    with self.lock:
                        self.benchmark_detection_time_list = []
                elif user_input == 'list_detection_time':
                    if self.is_introducer:
                        self.calculate_detection_time()
                    else:
                        print(f"[{self.node_id}] is not an introducer")
                else:
                    print(f"[{self.node_id}] Unknown command: {user_input}")
            except EOFError:
                break
            except Exception as e:
                print(f"[{self.node_id}] Input error: {e}")

    def stop(self):
        """Stop a node from doing anything, either because of fail or leave"""
        with self.lock:
            if self.benchmark_detection_time:
                self.benchmark_send_detection_time()
        self.alive = False
        self.sock.close()
        print(f"[{self.node_id}] Node has stopped")
        
    def set_failure_callback(self, callback):
        """Set a callback function to handle node failures"""
        self.failure_callback = callback

def parse_arguments():
    """Parse input arguments. See readme for clear instructions on how to run"""
    parser = argparse.ArgumentParser(description='Distributed Failure Detection Node')
    parser.add_argument('--node-id', required=True, help='Unique identifier for the node')
    parser.add_argument('--port', type=int, default=5000, help='Port to listen on')
    parser.add_argument('--is-introducer', action='store_true', help='Flag to indicate if this node is the introducer')
    return parser.parse_args()

def main():
    args = parse_arguments()
    introducer_host = "fa24-cs425-7001.cs.illinois.edu"
    introducer_port = INTRODUCER_PORT

    node = Node(
        node_id=args.node_id,
        host=socket.gethostname(),
        introducer_addr=(introducer_host, introducer_port),
        port=args.port,
        is_introducer=args.is_introducer
    )
    node.start()

    # Keep the main thread alive while daemon threads are running
    try:
        while node.alive:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()


if __name__ == '__main__':
    main()


