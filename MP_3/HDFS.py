
# HyDFS Implementation

import threading
import time
import socket
import argparse
import sys 
import json
import os
import hashlib
from collections import OrderedDict
from fd import Node
import math
import random
from datetime import datetime
import base64

# Constants
PING_INTERVAL = 1     
PING_TIMEOUT = 2      
INTRODUCER_PORT = 5000  
SUS_TIMEOUT = 1       
NUM_REPLICAS = 3      
CACHE_SIZE = 5000     
RING_SIZE = 256
WRITE_CONSISTENCY_LV = 5
READ_CONSISTENCY_LV = 3
CLEAN_CYCLE = 5
MULTI_APPEND_LOOP = 1000

class Communicator:
    """Class to handle TCP and UDP communication."""

    def __init__(self):
        pass

    def send_tcp_message(self, host, port, message):
        """Send a message over TCP."""
        data = json.dumps(message).encode('utf-8')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(10)
                sock.connect((host, port))
                sock.sendall(data)
                return True
        except socket.timeout:
            print(f"Connection to {host}:{port} timed out.")
            return False
        except Exception as e:
            print(f"Error sending TCP message to {host}:{port} - {e}")
            return False

    def receive_tcp_message(self, conn):
        """Receive a message over TCP."""
        data = b''
        while True:
            packet = conn.recv(4096)
            if not packet:
                break
            data += packet
        return json.loads(data.decode('utf-8'))
    
    def send_and_receive_tcp_message(self, host, port, message, timeout=10):
        # Add a timeout to make sure does not hang
        """Send a message over TCP and receive the response."""
        data = json.dumps(message).encode('utf-8')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect((host, port))
                sock.sendall(data)
                sock.shutdown(socket.SHUT_WR) # Sender and recieve are both using a loop to read data, so need a way lift the block.
                # Now wait for response
                response_data = b''
                while True:
                    packet = sock.recv(4096)
                    if not packet:
                        break
                    response_data += packet
                response = json.loads(response_data.decode('utf-8'))
                return response
        except socket.timeout:
            print(f"Connection to {host}:{port} timed out.")
            return None
        except Exception as e:
            print(f"Error sending TCP message to {host}:{port} - {e}")
            return None


class HDFS:
    def __init__(self, node_id, host, introducer_addr, hdfs_port, fd_port, is_introducer=False):
        self.node_id = node_id
        self.host = host
        self.introducer_addr = introducer_addr
        self.hdfs_port = hdfs_port
        self.is_introducer = is_introducer

        # Local failure detection instance, runs on another port
        self.fd = Node(
            node_id=node_id,
            host=host,
            introducer_addr=introducer_addr,
            port=fd_port,
            is_introducer=is_introducer
        )
        self.fd.hdfs = self 
        self.fd.set_failure_callback(self.handle_node_failure)
        self.fd.set_add_member_callback(self.handle_node_addition)

        self.file_table = {}  # All files stored in memory
        self.file_lock = threading.RLock()
        self.cache = OrderedDict() # LRU write-through 
        self.cache_lock = threading.RLock()
        self.cache_size = CACHE_SIZE # SIZE not in bytes but in number of chunks
        self.client_seq_num = 0  # (node_id, client_seq_num) will be unique to each node
        self.communicator = Communicator()

        self.alive = True
        self.sock = None
        
    def NUM_REPLICA(self):
        with self.fd.lock: 
            result = min(NUM_REPLICAS, len(self.fd.membership_list))
        return result
        
    def WRITE_CONSISTENCY_LEVEL(self): # ALL
        result = self.NUM_REPLICA()
        return result
    
    def READ_CONSISTENCY_LEVEL(self): # ONE
        return 1

    def start(self):
        """Start the hdfs server and failure detection concurrently."""
        # Start failure detection
        threading.Thread(target=self.fd.start, daemon=True).start()

        # Start the hdfs server
        print(f"[{self.node_id}] hdfs server running on port {self.hdfs_port}")
        threading.Thread(target=self.run_server, daemon=True).start()
        threading.Thread(target=self.clean_cycle, daemon=True).start()

        try:
            while self.alive:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop the hdfs node"""
        self.alive = False
        if self.sock:
            self.sock.close()
        self.fd.stop()
        print(f"[{self.node_id}] hdfs node has stopped")

    def run_server(self):
        """Listen to incoming traffic in a loop."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('', self.hdfs_port))
        backlog = socket.SOMAXCONN
        self.sock.listen(min(backlog, 50))
        print(f"[{self.node_id}] hdfs server listening on port {self.hdfs_port}")

        threading.Thread(target=self.user_input_loop, daemon=True).start()

        while self.alive:
            try:
                conn, addr = self.sock.accept()
                threading.Thread(target=self.handle_tcp_connection, args=(conn, addr), daemon=True).start()
            except Exception as e:
                if self.alive:
                    print(f"[{self.node_id}] Error accepting connection: {e}")

    def handle_tcp_connection(self, conn, addr):
        """Handle incoming TCP connections."""
        try:
            message = self.communicator.receive_tcp_message(conn)
            msg_type = message.get('type')
            if msg_type == 'CREATE_FILE':
                self.handle_create_file(conn, message)
            elif msg_type == 'REQUEST_FILE':
                self.handle_request_file(conn, message)
            elif msg_type == 'APPEND_FILE':
                self.handle_append_file(conn, message)
            elif msg_type == 'REQUEST_CHUNKS_METADATA':
                self.handle_request_chunk_metadata(conn, message)
            elif msg_type == 'UPDATE_CHUNKS':
                self.handle_update_chunks(conn, message)
            elif msg_type == 'REQUEST_CHUNK_DATA':
                self.handle_request_chunk_data(conn, message)
            elif msg_type == 'CHECK_FILE':
                self.handle_check_file(conn, message)
            elif msg_type == 'REMOTE_APPEND':
                self.handle_remote_append(conn, message)
            else:
                print(f"[{self.node_id}] Unknown TCP message type: {msg_type}")
        except Exception as e:
            print(f"[{self.node_id}] Error handling TCP connection from {addr}: {e}")
        finally:
            conn.close()  # Ensure the connection is always closed

    def clean_cycle(self):
        """Periodically remove files that are not the node's responsibility"""
        while self.alive:
            start_time = time.time()

            threading.Thread(target=self.re_evaluate_file_responsibilities, daemon=True).start()

            # Sleep until next ping cycle
            elapsed = time.time() - start_time
            time.sleep(max(0, CLEAN_CYCLE - elapsed))

    def consistent_hash(self, key):
        """md5 hash function to map a key to the ring, wrap around at RING_SIZE"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (RING_SIZE)

    def get_node_address(self, node_id):
        """Get the host and port of a node from the membership list."""
        with self.fd.lock:
            node_info = self.fd.membership_list.get(node_id)
            if node_info:
                return node_info['host'], self.hdfs_port
            else:
                return None, None

    def get_responsible_nodes(self, hdfsfilename):
        """Get nodes responsible for a given file using consistent hashing."""
        hash_val = self.consistent_hash(hdfsfilename)
        with self.fd.lock:
            node_hashes = sorted(
                [(self.consistent_hash(node_id), node_id) for node_id in self.fd.membership_list.keys()]
            )
        
        responsible_nodes = []
        
        # Find the first NUM_REPLICA successors
        for node_hash, node_id in node_hashes:
            if node_hash >= hash_val:
                responsible_nodes.append((node_hash, node_id))
            if len(responsible_nodes) >= self.NUM_REPLICA():
                break
        
        # Wrap around the ring if needed
        if len(responsible_nodes) < self.NUM_REPLICA():
            for node_hash, node_id in node_hashes:
                if node_id not in [n_id for _, n_id in responsible_nodes]:  # Avoid duplicates
                    responsible_nodes.append((node_hash, node_id))
                    if len(responsible_nodes) >= self.NUM_REPLICA():
                        break

        return [node_id for node_hash, node_id in responsible_nodes]


    def create(self, localfilename, hdfsfilename):
        """Create a file in the hdfs. Act as coordinator and send file to replicas."""
        with self.file_lock:
            if hdfsfilename in self.file_table:
                print(f"[{self.node_id}] File '{hdfsfilename}' already exists locally")
                return
        # increment seq_num so that chunk is unique
        self.client_seq_num += 1
        if os.path.exists(localfilename):
            responsible_nodes = self.get_responsible_nodes(hdfsfilename)
            with open(localfilename, 'rb') as f:
                file_data = f.read()
            # Counter for consistency levels
            success_count = [0]  # Use list to make it mutable in inner function
            success_lock = threading.Lock()
            success_event = threading.Event()

            # Wrap in thread function to concurrently create at each replica
            def send_to_node(node_id):
                host, _ = self.get_node_address(node_id)
                if host is None:
                    return
                if node_id == self.node_id: # local create
                    with self.file_lock:
                        self.file_table[hdfsfilename] = []
                        metadata = {
                            'client_id': self.node_id,
                            'seq_num': self.client_seq_num,
                            'timestamp': time.time()
                        }
                        self.file_table[hdfsfilename].append((metadata, file_data))
                        print(f"[{self.node_id}] File '{hdfsfilename}' created locally")
                    with success_lock:
                        success_count[0] += 1
                        if success_count[0] == self.WRITE_CONSISTENCY_LEVEL():
                            success_event.set()
                else: # send create to responsible node
                    message = {
                        'type': 'CREATE_FILE',
                        'hdfsfilename': hdfsfilename,
                        'file_data': base64.b64encode(file_data).decode('ascii'),
                        'metadata': {
                            'client_id': self.node_id,
                            'seq_num': self.client_seq_num,
                            'timestamp': time.time()
                        }
                    }
                    sent = self.communicator.send_and_receive_tcp_message(host, self.hdfs_port, message, timeout=500)
                    if sent is not None: # Create success
                        with success_lock:
                            success_count[0] += 1
                            if success_count[0] == self.WRITE_CONSISTENCY_LEVEL():
                                # Coordinator unblocks
                                success_event.set()
            threads = []
            for node_id in responsible_nodes:
                t = threading.Thread(target=send_to_node, args=(node_id,))
                t.start()
                threads.append(t)
            # Wait until write consistency level is reached
            success_event.wait()
            print(f"[{self.node_id}] Create File {hdfsfilename} success.")
        else:
            print(f"File '{localfilename}' not found")

    def handle_create_file(self, conn, message):
        """Create the file at local node."""
        
        hdfsfilename = message['hdfsfilename']
        file_data = base64.b64decode(message['file_data'])
        metadata = message['metadata']
        with self.file_lock:
            if hdfsfilename not in self.file_table:
                self.file_table[hdfsfilename] = []
                self.file_table[hdfsfilename].append((metadata, file_data))
                print(f"[{self.node_id}] File '{hdfsfilename}' created at node '{self.node_id}'")
            else:
                print(f"[{self.node_id}] File '{hdfsfilename}' already exists at node '{self.node_id}'")
        # Send ack back to coordinator
        response = {'type': 'ACK', 'hdfsfilename': hdfsfilename}
        conn.sendall(json.dumps(response).encode('utf-8'))

    def get(self, hdfsfilename, localfilename):
        """Retrieve a file from hdfs, and write it to disk"""
        # First try cache
        with self.cache_lock:
            if hdfsfilename in self.cache:
                print(f"[{self.node_id}] Retrieving '{hdfsfilename}' from cache")
                data = self.cache[hdfsfilename]
                with open(localfilename, 'wb') as f:
                    f.write(data)
                # LRU enviction policy
                self.cache.move_to_end(hdfsfilename)
                return

        responsible_nodes = self.get_responsible_nodes(hdfsfilename)
        
        # Try to get locally
        if self.node_id in responsible_nodes:
            with self.file_lock:
                if hdfsfilename in self.file_table:
                    data = b''.join(chunk_data for _, chunk_data in self.file_table[hdfsfilename])
                    with open(localfilename, 'wb') as f:
                        f.write(data)
                    print(f"[{self.node_id}] File '{hdfsfilename}' retrieved locally")
                    # Reads are written to cache.
                    with self.cache_lock:
                        self.cache[hdfsfilename] = data
                        if len(self.cache) > self.cache_size:
                            self.cache.popitem(last=False) # LRU enviction
                    return

        # Has to get over the network
        success_count = [0]
        success_lock = threading.Lock()
        success_event = threading.Event()
        file_data = None
        file_data_lock = threading.Lock()

        # Wrapped in thread function for concurrent get
        def fetch_from_node(node_id):
            nonlocal file_data
            host, _ = self.get_node_address(node_id)
            if host is None:
                return
            if node_id != self.node_id: # Need to get over the network
                message = {'type': 'REQUEST_FILE', 'hdfsfilename': hdfsfilename}
                response = self.communicator.send_and_receive_tcp_message(host, self.hdfs_port, message, timeout=500)
                if response and response.get('type') == 'FILE_DATA': # Fetch success
                    with file_data_lock:
                        if file_data is None or file_data['blocks'] < response['blocks']: # Naive assumption that more chunks = more recent version. Actually not needed since all replicas are up to date. See report.
                            file_data = response
                    with success_lock:
                        success_count[0] += 1
                        if success_count[0] == self.READ_CONSISTENCY_LEVEL():
                            # Coordinator unblocks
                            success_event.set()
                    print(f"[{self.node_id}] File '{hdfsfilename}' retrieved from node '{node_id}'")
                else:
                    print(f"[{self.node_id}] File '{hdfsfilename}' not found on node '{node_id}'")

        threads = []
        for node_id in responsible_nodes:
            t = threading.Thread(target=fetch_from_node, args=(node_id,))
            t.start()
            threads.append(t)

        # Wait until read consistency level is reached
        success_event.wait()

        # Combine or select the most recent data
        if file_data:
            data = base64.b64decode(file_data['file_data'])

            # Write to disk
            with open(localfilename, 'wb') as f:
                f.write(data)
            print(f"[{self.node_id}] File '{hdfsfilename}' retrieved successfully")

            # Update cache
            with self.cache_lock:
                self.cache[hdfsfilename] = data
                if len(self.cache) > self.cache_size:
                    self.cache.popitem(last=False)
        else:
            print(f"[{self.node_id}] File '{hdfsfilename}' not found in hdfs")

    def handle_request_file(self, conn, message):
        """Send requested file to coordinator."""
        hdfsfilename = message['hdfsfilename']
        with self.file_lock:
            if hdfsfilename in self.file_table:
                # Send entire file. Chunks no longer matter since we are writting to disk.
                data = b''.join(chunk_data for _, chunk_data in self.file_table[hdfsfilename])
                seq_nums = len(self.file_table[hdfsfilename])
                response = {
                    'type': 'FILE_DATA',
                    'hdfsfilename': hdfsfilename,
                    'file_data': base64.b64encode(data).decode('ascii'),
                    'blocks': seq_nums
                }

                conn.sendall(json.dumps(response).encode('utf-8'))
            else: 
                response = {'type': 'ERROR'}
                conn.sendall(json.dumps(response).encode('utf-8'))

    def append(self, localfilename, hdfsfilename):
        """Append data to a file in hdfs. Act as coordinator, send append to replicas"""
        if os.path.exists(localfilename):
            with open(localfilename, 'rb') as f:
                data_to_append = f.read()
            # make sure chunk is unique
            self.client_seq_num += 1
            append_metadata = {
                'client_id': self.node_id,
                'seq_num': self.client_seq_num,
                'timestamp': time.time()
            }

            # Invalidate cache, write-through
            with self.cache_lock:
                if hdfsfilename in self.cache:
                    del self.cache[hdfsfilename]

            success_count = [0]
            success_lock = threading.Lock()
            success_event = threading.Event()

            responsible_nodes = self.get_responsible_nodes(hdfsfilename)

            # Concurrent append function
            def send_append(node_id):
                host, _ = self.get_node_address(node_id)
                if host is None:
                    return
                if node_id == self.node_id:  # local append
                    with self.file_lock:
                        if hdfsfilename in self.file_table:
                            self.file_table[hdfsfilename].append((append_metadata, data_to_append))
                            print(f"[{self.node_id}] Appended data to '{hdfsfilename}' locally")
                        else:
                            print(f"[{self.node_id}] File '{hdfsfilename}' does not exist locally")
                    with success_lock:
                        success_count[0] += 1
                        if success_count[0] == self.WRITE_CONSISTENCY_LEVEL():
                            success_event.set()
                else:  # append over network
                    message = {
                        'type': 'APPEND_FILE',
                        'hdfsfilename': hdfsfilename,
                        'data': base64.b64encode(data_to_append).decode('ascii'),
                        'metadata': append_metadata
                    }
                    ack = self.communicator.send_and_receive_tcp_message(host, self.hdfs_port, message, timeout=500)
                    if ack and ack.get('type') == 'ACK':
                        with success_lock:
                            success_count[0] += 1
                            if success_count[0] == self.WRITE_CONSISTENCY_LEVEL():
                                # Coordinator unblocks
                                success_event.set()

            threads = []
            for node_id in responsible_nodes:
                t = threading.Thread(target=send_append, args=(node_id,))
                t.start()
                threads.append(t)
            # Wait until write consistency level is reached
            success_event.wait()
            print(f"[{self.node_id}] Append File {hdfsfilename} success.")
        else:
            print(f"File '{localfilename}' not found")

    def handle_append_file(self, conn, message):
        """Append file from coordinator locally"""
        hdfsfilename = message['hdfsfilename']
        data = base64.b64decode(message['data'])
        metadata = message['metadata']
        # Invalidate cache
        with self.cache_lock:
            if hdfsfilename in self.cache:
                del self.cache[hdfsfilename]
        with self.file_lock:
            if hdfsfilename in self.file_table:
                if not any(entry[0] == metadata for entry in self.file_table[hdfsfilename]):
                    self.file_table[hdfsfilename].append((metadata, data))
                    print(f"[{self.node_id}] Appended data to '{hdfsfilename}' at node '{self.node_id}'")
                response = {'type': 'ACK', 'hdfsfilename': hdfsfilename}
                conn.sendall(json.dumps(response).encode('utf-8'))
            else: 
                response = {'type': 'ERROR'}
                conn.sendall(json.dumps(response).encode('utf-8'))
                print(f"[{self.node_id}] File '{hdfsfilename}' does not exist at node '{self.node_id}'")
                

    def merge(self, hdfsfilename):
        """Merge replicas of a file to ensure consistency."""
        responsible_nodes = self.get_responsible_nodes(hdfsfilename)
        all_chunks = []
        # Get all metadata of all chunks on all replicas of a file
        for node_id in responsible_nodes:
            host, _ = self.get_node_address(node_id)
            if host is None: 
                continue
            if node_id == self.node_id:
                with self.file_lock:
                    if hdfsfilename in self.file_table:
                        all_chunks.extend(self.file_table[hdfsfilename])
            else:
                message = {'type': 'REQUEST_CHUNKS_METADATA', 'hdfsfilename': hdfsfilename}
                response = self.communicator.send_and_receive_tcp_message(host, self.hdfs_port, message)
                if response and response.get('type') == 'FILE_CHUNKS_METADATA':
                    chunks_metadata = response['chunks_metadata']
                    # Since we only need metadata, we can proceed
                    for metadata in chunks_metadata:
                        # Append a placeholder for data
                        all_chunks.append((metadata, None))

        # Remove duplicates and sort based on metadata
        unique_chunks = self.merge_chunks(all_chunks)
        # Update local file
        for node_id in responsible_nodes:
            # send correct metadata list to all responsible nodes
            if node_id != self.node_id:
                host, _ = self.get_node_address(node_id)
                if host is None: 
                    continue
                chunks_metadata = [metadata for metadata, _ in unique_chunks]
                message = {
                    'type': 'UPDATE_CHUNKS',
                    'hdfsfilename': hdfsfilename,
                    'chunks_metadata': chunks_metadata
                }
                self.communicator.send_tcp_message(host, self.hdfs_port, message)
                print(f"[{self.node_id}] Sent UPDATE_CHUNKS to node '{node_id}' for file '{hdfsfilename}'")
            else: # update locally if self is responsible
                with self.file_lock:
                    self.file_table[hdfsfilename] = []
                    for metadata, data in unique_chunks:
                        if data is None:
                            # Data is missing; fetch from other nodes
                            data = self.fetch_chunk_data(hdfsfilename, metadata)
                        self.file_table[hdfsfilename].append((metadata, data))
                    print(f"[{self.node_id}] File '{hdfsfilename}' merged at node '{self.node_id}'")
                    # current_time = datetime.now()
                    # print(current_time)
                    
    def fetch_chunk_data(self, hdfsfilename, metadata):
        """Fetch chunk data from other nodes based on metadata."""
        responsible_nodes = self.get_responsible_nodes(hdfsfilename)
        for node_id in responsible_nodes:
            host, _ = self.get_node_address(node_id)
            if host is None: 
                continue
            if node_id == self.node_id:
                continue  # Skip self; data is already missing
            message = {
                'type': 'REQUEST_CHUNK_DATA',
                'hdfsfilename': hdfsfilename,
                'metadata': metadata
            }
            response = self.communicator.send_and_receive_tcp_message(host, self.hdfs_port, message)
            if response and response.get('type') == 'CHUNK_DATA':
                chunk_data = base64.b64decode(response['chunk_data'])
                return chunk_data
        print(f"[{self.node_id}] Unable to fetch chunk data for {metadata} of file '{hdfsfilename}'")
        return None

    def handle_update_chunks(self, conn, message):
        """Compute up to date version of file based on metadata list sent by coordinator"""
        hdfsfilename = message['hdfsfilename']
        received_chunks_metadata = message['chunks_metadata']

        with self.file_lock:
            local_chunks = self.file_table.get(hdfsfilename, [])
            # Mapping for quick lookup
            local_chunks_dict = {(m['client_id'], m['seq_num']): (m, d) for m, d in local_chunks}

            reordered_chunks = []

            for metadata in received_chunks_metadata:
                key = (metadata['client_id'], metadata['seq_num'])
                if key in local_chunks_dict:
                    # Chunk exists locally; add to reordered list
                    reordered_chunks.append(local_chunks_dict[key])
                else:
                    # Fetch from other nodes
                    print(f"[{self.node_id}] Missing chunk {metadata} for file '{hdfsfilename}'")
                    data = self.fetch_chunk_data(hdfsfilename, metadata)
                    reordered_chunks.append((metadata, data))

            # Update local file with reordered chunks
            self.file_table[hdfsfilename] = reordered_chunks
            print(f"[{self.node_id}] Merged file '{hdfsfilename}'")
            # current_time = datetime.now()
            # print(current_time)
            
    def handle_request_chunk_data(self, conn, message):
        """Send the chunk data being requested if present."""
        hdfsfilename = message['hdfsfilename']
        metadata = message['metadata']
        key = (metadata['client_id'], metadata['seq_num'])

        with self.file_lock:
            if hdfsfilename in self.file_table:
                # Create a mapping for quick lookup
                local_chunks_dict = {(m['client_id'], m['seq_num']): (m, d) for m, d in self.file_table[hdfsfilename]}
                if key in local_chunks_dict:
                    _, data = local_chunks_dict[key]
                    response = {
                        'type': 'CHUNK_DATA',
                        'hdfsfilename': hdfsfilename,
                        'chunk_data': base64.b64encode(data).decode('ascii')
                    }
                    conn.sendall(json.dumps(response).encode('utf-8'))
                    return
        # If chunk not found
        response = {
            'type': 'ERROR',
            'message': f"Chunk {metadata} for file '{hdfsfilename}' not found"
        }
        conn.sendall(json.dumps(response).encode('utf-8'))

    def handle_request_chunk_metadata(self, conn, message):
        """Send metadatas of chunks of a file to coordinator."""
        hdfsfilename = message['hdfsfilename']
        with self.file_lock:
            if hdfsfilename in self.file_table:
                chunks_metadata = [metadata for metadata, _ in self.file_table[hdfsfilename]]
                response = {
                    'type': 'FILE_CHUNKS_METADATA',
                    'hdfsfilename': hdfsfilename,
                    'chunks_metadata': chunks_metadata
                }
                conn.sendall(json.dumps(response).encode('utf-8'))
            else:
                response = {
                    'type': 'ERROR',
                    'message': f"File '{hdfsfilename}' not found"
                }
                conn.sendall(json.dumps(response).encode('utf-8'))


    def merge_chunks(self, all_chunks):
        """Merge chunks based on metadata to ensure correct ordering."""
        # Remove duplicates based on (client_id, seq_num)
        unique_chunks_dict = {}
        for metadata, data in all_chunks:
            key = (metadata['client_id'], metadata['seq_num'])
            if key not in unique_chunks_dict:
                unique_chunks_dict[key] = (metadata, data)
        # Sort chunks
        sorted_chunks = sorted(unique_chunks_dict.values(), key=lambda x: (x[0]['timestamp'], x[0]['client_id'], x[0]['seq_num']))
        return sorted_chunks
    
    # ========== Logic for node failure file re replication ==========

    def handle_node_failure(self, failed_node_id):
        """Handle node failure and re-replicate files."""
        # Check if any files need to be re-replicated
        threading.Thread(target=self.re_replicate_files, daemon=True).start()

    def re_replicate_files(self):
        start = time.time()
        with self.file_lock:
            for hdfsfilename in self.file_table.keys():
                # Get the list of responsible nodes
                responsible_nodes = self.get_responsible_nodes(hdfsfilename)
                if not responsible_nodes:
                    continue 
                # Check if self is the primary node
                primary_node_id = responsible_nodes[0]
                if self.node_id == primary_node_id:
                    # Self is the primary node
                    # Which responsible nodes already hold the file
                    current_replicas = [self.node_id]
                    for node_id in responsible_nodes:
                        if self.node_id != node_id:
                            if self.node_holds_file(node_id, hdfsfilename):
                                current_replicas.append(node_id)
                    # Replicas are missing, replicate the file
                    if len(current_replicas) < len(responsible_nodes):
                        missing_replicas = [node_id for node_id in responsible_nodes if node_id not in current_replicas]
                        for replica_id in missing_replicas:
                            self.send_file_to_node(hdfsfilename, replica_id)
                else:
                    # Not the primary node, no action needed
                    continue
        end = time.time()
        print(f"Time spent re replicating files: {end - start:.2f} seconds")
    # ===============================================================
                  
            
    # ========== Logic for node join file remap ==========
        
    def handle_node_addition(self, new_node_id):
        """Handle the addition of a new node to the membership list."""
        if self.node_id == new_node_id: # new node shouldn't be allowed to run this function. needed due to the way we set up callbacks
            return
        threading.Thread(target=self.transfer_files_to_new_node, args=(new_node_id,), daemon=True).start()
        
    def re_evaluate_file_responsibilities(self):
        """Re-evaluate file responsibilities after a membership change."""
        with self.file_lock:
            for hdfsfilename in list(self.file_table.keys()):
                responsible_nodes = self.get_responsible_nodes(hdfsfilename)
                # If the current node is not among the responsible nodes, delete the file                
                if self.node_id not in responsible_nodes:
                    del self.file_table[hdfsfilename]
                    print(f"[{self.node_id}] Deleted file '{hdfsfilename}' as it's no longer responsible.")
     
    def transfer_files_to_new_node(self, new_node_id):
        """Transfer files to the new node that it is now responsible for, does not replace re-replication logic, need both"""
        # This re_evaluate is not enough, there is a seperate loop that re_evaluate excess replicas.
        self.re_evaluate_file_responsibilities()
        new_node_hash = self.consistent_hash(new_node_id)
        with self.fd.lock:
            # Get the sorted list of node IDs based on consistent hashing
            node_ids = sorted(self.fd.membership_list.keys(), key=lambda nid: self.consistent_hash(nid))
        self_hash = self.consistent_hash(self.node_id)
        predecessor_hash = self.get_predecessor_hash(new_node_id, node_ids)

        # Check if self is the successor of the new node
        if self_hash == self.get_successor_hash(new_node_id, node_ids):
            # Self is the successor of the new node
            files_to_transfer = []
            with self.file_lock:
                for hdfsfilename in self.file_table.keys():
                    file_hash = self.consistent_hash(hdfsfilename)
                    # Determine if the file now belongs to the new node
                    if self.is_file_responsibility_shifted(file_hash, predecessor_hash, new_node_hash):
                        files_to_transfer.append(hdfsfilename)

            # Transfer files to the new node
            for hdfsfilename in files_to_transfer:
                self.send_file_to_node(hdfsfilename, new_node_id)
        
        # New node may not be primary replica, but may still be a replica and needs re-replication
        self.re_replicate_files()
                
    # ================================================================
          
    def node_holds_file(self, node_id, hdfsfilename):
        """Request to check if node holds given file."""
        if node_id == self.node_id:
            with self.file_lock:
                return hdfsfilename in self.file_table
        else:
            # Send a message to the node to check if it holds the file
            host, _ = self.get_node_address(node_id)
            if host is None: 
                return True # not actually holds file but since it is removed from membership we dont care 
            message = {'type': 'CHECK_FILE', 'hdfsfilename': hdfsfilename}
            response = self.communicator.send_and_receive_tcp_message(host, self.hdfs_port, message)
            if response:
                return response.get('holds_file', False)
            else:
                return False
                
    def get_predecessor_hash(self, node_id, node_ids):
        """Find predecessor"""
        index = node_ids.index(node_id)
        predecessor_index = (index - 1) % len(node_ids)
        predecessor_id = node_ids[predecessor_index]
        return self.consistent_hash(predecessor_id)

    def get_successor_hash(self, node_id, node_ids):
        """Find successor"""
        index = node_ids.index(node_id)
        successor_index = (index + 1) % len(node_ids)
        successor_id = node_ids[successor_index]
        return self.consistent_hash(successor_id)
    
    def is_file_responsibility_shifted(self, file_hash, old_hash, new_hash):
        """Determine if new node has become the primary node for a file."""
        # Handle wrap-around case in the ring
        if old_hash < new_hash:
            return old_hash < file_hash <= new_hash
        else:
            return file_hash > old_hash or file_hash <= new_hash

    def send_file_to_node(self, hdfsfilename, node_id):
        """Send file to node's file table. Need to retain chunk information to make merge logic consistent"""
        host, _ = self.get_node_address(node_id)
        if host is None:
            return
        with self.file_lock:
            chunks = self.file_table[hdfsfilename]
        if not chunks:
            print(f"[{self.node_id}] No chunks to send for file '{hdfsfilename}'")
            return

        # Send the initial chunk as CREATE_FILE
        initial_metadata, initial_data = chunks[0]
        create_message = {
            'type': 'CREATE_FILE',
            'hdfsfilename': hdfsfilename,
            'file_data': base64.b64encode(initial_data).decode('ascii'),
            'metadata': initial_metadata
        }
        self.communicator.send_tcp_message(host, self.hdfs_port, create_message)
        print(f"[{self.node_id}] Sending '{hdfsfilename}' to node '{node_id}'")

        # Send subsequent chunks as APPEND_FILE messages
        for metadata, data in chunks[1:]:
            append_message = {
                'type': 'APPEND_FILE',
                'hdfsfilename': hdfsfilename,
                'data': base64.b64encode(data).decode('ascii'),
                'metadata': metadata
            }
            self.communicator.send_tcp_message(host, self.hdfs_port, append_message)
        
    def handle_check_file(self, conn, message):
        """Replies whether given file is present in node"""
        hdfsfilename = message['hdfsfilename']
        with self.file_lock:
            holds_file = hdfsfilename in self.file_table
        response = {'type': 'CHECK_FILE_RESPONSE', 'holds_file': holds_file}
        
        conn.sendall(json.dumps(response).encode('utf-8'))

    def ls(self, hdfsfilename):
        """List all machines where this file is currently being stored."""
        responsible_nodes = self.get_responsible_nodes(hdfsfilename)
        print(f"[{self.node_id}] File '{hdfsfilename}' with ID '{self.consistent_hash(hdfsfilename)}' is stored on nodes:")
        for node_id in responsible_nodes:
            host, _ = self.get_node_address(node_id)
            print(f"  Node ID: {node_id}, Host: {host}")

    def store(self):
        """List all files currently being stored at this machine."""
        with self.file_lock:
            print(f"[{self.node_id}] Files stored at node '{self.node_id}':")
            for hdfsfilename in self.file_table.keys():
                ring_id = self.consistent_hash(hdfsfilename)
                print(f"  File: {hdfsfilename}, Ring ID: {ring_id}")

    def get_from_replica(self, vm_address, hdfsfilename, localfilename):
        """Get a file from a particular replica indicated by VM address."""
        host = vm_address
        message = {'type': 'REQUEST_FILE', 'hdfsfilename': hdfsfilename}
        response = self.communicator.send_and_receive_tcp_message(host, self.hdfs_port, message)
    
        if response and 'file_data' in response:
            file_data = base64.b64decode(response['file_data']) if isinstance(response['file_data'], str) else response['file_data']
        
            with open(localfilename, 'wb') as f:
                f.write(file_data)
            print(f"[{self.node_id}] File '{hdfsfilename}' retrieved from '{vm_address}'")
        else:
            print(f"[{self.node_id}] File '{hdfsfilename}' not found at '{vm_address}'")

    def list_mem_ids(self):
        """List the membership list along with their ring IDs."""
        with self.fd.lock:
            members = [(self.consistent_hash(node_id), node_id, info['host']) for node_id, info in self.fd.membership_list.items()]
        members.sort()
        print(f"[{self.node_id}] Membership List with Ring IDs:")
        for ring_id, node_id, host in members:
            print(f"  Ring ID: {ring_id}, Node ID: {node_id}, Host: {host}")

    def multiappend(self, hdfsfilename, vm_list, localfilename_list, verbose=True):
        """Launches appends from VMs simultaneously to the hdfsfilename."""
        if len(vm_list) != len(localfilename_list):
            print(f"[{self.node_id}] Error: Number of VMs and local filenames must match")
            return
        threads = []
        for vm, localfilename in zip(vm_list, localfilename_list):
            t = threading.Thread(target=self.send_remote_append, args=(vm, hdfsfilename, localfilename, verbose))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
        print(f"[{self.node_id}] Multiappend operation completed.")

    def send_remote_append(self, node_id, hdfsfilename, localfilename, verbose):
        """Tell node to perform an append command."""
        host, _ = self.get_node_address(node_id)
        if host is None:
            print(f"[{self.node_id}] Node ID '{node_id}' not found in membership list.")
            return
        message = {
            'type': 'REMOTE_APPEND',
            'hdfsfilename': hdfsfilename,
            'localfilename': localfilename
        }
        response = self.communicator.send_and_receive_tcp_message(host, self.hdfs_port, message)
        if response and response.get('type') == 'ACK':
            if verbose:
                print(f"[{self.node_id}] Mutiappend append to '{hdfsfilename}' at [{node_id}] of file: {localfilename} succeeded.")
        else:
            error_msg = 'No response received' if response is None else response.get('message', 'Unknown error')
            print(f"[{self.node_id}] Remote append to '{hdfsfilename}' at [{node_id}] failed: {error_msg}")


    def handle_remote_append(self, conn, message):
        """Perform concurrent append at local node."""
        hdfsfilename = message['hdfsfilename']
        localfilename = message['localfilename']
        if os.path.exists(localfilename):
            self.append(localfilename, hdfsfilename)
            response = {'type': 'ACK'}
            conn.sendall(json.dumps(response).encode('utf-8'))
        else:
            response = {'type': 'ERROR', 'message': f"Local file '{localfilename}' not found"}
            conn.sendall(json.dumps(response).encode('utf-8'))
            
    def multiappend_loop(self, hdfsfilename, vm_list, localfilename_list):
        """Multiappend 1000 times"""
        num_iter = math.ceil(1000 / len(vm_list))
        for _ in range(num_iter):
            self.multiappend(hdfsfilename, vm_list, localfilename_list, verbose=False)
        print(f"[{self.node_id}] {num_iter} concurrent appends completed.")
        
    def load_dataset(self, localfilename):
        """Report code"""
        for i in range(10000):
            hdfsname = f"{localfilename}_{i}"
            self.create(localfilename, hdfsname)
        print(f"[{self.node_id}] Loaded dataset.")
        
    def read_dataset(self, hdfsfilename, distribution='uniform'):
        """Report code"""
        start_time = time.time()
        indices = []
        
        if distribution == 'zipfian':
            high_frequency = int(20000 * 0.7)  
            low_frequency = 20000 - high_frequency 

            indices.extend([random.randint(0, 999) for _ in range(high_frequency)])
            indices.extend([random.randint(1000, 9999) for _ in range(low_frequency)])
            random.shuffle(indices)
        
        else:
            # Uniformly select random indices between 0 and 9999
            indices = [random.randint(0, 9999) for _ in range(20000)]
        
        for i in range(20000):
            hdfsname = f"{hdfsfilename}_{indices[i]}"
            self.get(hdfsname, 'temp') # garbage file
        
        end_time = time.time()
        print(f"[{self.node_id}] Finished reading dataset with {distribution} distribution.")
        print(f"Time taken: {end_time - start_time:.2f} seconds")
        
    def read_dataset_with_appends(self, hdfsfilename, localfile, distribution='uniform'):
        start_time = time.time()  # Start the timer

        indices = []
        
        if distribution == 'zipfian':
            high_frequency = int(20000 * 0.7)  
            low_frequency = 20000 - high_frequency 
            
            indices.extend([random.randint(0, 999) for _ in range(high_frequency)])
            indices.extend([random.randint(1000, 9999) for _ in range(low_frequency)])
            random.shuffle(indices)
        
        else:
            # Uniformly select random indices between 0 and 9999
            indices = [random.randint(0, 9999) for _ in range(20000)]

        for i in range(20000):
            hdfsname = f"{hdfsfilename}_{indices[i]}"
            operation = random.choices(['get', 'append'], weights=[0.9, 0.1], k=1)[0]  # 90% gets, 10% appends

            if operation == 'get':
                self.get(hdfsname, 'temp')
            else:
                self.append(localfile, hdfsname)

        end_time = time.time() 

        print(f"[{self.node_id}] Finished reading dataset with {distribution} distribution and 10% appends.")
        print(f"Time taken: {end_time - start_time:.2f} seconds")
        
    def fd_enable_sus(self):
        with self.fd.lock:
            self.fd.enable_sus = True
        message = {'type': 'ENABLE_SUS'}
        self.fd.broadcast(message)
        
    def fd_disable_sus(self):
        with self.fd.lock:
            self.fd.enable_sus = False
        message = {'type': 'DISABLE_SUS'}
        self.fd.broadcast(message)
        
    def fd_show_sus_status(self):
        with self.fd.lock:
            print(f"Sus status: {self.fd.enable_sus}")


    def user_input_loop(self):
        """Handle user inputs for file operations."""
        while self.alive:
            try:
                user_input = input("Enter command: ")
                if user_input.startswith("create"):
                    _, localfile, hdfsfile = user_input.strip().split()
                    self.create(localfile, hdfsfile)
                elif user_input.startswith("get_from_replica"):
                    _, vm_address, hdfsfile, localfile = user_input.strip().split()
                    self.get_from_replica(vm_address, hdfsfile, localfile)
                elif user_input.startswith("get"):
                    _, hdfsfile, localfile = user_input.strip().split()
                    self.get(hdfsfile, localfile)
                elif user_input.startswith("append"):
                    _, localfile, hdfsfile = user_input.strip().split()
                    self.append(localfile, hdfsfile)
                elif user_input.startswith("merge"):
                    _, hdfsfile = user_input.strip().split()
                    # for report
                    current_time = datetime.now()
                    print(current_time)
                    self.merge(hdfsfile)
                elif user_input.startswith("ls"):
                    _, hdfsfile = user_input.strip().split()
                    self.ls(hdfsfile)
                elif user_input.strip() == "store":
                    self.store()
                elif user_input.strip() == "list_mem":
                    self.fd.list_membership()
                elif user_input.strip() == "list_mem_ids":
                    self.list_mem_ids()
                elif user_input.startswith("load_dataset"):
                    _, localfile = user_input.strip().split()
                    self.load_dataset(localfile)
                elif user_input.strip() == "leave":
                    self.stop()
                    break
                elif user_input.startswith("multiappend"):
                    tokens = user_input.strip().split()
                    if len(tokens) < 4:
                        print(f"[{self.node_id}] Error: Not enough arguments for multiappend")
                        continue
                    hdfsfilename = tokens[1]
                    # '--' separates VM list and localfilename list
                    if '--' in tokens:
                        idx = tokens.index('--')
                        vm_list = tokens[2:idx]
                        localfilename_list = tokens[idx+1:]
                    else:
                        num_vms = (len(tokens) - 2) // 2
                        vm_list = tokens[2:2+num_vms]
                        localfilename_list = tokens[2+num_vms:]
                    self.multiappend(hdfsfilename, vm_list, localfilename_list)
                elif user_input.startswith("report_multiappend_1000"):
                    tokens = user_input.strip().split()
                    if len(tokens) < 4:
                        print(f"[{self.node_id}] Error: Not enough arguments for multiappend")
                        continue
                    hdfsfilename = tokens[1]
                    # Assuming '--' separates VM list and localfilename list
                    if '--' in tokens:
                        idx = tokens.index('--')
                        vm_list = tokens[2:idx]
                        localfilename_list = tokens[idx+1:]
                    else:
                        num_vms = (len(tokens) - 2) // 2
                        vm_list = tokens[2:2+num_vms]
                        localfilename_list = tokens[2+num_vms:]
                    self.multiappend_loop(hdfsfilename, vm_list, localfilename_list)
                elif user_input.startswith("report_read_dataset"):
                    _, hdfsfile, distribution = user_input.strip().split()
                    self.read_dataset(hdfsfile, distribution=distribution)
                elif user_input.startswith("report_read_append_dataset"):
                    _, localfile, hdfsfile, distribution = user_input.strip().split()
                    self.read_dataset_with_appends(hdfsfile, localfile, distribution=distribution)
                # Debugging commands
                elif user_input.startswith("file_table"):
                    print(self.file_table)
                elif user_input.startswith("enable_sus"):
                    self.fd_enable_sus()
                elif user_input.startswith("disable_sus"):
                    self.fd_disable_sus()
                elif user_input.startswith("status_sus"):
                    self.fd_show_sus_status()
                else:
                    print(f"[{self.node_id}] Unknown command: {user_input}")
            except Exception as e:
                print(f"[{self.node_id}] Error processing command: {e}")

def parse_arguments():
    """Parse input arguments."""
    parser = argparse.ArgumentParser(description='HyDFS Node')
    parser.add_argument('--fd-port', type=int, default=5000, help='Port for failure detector')
    parser.add_argument('--hdfs-port', type=int, default=6000, help='Port for hdfs')
    parser.add_argument('--is-introducer', action='store_true', help='Flag to indicate if this node is the introducer')
    return parser.parse_args()

def main():
    args = parse_arguments()
    introducer_host = "fa24-cs425-7001.cs.illinois.edu"  
    introducer_port = INTRODUCER_PORT

    node = HDFS(
        node_id=socket.gethostname().split('-')[2][2:4],
        host=socket.gethostname(),
        introducer_addr=(introducer_host, introducer_port),
        hdfs_port=args.hdfs_port,
        fd_port=args.fd_port,
        is_introducer=args.is_introducer
    )
    node.start()

if __name__ == "__main__":
    main()
