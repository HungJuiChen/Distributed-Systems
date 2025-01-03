import subprocess
from HDFS import Communicator
from HDFS_client import HDFSClient
from Scheduler import Scheduler
from fd import Node
import threading
import time
import socket
import argparse
import json
import hashlib
import random

RAINSTORM_DIR = 'rainstorm_logs'
RAINSTORM_FD_PORT = 5050
BATCH_SIZE = 10



class RainStorm:
    def __init__(self, node_id, host, introducer_addr, rainstorm_port, hdfs_port, fd_port, is_introducer=False):
        self.node_id = node_id
        self.host = host
        self.introducer_addr = introducer_addr
        self.is_introducer = is_introducer
        self.port = rainstorm_port
        self.alive = True

        self.hdfs_client = HDFSClient(host, hdfs_port)
        
        self.communicator = Communicator()
        
        self.fd = Node(
            node_id=node_id,
            host=host,
            introducer_addr=introducer_addr,
            port=fd_port,
            is_introducer=is_introducer
        )
        
        self.fd.rainstorm = self  
        
        #TODO: set failure callback in self.hdfs.fd
        self.fd.set_add_member_callback(self.handle_node_join)
        self.fd.set_failure_callback(self.handle_node_failure)
        #TODO: define other useful variables
        
        # Leader exclusive
        # TODO: handle node failure and rejoin for scheduler
        self.scheduler = Scheduler(self) 
        self.op1 = None # reset for every rainstorm command
        self.op2 = None # reset for every rainstorm command
        self.lock = threading.RLock()
        self.need_recover = threading.Event()
        self.up_to_date = threading.Event()
        self.up_to_date.set()
        self.wait_for_recover = threading.Event()
        self.wait_for_recover.set()
        
        self.processed_ids_buffer = {} # task_id: [buffer]
        self.output_buffer = {}
        self.ack_buffer = {}
        self.dest_buffer = {}        
        self.dest_files = {} 
        self.process_buffer_lock = threading.RLock()
        self.output_buffer_lock = threading.RLock()
        self.ack_buffer_lock = threading.RLock()
        self.dest_buffer_lock = threading.RLock()
        
    def start(self):
        """Start the rainstorm server and run hdfs on a seperate port."""
        # Assume HDFS is running in another process
        threading.Thread(target=self.fd.start, daemon=True).start()
        # Start the Rainstorm server
        print(f"[{self.node_id}] Rainstorm server running on port {self.port}")
        threading.Thread(target=self.run_server, daemon=True).start()
        
        try:
            while self.alive:
                time.sleep(1)
        except KeyboardInterrupt:
                self.stop()

    def stop(self):
        """Stop the rainstorm node"""
        self.alive = False
        if self.sock:
            self.sock.close()
        self.fd.stop()
        print(f"[{self.node_id}] Rainstorm node has stopped")
        
    def periodic_flush(self):
        """Periodically flush all buffers."""
        while self.alive:
            time.sleep(10)
            self.flush_all_buffers()
        
        
    def flush_all_buffers(self):
        """Flush all buffers if any remain. ."""

        for task in self.scheduler.get_self_tasks():
            task_id = task['task_id']
            self.flush_processed_ids(task_id)
            self.flush_output(task_id)
            self.flush_dest(task_id)
            self.flush_ack(task_id)
            
    def flush_processed_ids(self, task_id):
        with self.process_buffer_lock:
            if self.processed_ids_buffer[task_id]:
                file = f"{RAINSTORM_DIR}/{task_id}_processed_ids.log"
                data = '\n'.join(self.processed_ids_buffer[task_id]) + '\n'
                self.hdfs_client.append(data, file, localfilename_is_data=True)
                self.processed_ids_buffer[task_id].clear()

    def flush_output(self, task_id):
        with self.output_buffer_lock:
            if self.output_buffer[task_id]:
                file = f"{RAINSTORM_DIR}/{task_id}_output.log"
                data = ''.join(self.output_buffer[task_id])
                self.hdfs_client.append(data, file, localfilename_is_data=True)
                self.output_buffer[task_id].clear()

    def flush_ack(self, task_id):
        with self.ack_buffer_lock:
            if self.ack_buffer[task_id]:
                file = f"{RAINSTORM_DIR}/{task_id}_acks.log"
                data = '\n'.join(self.ack_buffer[task_id]) + '\n'
                self.hdfs_client.append(data, file, localfilename_is_data=True)
                self.ack_buffer[task_id].clear()
                
    def flush_dest(self, task_id):
        with self.dest_buffer_lock:
            if task_id in self.dest_buffer and self.dest_buffer[task_id]:
                dest_file = self.dest_files.get(task_id, None)
                if dest_file:
                    data = ''.join(self.dest_buffer[task_id])  # lines already include '\n'
                    self.hdfs_client.append(data, dest_file, localfilename_is_data=True)
                self.dest_buffer[task_id].clear()

    def run_server(self):
        """Listen to incoming traffic in a loop."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('', self.port))
        backlog = socket.SOMAXCONN
        self.sock.listen(5)
        print(f"[{self.node_id}] RainStorm server listening on port {self.port}")

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
            #TODO: put message handlers here
            if msg_type == 'START_STREAM':
                if self.is_introducer:
                    op1, op2, src, dest, num_tasks = tuple(message.get('args'))
                    threading.Thread(target=self.leader_start_stream, args=(op1, op2, src, dest, num_tasks), daemon=True).start()
                    
            elif msg_type == 'ASSIGN_TASK':
                # TODO: worker recieves task
                task_info = message.get('task_info')
                threading.Thread(target=self.execute_task, args=(task_info,), daemon=True).start()
                
            elif msg_type == 'TASK_COMPLETED':
                if self.is_introducer:
                    task_id = message.get('task_id')
                    worker_id = message.get('worker_id')
                    self.scheduler.remove_task(worker_id, task_id)
                    print(f"[{self.node_id}] Task {task_id} completed by worker {worker_id}")
                    
            elif msg_type == 'UPDATE_NODE_PER_STAGE':
                self.scheduler.worker_update_node_per_stage(message['nodes_per_stage'])  
                self.up_to_date.set()
                # failure handling finished. recover potential lost data
                # we limit source and recover task to not run exclusively to cap the 
                # overall bandwidth since we are communicating between 2 processes
                self.need_recover.set()
                
            elif msg_type == 'RECORD':
                # process record
                threading.Thread(target=self.process_record, args=(message,), daemon=True).start()
                
            elif msg_type == 'ACK':
                self.handle_ack(message)
                
            elif msg_type == 'KILL':
                self.stop()
                
            elif msg_type == 'FLUSH':
                self.flush_all_buffers()
                     
            else:
                print(f"[{self.node_id}] Unknown TCP message type: {msg_type}")
        except Exception as e:
            print(f"[{self.node_id}] Error handling TCP connection from {addr}: {e}")
        finally:
            conn.close()
            
    def user_input_loop(self):
        """Handle user inputs for file operations."""
        while self.alive:
            try:
                user_input = input("Enter command: ")
                
                # TODO: put user commands here
                # Testing function of HDFS
                if user_input.strip() == "list_mem":
                    self.fd.list_membership()
                elif user_input.startswith("create"):
                    _, localfile, hdfsfile = user_input.strip().split()  
                    self.hdfs_client.create(localfile, hdfsfile)     
                elif user_input.startswith("get"):
                    _, hdfsfile, localfile = user_input.strip().split()
                    self.hdfs_client.get(hdfsfile, localfile)
                elif user_input.startswith("append"):
                    _, localfile, hdfsfile = user_input.strip().split()
                    self.hdfs_client.append(localfile, hdfsfile)
                elif user_input.startswith("merge"):
                    _, hdfsfile = user_input.strip().split()
                    self.hdfs_client.merge(hdfsfile)        
                # Commands for Rainstorm
                elif user_input.startswith("RainStorm"):
                    _, op1, op2, src, dest, num_tasks = user_input.strip().split()
                    self.start_stream(op1, op2, src, dest, int(num_tasks))
                # Debugging commands
                elif user_input.startswith("FailRainStorm"):
                    _, op1, op2, src, dest, num_tasks = user_input.strip().split()
                    self.start_stream(op1, op2, src, dest, int(num_tasks))
                    time.sleep(1.5)
                    self.kill_2_node()
                elif user_input.startswith('flush'):
                    if self.is_introducer:
                        self.scheduler.broadcast({'type': 'FLUSH'})
                else:
                    print(f"[{self.node_id}] Unknown command: {user_input}")
            except Exception as e:
                print(f"[{self.node_id}] Error processing command: {e}") 
                
    def kill_2_node(self):
        """ kill 2 nodes in op1 and op2 for demo """
        if not self.is_introducer:
            print(f"[{self.node_id}] INVALID: Cannot kill nodes as worker")  
            return 
        message = {'type': 'KILL'}
        tasks = self.scheduler.get_tasks('op1') + self.scheduler.get_tasks('op2')
        tasks2kill = random.sample(tasks, 2)
        for task in tasks2kill:
            host, _ = self.get_node_address(task['worker_id'])
            if host is None:
                print(f"[{self.node_id}] Target node is already down")
                return
            self.communicator.send_tcp_message(host, self.port, message)
        
                
    def get_node_address(self, node_id):
        """Get the host and port of a node from the membership list."""
        with self.fd.lock:
            node_info = self.fd.membership_list.get(node_id)
            if node_info:
                return node_info['host'], self.port
            else:
                return None, None
                
    def start_stream(self, op1, op2, src, dest, num_tasks):
        """Start a rainstorm command. If called on worker node, send to leader"""
        if self.is_introducer:
            self.leader_start_stream(op1, op2, src, dest, num_tasks)
        else: # send command to leader
            message = {
                'type': 'START_STREAM',
                'args': (op1, op2, src, dest, num_tasks)
            }
            leader_host, leader_port = self.introducer_addr
            self.communicator.send_tcp_message(leader_host, leader_port, message)
            
    # leader code ================================================================================================================================================
            
    def leader_start_stream(self, op1, op2, src, dest, num_tasks):
        """Start rainstorm command. Should only be called on leader node"""
        print(f"[{self.node_id}] Leader starting stream: '{op1} {op2} {src} {dest}' on {num_tasks} workers...") 
 
        with self.fd.lock:
            worker_nodes = [node_id for node_id in self.fd.membership_list if node_id != self.node_id]
        self.scheduler.reset({node_id: [] for node_id in worker_nodes}, num_tasks)
        self.op1 = op1
        self.op2 = op2
        op2_state = read_first_line(op2)
        stateful = op2_state == '# stateful'

        for id in range(num_tasks):
            # Assign task to worker with the least number of tasks assigned
            worker_id = self.scheduler.get_min_task_worker()

            task_info = {
                # self metadata
                'task_id': self.scheduler.new_task_id(), # unique id across all tasks
                'worker_id': worker_id,
                'stage': 'source',
                'op': None,
                'input': src,
                'output': None,
                # partition info
                'num_partitions': num_tasks,
                'partition_id': id,
                'dest': dest,
                'op1': op1,
                'op2': op2
            }
            self.scheduler.add_task(task_info)
            
        for id in range(num_tasks):
            worker_id = self.scheduler.get_min_task_worker()
            task_info = {
                'task_id': self.scheduler.new_task_id(),
                'worker_id': worker_id,
                'stage': 'op1',
                'op': op1,
                'input': None,
                'output': None,
                'num_partitions': num_tasks,
                'partition_id': id,
                'dest': dest,
                'op1': op1,
                'op2': op2,
                'stateful': False
            }
            self.scheduler.add_task(task_info)
            
        for id in range(num_tasks):
            worker_id = self.scheduler.get_min_task_worker()
            task_info = {
                'task_id': self.scheduler.new_task_id(),
                'worker_id': worker_id,
                'stage': 'op2',
                'op': op2,
                'input': None,
                'output': dest,
                'num_partitions': num_tasks,
                'partition_id': id,
                'dest': dest,
                'op1': op1,
                'op2': op2,
                'stateful': stateful
            }
            self.scheduler.add_task(task_info)
        
        for task in (
            self.scheduler.get_tasks('source') +
            self.scheduler.get_tasks('op1') +
            self.scheduler.get_tasks('op2')
        ):
            worker_id = task['worker_id']
            message = {
                'type': 'ASSIGN_TASK',
                'task_info': task
            }          
            self.leader_assign_task_to_worker(message)
        self.scheduler.leader_update_node_per_stage()
            
    def leader_assign_task_to_worker(self, task_message):
        """Thread function to assign task to worker."""
        while self.alive:
            worker_id = task_message['task_info']['worker_id']
            host, _ = self.get_node_address(worker_id)
            if host is None:
                # reassign the task
                time.sleep(1)
                self.scheduler.remove_task(worker_id, task_message['task_info']['task_id'])
                task_message['task_info']['worker_id'] = self.scheduler.get_min_task_worker()
                self.scheduler.add_task(task_message['task_info'])
                continue
            self.communicator.send_tcp_message(host, self.port, task_message)
            print(f"[{self.node_id}] Assigned task {task_message['task_info']['task_id']} to worker {worker_id}")
            break

        
    
    
    
    
    
    # worker code ========================================================================================================================
    
    def execute_task(self, task_info):
        """Execute the assigned task."""
        task_id = task_info['task_id']
        stage = task_info['stage']
        partition_id = task_info['partition_id']
        num_partitions = task_info['num_partitions']
        input_file = task_info['input']
        
        self.create_logs(task_id)
        
        self.scheduler.local_add_task(task_info)

        print(f"[{self.node_id}] Starting task {task_id} for stage '{stage}'")
        
        threading.Thread(target=self.recover_thread, args=(task_info,), daemon=True).start()

        if stage == 'source':

            processed_ids = self.load_processed_ids(self.load_output(task_id))
            start_index, data = self.read_partition_from_hdfs(input_file, partition_id, num_partitions)
            unacked_records = {}
            line_number = start_index

            for line in data:
                self.wait_for_recover.wait()
                time.sleep(0.05) # cap the bandwidth
                line_number += 1
                unique_id = f"{task_id}:{line_number}"
                if unique_id in processed_ids:
                    continue  # Skip duplicate
                key = f"{input_file}:{line_number}"
                value = line.strip()

                self.log_output(task_info, unique_id, key, value)
                op1_worker_id = self.partition_key_to_worker(key, stage='op1')
                self.send_record(unique_id, key, value, op1_worker_id, task_info)
                unacked_records[unique_id] = (key, value)
                print(f"[{self.node_id}] Task {task_id}: Sent {key} -> {value} to worker {op1_worker_id}")

            print(f"[{self.node_id}] Task {task_id}: finished source")

        else:
            self.execute_operator_task(task_info)

            
    def execute_operator_task(self, task_info):
        """Execute an operator task (op1 or op2)."""
        stage = task_info['stage']
        print(f"[{self.node_id}] Starting task {task_info['task_id']} for stage '{stage}'")

        # actually does nothing special right now
        
        
    def recover_thread(self, task_info):
        while self.alive:
            self.need_recover.wait()
            time.sleep(10) # wait for system to be stable
            self.recover_task(task_info)
            self.wait_for_recover.set()
            self.need_recover.clear()

            
    def recover_task(self, task_info):
        """Recover the task after failure."""
        if task_info['stage'] == 'op2':
            with self.lock:
                output_lines = self.load_output(task_info['task_id'])
                dest_file = task_info['dest']
                dest_lines = self.load_dest_lines(dest_file, task_info['task_id'])
            output_ids = set()
            for line in output_lines:
                record = json.loads(line.strip())
                output_ids.add(record['unique_id'])

            dest_ids = set()
            for line in dest_lines:
                record = json.loads(line.strip())
                dest_ids.add(record['unique_id'])

            # Log to output but not delivered, need to recover
            undelivered_ids = output_ids - dest_ids

            # Resend these undelivered records
            for line in output_lines:
                record = json.loads(line.strip())
                uid = record['unique_id']
                if uid in undelivered_ids:
                    key = record['key']
                    value = record['value']
                    
                    self.log_to_dest(task_info, uid, key, value)
            return

        # Resend unacked records
        with self.lock:        
            data = self.load_output(task_info['task_id'])
            acked_ids = self.load_acked_ids(task_info)
        if data:
            for line in data:
                record = json.loads(line.strip())
                unique_id = record['unique_id']
                if unique_id not in acked_ids:
                    key = record['key']
                    value = record['value']
                    time.sleep(0.05) # cap the bandwidth
                    if task_info['stage'] == 'source':
                        op1_worker_id = self.partition_key_to_worker(key, stage='op1')
                        self.send_record(unique_id, key, value, op1_worker_id, task_info)
                    elif task_info['stage'] == 'op1':
                        op2_worker_id = self.partition_key_to_worker(key, stage='op2')
                        self.send_record(unique_id, key, value, op2_worker_id, task_info)

        
    def read_partition_from_hdfs(self, filename, partition_id, num_partitions):
        """Read a partition of a file from HDFS."""
        all_data = self.hdfs_client.read_file(filename)
        partition_size = len(all_data) // num_partitions
        start_index = partition_id * partition_size
        if partition_id == num_partitions - 1:
            end_index = len(all_data)
        else:
            end_index = (partition_id + 1) * partition_size
        return start_index ,all_data[start_index:end_index]
    
    def load_output(self, task_id):
        output_log_file = f"{RAINSTORM_DIR}/{task_id}_output.log"
        with self.output_buffer_lock:
            lines = self.hdfs_client.read_file(output_log_file) or []
            if task_id in self.output_buffer and self.output_buffer[task_id]:
                buffered_lines = [line for line in self.output_buffer[task_id]]
                lines += buffered_lines
        return lines


    def load_processed_ids(self, data):
        """Load processed unique IDs from output log."""
        seen_ids = set()
        if data:
            for line in data:
                record = json.loads(line.strip())
                seen_ids.add(record['unique_id'])
        return seen_ids
    
    def load_dest_lines(self, dest_file, task_id):
        """Load the final dest lines from HDFS plus any buffered lines not flushed yet."""
        with self.dest_buffer_lock:
            lines = self.hdfs_client.read_file(dest_file) or []
            if task_id in self.dest_buffer:
                buffered_lines = [line for line in self.dest_buffer[task_id]]
                lines += buffered_lines
        return lines

    def load_acked_ids(self, task_info):
        ack_file = f"{RAINSTORM_DIR}/{task_info['task_id']}_acks.log"
        with self.ack_buffer_lock:
            data = self.hdfs_client.read_file(ack_file)
            acked = set(line.strip() for line in data) if data else set()
            if task_info['task_id'] in self.ack_buffer:
                for unique_id in self.ack_buffer[task_info['task_id']]:
                    acked.add(unique_id.strip())
        return acked
    
    def log_to_dest(self, task_info, unique_id, key, value):
        """Add line (including unique_id) to dest buffer and flush if needed."""
        task_id = task_info['task_id']
        with self.dest_buffer_lock:
            if task_id not in self.dest_buffer:
                self.dest_buffer[task_id] = []
            if task_id not in self.dest_files:
                self.dest_files[task_id] = task_info['dest']

            # Store as JSON line for easy parsing
            line = json.dumps({'unique_id': unique_id, 'key': key, 'value': value}) + '\n'
            self.dest_buffer[task_id].append(line)
            if len(self.dest_buffer[task_id]) >= BATCH_SIZE:
                self.flush_dest(task_id)

    def log_processed_id(self, task_info, unique_id):
        """Log the processed unique ID to HyDFS."""
        with self.process_buffer_lock:
            self.processed_ids_buffer[task_info['task_id']].append(unique_id)

            if len(self.processed_ids_buffer[task_info['task_id']]) >= BATCH_SIZE:
                self.flush_processed_ids(task_info['task_id'])
    
            
    def create_logs(self, task_id):
        """ create logs in advance so we dont have to check for file exist """
        log_file = f"{RAINSTORM_DIR}/{task_id}_processed_ids.log"
        output_log = f"{RAINSTORM_DIR}/{task_id}_output.log"
        ack_log = f"{RAINSTORM_DIR}/{task_id}_acks.log"
        self.hdfs_client.create('', log_file, localfilename_is_data=True)
        self.hdfs_client.create('', output_log, localfilename_is_data=True)
        self.hdfs_client.create('', ack_log, localfilename_is_data=True)
        
        self.processed_ids_buffer[task_id] = []
        self.output_buffer[task_id] = []
        self.ack_buffer[task_id] = []
        
    def log_output(self, task_info, unique_id, key, value):
        """Log the key-value output to HyDFS."""
        output_data = json.dumps({'unique_id': unique_id, 'key': key, 'value': value}) + '\n'
        with self.output_buffer_lock:
            self.output_buffer[task_info['task_id']].append(output_data)
            if len(self.output_buffer[task_info['task_id']]) >= BATCH_SIZE:
                self.flush_output(task_info['task_id'])
    
    def log_ack(self, task_info, unique_id):
        """Log the acknowledgment received to HyDFS. Actually does nothing write now since we are not using it"""
        return

        
    def send_record(self, unique_id, key, value, worker_id, task_info):
        """Send a record to the next stage task."""
        host, _ = self.get_node_address(worker_id)
        while host is None:
            self.up_to_date.wait()
            worker_id = self.partition_key_to_worker(key, task_info['stage'])
            host, _ = self.get_node_address(worker_id)
        message = {
            'type': 'RECORD',
            'unique_id': unique_id,
            'key': key,
            'value': value,
            'sender_task_id': task_info['task_id'],
            'sender_worker_id': self.node_id,
            'stage': task_info['stage']
        }
        self.communicator.send_tcp_message(host, self.port, message)

    def handle_ack(self, message):
        """Handle acknowledgment messages."""
        unique_id = message['unique_id']
        sender_task_id = message['sender_task_id']
        # Log the acknowledgment
        self.log_ack(self.scheduler.local_get_task_by_id(sender_task_id), unique_id)
        
    def process_record(self, message):
        """Process an incoming record."""
        unique_id = message['unique_id']
        key = message['key']
        value = message['value']
        sender_task_id = message['sender_task_id']
        sender_worker_id = message['sender_worker_id']
        task_info = self.scheduler.local_get_next_task(message['stage'])
        while task_info is None:
            task_info = self.scheduler.local_get_next_task(message['stage'])
        stage = task_info['stage']
        # Load processed IDs
        
        if stage == 'op1' or stage == 'op2':
            if task_info['stateful'] and stage == 'op2':
                self.apply_operator_stateful(task_info, unique_id, key, value)
            else: 
                self.apply_operator(task_info, unique_id, key, value)

        self.send_ack(sender_worker_id, sender_task_id, unique_id)
    
    def send_ack(self, worker_id, task_id, unique_id):
        """Send acknowledgment back to the sender."""
        host, port = self.get_node_address(worker_id)
        if host is None:
            return
        message = {
            'type': 'ACK',
            'unique_id': unique_id,
            'sender_task_id': task_id,
            'sender_worker_id': self.node_id
        }
        self.communicator.send_tcp_message(host, self.port, message)
        
    def apply_operator(self, task_info, unique_id, key, value):
        """Apply the operator and send output to next stage or output."""
        with self.lock:
            stage = task_info['stage']
            processed_ids = self.load_processed_ids(self.load_output(task_info['task_id']))
                
            if unique_id in processed_ids:
                return 

            transformed_value = self.excecute(task_info[stage], key, value)       
            if transformed_value is None:
                return
            
            key, value = parse_key_value(transformed_value)
            self.log_output(task_info, unique_id, key, value)
            if stage == 'op1':
                op2_worker_id = self.partition_key_to_worker(key, stage='op2')
                self.send_record(unique_id, key, value, op2_worker_id, task_info)
                print(f"[{self.node_id}] Task {task_info['task_id']}: Sent {key} -> {value} to worker {op2_worker_id}")
            elif stage == 'op2':
                # Final stage: output to console and append to HyDFS
                print(f"[{self.node_id}] Output: {key} -> {value}")
                # Append to HyDFS file
                self.log_to_dest(task_info, unique_id, key, value)
        
    def apply_operator_stateful(self, task_info, unique_id, key, value):
        """Aggregate key"""
        stage = task_info['stage']
        with self.lock:
            output = self.load_output(task_info['task_id'])
            processed_ids = self.load_processed_ids(output)
                
            if unique_id in processed_ids:
                return 
             
            result = self.excecute(task_info[stage], key, value)
            if result is None:
                return
            
            key, value = parse_key_value(result)
            count = self.aggegrate(output, key) + 1
            
            
            self.log_output(task_info, unique_id, key, count)
            print(f"[{self.node_id}] Output: {key} -> {count}")
            self.log_to_dest(task_info, unique_id, key, count)
        
        
            
    def excecute(self, op, key, line):
        try:
            result = subprocess.run(
                ["python3", op, key, line],  
                text=True,
                stdout=subprocess.PIPE,
                check=True
            )
            # Return the output if it exists, or None if empty
            return result.stdout.strip() if result.stdout.strip() else None
        except subprocess.CalledProcessError as e:
            print(f"Subprocess failed: {e.stderr}")
            return None
    
    
    def partition_key_to_worker(self, key, stage):
        """Partition the key to a worker based on consistent hashing."""
        hash_val = int(hashlib.sha1(key.encode()).hexdigest(), 16)
        # Get the list of nodes responsible for the stage
        nodes = self.scheduler.get_nodes_for_stage(stage)
        while nodes is None:
            time.sleep(1)
            nodes = self.scheduler.get_nodes_for_stage(stage)
        worker_id = nodes[hash_val % len(nodes)]
        return worker_id

    def aggegrate(self, output, key):
        """Count occurrences of all keys in the file."""
        count = 0
        if output:
            for line in output:
                record = json.loads(line.strip())
                if record['key'] == key:
                    count += 1
        return count
    
    def consistent_hash(self, key):
        """md5 hash function to map a key to the ring, wrap around at RING_SIZE"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (256)


        
        
        
    # callbacks for failure and rejoin ========================================================
    def handle_node_failure(self, node_id):
        """ Remove node from scheduler and reassign task to each node """
        if not self.is_introducer:
            return
        self.up_to_date.clear()
        self.wait_for_recover.clear()
        tasks = self.scheduler.remove_node(node_id)
        threading.Thread(target=self.reassign_tasks, args=(tasks,), daemon=True).start()
        
    def reassign_tasks(self, tasks):
        """reassign tasks that were assigned to failed workers"""
        for task in tasks:
            worker_id = self.scheduler.get_min_task_worker()
            task['worker_id'] = worker_id
            self.scheduler.add_task(task)
            message = {
                'type': 'ASSIGN_TASK',
                'task_info': task
            }
            self.leader_assign_task_to_worker(message)
        self.scheduler.leader_update_node_per_stage()
            
    def handle_node_join(self, node_id):
        """ handle node join callback, add node to scheduler """
        if not self.is_introducer:
            return
        self.scheduler.add_node(node_id)


def parse_key_value(input_string):
    """Parse a string in the format 'KEY: key, VALUE: value' into a key-value pair."""

    key_part = input_string.split("KEY:")[1].split(", VALUE:")[0].strip()
    value_part = input_string.split("VALUE:")[1].strip()
    
    if key_part is None: 
        key_part = ' '
    if value_part is None:
        value_part = ' '

    return key_part, value_part       
 
def read_first_line(file_path):
    """Read the first line of a file and return it as a string."""
    with open(file_path, 'r') as f:
        return f.readline().strip() 
                 
def parse_arguments():
    """Parse input arguments."""
    parser = argparse.ArgumentParser(description='RainStorm Node')
    #parser.add_argument('--node-id', required=True, help='Unique identifier for the node')
    parser.add_argument('--fd-port', type=int, default=5000, help='Port for failure detector')
    parser.add_argument('--hdfs-port', type=int, default=6000, help='Port for hdfs')
    parser.add_argument('--rainstorm-port', type=int, default=7000, help='Port for rainstorm')
    parser.add_argument('--is-introducer', action='store_true', help='Flag to indicate if this node is the introducer')
    return parser.parse_args()
    
def main():
    args = parse_arguments()
    introducer_host = "fa24-cs425-7001.cs.illinois.edu"  
    introducer_port = RAINSTORM_FD_PORT

    node = RainStorm(
        node_id=socket.gethostname().split('-')[2][2:4],
        host=socket.gethostname(),
        introducer_addr=(introducer_host, introducer_port),
        rainstorm_port=args.rainstorm_port,
        hdfs_port=args.hdfs_port,
        fd_port=RAINSTORM_FD_PORT,
        is_introducer=args.is_introducer
    )
    node.start()
    
if __name__ == "__main__":
    main()