import threading
NODE_ID = 'worker_id' 
TASK_ID = 'task_id'
# user repsonsibility to maintain membership list (keys in state)
class Scheduler:
    def __init__(self, rainstorm):
        # leader variables
        self.state = {} # {node_id: [list of task_metadata]} track number of tasks assigned to each worker.
        self.counter = 0 # # increment for every task for uniqueness. reset for every rainstorm command
        self.num_tasks_stage = {} #store num_tasks for each stage stage: int. IMPORTANT: failures are handled seperatly
        self.rainstorm = rainstorm
        
        # worker variables
        self.nodes_per_stage = {} # stage: [nodes], leader node should rely on get_tasks instead
        self.tasks = [] # locally store assigned task on each node. 
        
        # locks
        self.lock = threading.RLock()
        
    def new_task_id(self):
        """assign new id to task"""
        self.counter += 1
        return self.counter
    
    def reset(self, state, num_tasks):
        """called every rainstorm invocation to get a new scheduler"""
        with self.lock:
            self.state = state
            self.counter = 0
            self.num_tasks_stage = {'source': num_tasks, 'op1': num_tasks, 'op2': num_tasks}
            
    def add_node(self, node_id):
        """called when new node join membership"""
        with self.lock:
            self.state[node_id] = []
            
    def remove_node(self, node_id):
        """called when node failed in fd, remove node from state and update the number of tasks that need to be assigned"""
        with self.lock:
            if node_id in self.state:
                for task in self.state[node_id]:
                    self.num_tasks_stage[task['stage']] += 1
                tasks = self.state[node_id]
                del self.state[node_id]
                return tasks
            else:
                return []
            
    def add_task(self, task_info):
        """add task only if total task does not exceed num_tasks"""
        with self.lock:
            if self.need_task(task_info['stage']):
                self.state[task_info[NODE_ID]].append(task_info)
                self.num_tasks_stage[task_info['stage']] -= 1
            
    def remove_task(self, node_id, task_id):
        """called when task sucessfully completes"""
        with self.lock:
            if node_id in self.state:
                tasks = self.state[node_id]
                self.state[node_id] = [t for t in tasks if t[TASK_ID] != task_id]
            
    def get_min_task_worker(self):
        """get worker who has the least task"""
        with self.lock:
            return min(self.state, key=lambda node_id: len(self.state[node_id]))
            
    def get_tasks(self, stage):
        """get all tasks for a specific stage"""
        tasks = []
        with self.lock:
            for _, task_infos in self.state.items():
                for task in task_infos:
                    if task['stage'] == stage:
                        tasks.append(task)
                        
            return tasks
    
    def need_task(self, stage):
        """check if we need to assign more task in a stage"""
        with self.lock:
            return self.num_tasks_stage[stage] > 0
        
    def broadcast(self, message):
        with self.lock: 
            nodes = [node_id for node_id in self.rainstorm.fd.membership_list if node_id != self.rainstorm.node_id]
            for node in nodes:
                host, _ = self.rainstorm.get_node_address(node)
                if host is not None:
                    self.rainstorm.communicator.send_tcp_message(host, self.rainstorm.port, message)
                
    def worker_update_node_per_stage(self, new_nodes_per_stage):
        with self.lock: 
            for key in new_nodes_per_stage:
                self.nodes_per_stage[key] = new_nodes_per_stage[key]
            
    def leader_update_node_per_stage(self):
        with self.lock:
            source = sorted(self.get_tasks("source"),key=lambda task: task[TASK_ID])
            op1 = sorted(self.get_tasks("op1"),key=lambda task: task[TASK_ID])
            op2 = sorted(self.get_tasks("op2"),key=lambda task: task[TASK_ID])
            message = {
                'type': 'UPDATE_NODE_PER_STAGE',
                'nodes_per_stage': {'source': [task[NODE_ID] for task in source],
                                    'op1': [task[NODE_ID] for task in op1],
                                    'op2': [task[NODE_ID] for task in op2]
                }
            }
            self.broadcast(message)
        
    def get_nodes_for_stage(self, stage):
        with self.lock:
            if stage in self.nodes_per_stage:
                return self.nodes_per_stage[stage]
            else: 
                return None
        
    def local_add_task(self, task_info):
        """called during assign task to add to self.tasks"""
        with self.lock:
            self.tasks.append(task_info)
            
    def local_get_next_task(self, stage):
        with self.lock:
            if stage == 'source':
                for task in self.tasks:
                    if task['stage'] == 'op1':
                        return task
            elif stage == 'op1':
                for task in self.tasks:
                    if task['stage'] == 'op2':
                        return task
            return None 
            
    def local_get_task_by_id(self, task_id):
        with self.lock:
            for task in self.tasks:
                if task[TASK_ID] == task_id:
                    return task
        return None
    
    def get_self_tasks(self):
        return self.tasks

        
        
    
                    
        
            
    
            
        
    
    