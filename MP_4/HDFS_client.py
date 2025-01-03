from HDFS import Communicator

class HDFSClient:
    def __init__(self, hdfs_host, hdfs_port):
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.communicator = Communicator()

    def request(self, request):
        return self.communicator.send_and_receive_tcp_message(self.hdfs_host, self.hdfs_port, request, timeout=1000)

    def create(self, localfile, hdfsfile, localfilename_is_data=False):
        request = {
            'type': 'CREATE',
            'localfile': localfile,
            'hdfsfile': hdfsfile,
            'use_data': localfilename_is_data
        }
        return self.request(request)

    def get(self, hdfsfile, localfile):
        request = {
            'type': 'GET',
            'hdfsfile': hdfsfile,
            'localfile': localfile
        }
        return self.request(request)

    def append(self, localfile, hdfsfile, localfilename_is_data=False):
        request = {
            'type': 'APPEND',
            'localfile': localfile,
            'hdfsfile': hdfsfile,
            'use_data': localfilename_is_data
        }
        return self.request(request)

    def merge(self, hdfsfile):
        request = {
            'type': 'MERGE',
            'hdfsfile': hdfsfile
        }
        return self.request(request)

    def read_file(self, filename):
        request = {
            'type': 'READ_FILE',
            'hdfsfile': filename
        }
        response = self.request(request)
        if response and 'data' in response:
            return response['data']
        return None
