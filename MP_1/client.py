import socket
import argparse
import threading
import json
import os
import time

PORT = 9999
SERVERS = [('fa24-cs425-7001.cs.illinois.edu', PORT), 
           ('fa24-cs425-7002.cs.illinois.edu', PORT), 
           ('fa24-cs425-7003.cs.illinois.edu', PORT), 
           ('fa24-cs425-7004.cs.illinois.edu', PORT),
           ('fa24-cs425-7005.cs.illinois.edu', PORT),
           ('fa24-cs425-7006.cs.illinois.edu', PORT),
           ('fa24-cs425-7007.cs.illinois.edu', PORT),
           ('fa24-cs425-7008.cs.illinois.edu', PORT),
           ('fa24-cs425-7009.cs.illinois.edu', PORT),
           ('fa24-cs425-7010.cs.illinois.edu', PORT)]

results = [''] * 10  # stdout result
times = [None] * 10  # timer


def send_grep_command(host, port, grep_command):
    '''Worker thread to send grep command to a server, waits for response and update the result list and output log files'''
    try:
        # Connect to server
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((host, port))

        # Send the grep command and start timer ========================================
        start = time.time()
        client_socket.sendall(grep_command.encode())

        # Receive the response from the server and end timer
        response = b""
        while True:
            part = client_socket.recv(4096)
            if not part:
                break
            response += part
        end = time.time()
        # ==============================================================================
        data = json.loads(response.decode())  # Parse the JSON response

        result_count = data["result_count"]
        result_line = data["result_line"]

        # Index is computed based on vm number
        index = int(host[13:15]) - 1
        results[index] = result_count
        times[index] = (start, end)

        # Write grep content to log
        out_file = f"output/output.{host[13:15]}.log"
        os.makedirs(os.path.dirname(out_file), exist_ok=True)  # Ensure the directory exists

        with open(out_file, "w") as f:
            f.write(result_line)

        client_socket.close()
    except Exception as _:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send a grep command to SERVERS')
    
    parser.add_argument('--show-times', action='store_true', help='Display query time')
    parser.add_argument('flags', nargs='*', help='Optional flags to forward')
    parser.add_argument('pattern', help='Pattern to search for using grep')

    args = parser.parse_args()
    flags = ' '.join(args.flags) if args.flags else ''
    grep_command = f"grep {flags} '{args.pattern}'"
    grep_command = ' '.join(grep_command.split())
    
    # Run grep on each machine
    threads = []

    for server_host, server_port in SERVERS:
        thread = threading.Thread(target=send_grep_command, args=(server_host, server_port, grep_command))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Line counts to stdout
    results = [s for s in results if s] # filter out empty strings (machines that are not used)
    for response in results:
        print(response)
        
    total_lines = sum(int(response.split(':')[1]) for response in results)
    print(f"Total Line Count: {total_lines}")
    
    # Calculate query time
    times = [t for t in times if t is not None]
    if times:
        earliest_start = min(t[0] for t in times)
        latest_end = max(t[1] for t in times)
        
    if args.show_times: 
        print(f"Query time: {latest_end - earliest_start}")
