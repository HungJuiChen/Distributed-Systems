import json
import socket
import subprocess
import argparse

def start_server(host='0.0.0.0', port=9999, machine='01', fname = 'machine'):
    """Start server on vm with number [machine] and grep on file [fname], upon recieving client command. Return stdout line count and log grep content to client as a json"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Server started on {host}:{port}")
    print("Version: " + 'Unit Test' if fname == 'unittest' else 'DEMO')

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr}")

        command = client_socket.recv(1024).decode()
        print(f"Received command: {command}")

        if "grep" in command:
            try:
                # Run grep on machine, use unit test file or machine log based on argument
                command_temp = " ".join(str(x) for x in command.split()[1:])
                result_count = subprocess.check_output(f"grep -c {command_temp} {fname}.{machine}.log", shell=True).decode().strip()
                result_line = subprocess.check_output(f"{command} {fname}.{machine}.log", shell=True).decode()

                print(result_count)

                # Stdout and log file results
                data = {"result_count": result_count,
                        "result_line": result_line}
                client_socket.sendall(json.dumps(data).encode())

            except Exception as _:
                # check_output problematic for grep with no match, handle edge case
                data = {"result_count": f"{fname}.{machine}.log: 0",
                        "result_line": ""}
                client_socket.sendall(json.dumps(data).encode())
        client_socket.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start a grep server')
    parser.add_argument('machine', help='Current machine number')
    parser.add_argument('--is-test', action='store_true', help='change machine.i.log to unittest.i.log')

    args = parser.parse_args()

    start_server(machine=args.machine, fname='unittest' if args.is_test else 'machine')

