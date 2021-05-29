from http.server import HTTPServer, CGIHTTPRequestHandler
from http import HTTPStatus
import sys
import threading
import os

# This application is docker-hadoop agent which runs script when it receives request.
# It is written to run script remotely, hence there is a security leak
# The reason that why it doesn't concern this security leak is because, 
# docker hadoop is NOT for production environment but for test on local, study purpose

class RequestHandler(CGIHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(HTTPStatus.OK, "Agent is running")
        self.flush_headers()

    def do_POST(self) -> None:
        if "/exit" == self.path:
            self.send_response(HTTPStatus.OK, "Agent is terminating")
            self.flush_headers()
            threading.Thread(target=self.server.shutdown).start()
        else:
            return_code = os.system(self.path + " &") # run in background
            status = HTTPStatus.OK if return_code == 0 else HTTPStatus.INTERNAL_SERVER_ERROR
            msg = ("Script " + self.path + " has been ran successfully") if return_code == 0 else ("Script " + self.path + " failed to run")
            self.send_response(status, msg)
            self.flush_headers()
            # super().do_POST()

    def translate_path(self, path) -> str:
        return path


def main():
    argv = sys.argv
    script_path = sys.argv[1] if len(argv) > 1 and argv[1] else "/scripts"
    port = int(sys.argv[2]) if len(argv) > 2 and argv[2] else 3080
    server_address = ('', port)
    RequestHandler.cgi_directories = [script_path]

    server = HTTPServer(server_address, RequestHandler)
    RequestHandler.server = server

    try:
        server.serve_forever()
    except:
        server.socket.close()


if __name__ == '__main__':
    main()
