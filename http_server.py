import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse


def make_handler(store):
    class HttpRequestHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_GET(self):
            path = urlparse(self.path).path
            if path == '/keys/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                message = page_template('Keys', store.keys())
                self.wfile.write(bytes(message, "utf8"))
                return
            elif path == '/length/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                message = page_template('Length', store.length())
                self.wfile.write(bytes(message, "utf8"))
                return
            elif path == '/get/':
                print(parse_qs(urlparse(self.path).query))
                key = parse_qs(urlparse(self.path).query).get('key', [None])[0]
                if key is not None:
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    message = page_template(key, store.get(key).value)
                    self.wfile.write(bytes(message, "utf8"))
                    return
            self.send_error(404, 'Not found')
            return
    return HttpRequestHandler


def page_template(title, content=''):
    return """
        <html>
            <head>
            </head>
            <body>
                <h1>{title}</h1>
                {content}
            </body>
        </html>""".format(title=title, content=content)


def start_server(store, port, addr=''):
    server_address = (addr, port)
    httpd = ThreadingHTTPServer(server_address, make_handler(store))
    t = threading.Thread(target=httpd.serve_forever)
    t.daemon = True
    t.start()
