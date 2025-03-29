from http.server import BaseHTTPRequestHandler
import monthly_data_extractor  # Import your script

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Call the main function of your script
            result = monthly_data_extractor.run()  # Ensure `run()` exists in your script
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Monthly data extraction completed.")
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(f"Error: {str(e)}".encode())

handler = Handler
