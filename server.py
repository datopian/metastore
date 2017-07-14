import os
import sys
import metastore

# Create application
app = metastore.create()

# Port to listen
port = os.environ.get('PORT') or 5000
if len(sys.argv) > 1:
    port = int(sys.argv[1])

# Debug mode flag
debug = True

# Run application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=debug)
