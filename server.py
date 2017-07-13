import os
import sys
import metastore

# Create application
app = metastore.create()

# Port to listen
port = int(metastore.config.PORT)
if len(sys.argv) > 1:
    port = int(sys.argv[1])

# Debug mode flag
debug = metastore.config.DEBUG

# Run application
app.run(host='0.0.0.0', port=port, debug=debug)
