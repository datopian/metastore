import os
import yaml


# File

filepath = os.path.join(os.path.dirname(__file__), '..', 'config.yml')
for key, value in yaml.load(open(filepath)).items():
    locals()[key.upper()] = value

# Runtime

# ...

# Environment

for key, value in os.environ.items():
    locals()[key.upper()] = value
