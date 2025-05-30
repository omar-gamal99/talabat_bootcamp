import os
import dagfactory

# Path to the YAML folder
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "configs")

# Loop through all .yaml files in the folder
for file_name in os.listdir(CONFIG_DIR):
    if file_name.endswith(".yaml") or file_name.endswith(".yml"):
        dag_config_path = os.path.join(CONFIG_DIR, file_name)
        dag_factory = dagfactory.DagFactory(dag_config_path)
        globals().update(dag_factory.generate_dags())
