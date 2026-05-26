import json
import os
import threading

class StateUpdater:
    """
    Helper class for scripts to update their execution state.
    The state file path is provided by the reporter via the 
    'REPORT_STATE_FILE' environment variable.
    """
    def __init__(self):
        self.file_path = os.environ.get('REPORT_STATE_FILE')
        self._lock = threading.Lock()

    def update(self, **kwargs):
        """
        Updates the JSON state file with the provided keyword arguments.
        Example: updater.update(files_uploaded=10, last_file="image.png")
        """
        if not self.file_path:
            # If not running through the reporter, do nothing or log to console
            return

        with self._lock:
            data = {}
            if os.path.exists(self.file_path):
                try:
                    with open(self.file_path, 'r') as f:
                        data = json.load(f)
                except (json.JSONDecodeError, IOError):
                    data = {}

            data.update(kwargs)

            try:
                with open(self.file_path, 'w') as f:
                    json.dump(data, f, indent=4)
            except IOError as e:
                print(f"Error writing to state file: {e}")

    def get_state(self):
        """Returns the current state dictionary."""
        if not self.file_path or not os.path.exists(self.file_path):
            return {}
        
        with self._lock:
            try:
                with open(self.file_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}

# Singleton instance for easy usage
updater = StateUpdater()
