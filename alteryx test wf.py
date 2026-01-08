import subprocess
from pathlib import Path

ENGINE = r"C:\Program Files\Alteryx\bin\AlteryxEngineCmd.exe"
WORKFLOW = r"C:\Users\57948\OneDrive - Bain\Documents\Ep-DD-wideformat\New Workflow3.yxmd"

result = subprocess.run(
    [ENGINE, WORKFLOW],
    capture_output=True,
    text=True
)

print("Return code:", result.returncode)
print("STDOUT:\n", result.stdout)
print("STDERR:\n", result.stderr)

if result.returncode != 0:
    raise RuntimeError("Alteryx workflow failed")
