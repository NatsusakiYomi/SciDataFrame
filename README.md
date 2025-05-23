# SciDB2Dataset
+ Environment Configuration
  + venv
      ```bash
      python -m venv venv
      venv/bin/activate
      pip install -r requirements.txt
      ```
  + Conda
       ```bash
      conda create -n myenv python=3.11
      conda activate myenv
      pip install -r requirements.txt
      ```
+ Run Example
```bash
python ./arrow_flight/server.py
```
```bash
python ./arrow_flight/example.py
```

