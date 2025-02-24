# Luigi Demo

To use the Luigi demo, you must create a virtual environment and install the prerequisites.
Run this command from the room 

```
python -m venv .demo
source .demo/bin/activate
pip install --upgrade setuptools
pip install -r requirements.txt
```

The file existence task is used by each subtask:

```python pipeline.py```

We can start start the task visualizer (requires opening port 8082).

```
luigid --background --logdir tmp
```

Go to http://hostname:8082
