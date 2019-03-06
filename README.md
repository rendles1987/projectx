# build image
cd ~/code/git/projectx/docker 
docker build -t projectx .
# not providing a tag "docker build -t projectx:<tag_id> ." so that tag is 
# 'latest'. This is nice as every rebuild (after e.g. change requirements.txt)
# gets this latest tag. Project interpreter points to this tag  

# pycharm project interpreter 
- server: Docker
- Image name: projectx:latest
- Python interpreter path: python3

# pycharm debug config
script_path: ~/code/git/projectx/__init__.py
Environment variables: PYTHONUNBUFFERED=1
Python interpreter: Remote Python 3.6.7 Docker (projectx:latest))
Working directory ~/code/git/projectx
Docker container settings: -v /home/renierkramer/code/git/projectx:/opt/project
