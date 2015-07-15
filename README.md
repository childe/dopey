# dopey
ES索引的维护脚本, 每天close delete reallocate optimize索引

# 依赖
click==3.3
elasticsearch==1.3.0
elasticsearch-curator==3.2.1
PyYAML==3.11
urllib3==1.10
wheel==0.24.0

# 使用
dopey.py -c dopey.yaml -l /var/log/dopey.log
