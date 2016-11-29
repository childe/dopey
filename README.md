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
dopey.py -c dopey.yaml -l /var/log/dopey.log --level debug

## advanced
dopey.py -c dopey.yaml --base-day -1  #以昨天做为基准日期计算

dopey.py -c dopey.yaml --base-day 1  #以明天做为基准日期计算

dopey.py -c dopey.yaml --base-day 2016-11-11  #以指定日期做为基准日期计算

dopey.py -c dopey.yaml --action-filters u,c  #只做update setting, close index.  一共有4种操作, d:delete, c:close, u:update settings,f:force merge.  不加这个参数代表全部都可以执行.

dopey.py --help

## 下面这样可以实现: 按月建的索引, 在34天后删除, 按天建的索引, 2天后删除

```
  .*-(?=\d{4}\.\d{2}$):
    - delete_indices:
        days: 34
  .*-(?=\d{4}\.\d{2}\.\d{2}$):
    - delete_indices:
        days: 2
```
