# Pig Latin Examples

Here i'm not going to explain what is Pig and how to use it. I created some example pig scripts which flattens 
nested json files. 

### How to install Pig
You can find about it [here](http://pig.apache.org/docs/r0.17.0/start.html#Pig+Setup)

### How to quickly run and tests pig scripts
To quickly run and test pig scripts read [here](http://pig.apache.org/docs/r0.17.0/start.html#run)


### Store Pig data to DBStorage
To store Pig data to DBStorage you will need to copy library `./pig/lib/com.mysql.jdbc_5.1.5.jar` to your pig location
`cp ./pig/lib/com.mysql.jdbc_5.1.5.jar $PIG_HOME/lib/`. This way you will be able to test it locally.
 
### Pig and Hadoop log level
Copy the log4j config file to the folder where my pig scripts are located

`cp /etc/pig/conf.dist/log4j.properties log4j_WARN`
Edit `log4j_WARN` file and make sure these two lines are present
```ini
log4j.logger.org.apache.pig=WARN, A
log4j.logger.org.apache.hadoop = WARN, A
```

Run pig script and instruct it to use the custom log4j 
```bash
pig -x local -4 log4j_WARN
```



Some commands in short:
`dump` - prints the data to the output
`describe` - describes the schema of the dataset

Convert json file to one line json file 
```bash
awk -v RS= '{$1=$1}1' input.json
```

Entirely switch off logs
```bash
cat > log.conf
```
Paste there `log4j.rootLogger=fatal` and hit `CTRL+D`

Pig creates dynamic the name of log file. You can specify log file name with command:
```bash
pig -x local -4 log.conf -l pig.log
# and then view logs this way
tail -f pig.log
```


On AWS you can run EMR Cluster with apache Pig and Hadoop ready to use. 
```bash
aws emr create-cluster --applications Name=Ganglia Name=Hadoop Name=Hive Name=Hue Name=Mahout Name=Pig Name=Tez \
--ec2-attributes '{"KeyName":"id_rsa","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-6e9a8c0a",\
"EmrManagedSlaveSecurityGroup":"sg-837417fb","EmrManagedMasterSecurityGroup":"sg-c27b18ba"}' --service-role \
EMR_DefaultRole --enable-debugging --release-label emr-5.11.1 --log-uri 's3n://[YOUR_BUCKET]/' --name 'My EMR' \
--instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge",\
"Name":"Master Instance Group"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region eu-west-1
```