import argparse
import json
import boto.vpc


def getInstances():
    #bound_regional_ec2_conn = boto.ec2.connect_to_region(setup['region'])
    a= boto.ec2.connect_to_region(setup['region'], aws_access_key_id=setup['aws_access_key_id'],aws_secret_access_key=setup['aws_secret_access_key']).get_all_instances()
    instanceList=[]
    for instance in a:
        instanceList.append(instance.instances)
    return instanceList



with open('setup.json') as data_file:
    setup = json.load(data_file)

def main():
    parser = argparse.ArgumentParser(description='Spin your own cluster with Spark, Kafka, Hadoop, Mesos, Yarn, Hbase')
    parser.add_argument('--spark', help='Spins a SPARK cluster', action='store_true')
    parser.add_argument('--kafka', help='Spins a Kafka cluster', action='store_true')

    parser.add_argument('--hbase', help='Spins a hbase cluster', action='store_true')

    parser.add_argument('--mesos', help='Spins a mesos cluster', action='store_true')
    parser.add_argument('--zoo', help='Spins a zookeeper cluster', action='store_true')
    parser.add_argument('--yarn', help='Spins a yarn cluster', action='store_true')
    parser.add_argument('--add-instance', help='deploy services on existing cluster', action='store_true')
    parser.add_argument('--print-configuration',help="instance ID's, tags, services running", action='store_true')
    parser.add_argument('--start-service-on-instance',help='provide instance id and service name', action='store_true')
    args = parser.parse_args()

    print(args)

    if(args.spark):
        spinSparkCluster()
    if(args.zoo):
        spinZooCluster()
    if(args.kafka):
        spinKafkaCluster()
    if(args.hbase):
        spinHbaseCluster()
    if(args.mesos):
        spinMesosCluster()
    if(args.yarn):
        spinYarnCluster()
    if(args.print_configuration):
        printConfiguration()
    if(args.start_service_on_instance):
        startServiceOnInstance()
    if(args.add_instance):
        service=raw_input("Service to start on the machine[ Spark(S) / Mesos(M) / Yarn(Y) / Kafka(K) / Hbase(H)]:")
        for instance in setup['instance_types']:
            print(str(i + 1) + ". " + instance + " " + setup['instance_cores'][i] + " " + setup['instance_memory'][i])
            i = i + 1
        instanceType = int(raw_input("Instance Type:[1-" + str(i) + "]"))
        instanceId=startInstance(instanceType)
        startServiceOnInstance(instanceId,service)



#security_groups, cluster_size, mode
def spinSparkCluster():
    print("Provide information about the spark cluster")
    mode = raw_input("Mode - Standalone / Mesos / Yarn (S / m / y):")
    if (mode == None):
        mode = 'm'
    if (not mode in ('S', 's', 'm', 'M', 'y', 'Y')):
        print("Invalid Mode")
    clustersize = int(raw_input("Cluster Size [3] :"))
    if(clustersize==None):
        clustersize=3
    if(not isinstance(clustersize,int)):
        print("Cluster Size not a number")

    i=0
    for instance in setup['instance_types']:
        print(str(i+1)+". "+instance+" "+str(setup['instance_cores'][i]) +" cores "+str(setup['instance_memory'][i])+" GB ")
        i=i+1
    instanceType = int(raw_input("Instance Type:[1-"+str(i)+"]"))

    for x in range(0,clustersize):
        instanceId=startInstance(instanceType)
        startSparkService(instanceId)



# security_groups, cluster_size, storage(s3/HDFS)
def spinKafkaCluster():
    spinHdfsCluster()
    print("Provide information about the Kafka cluster")
    clustersize = int(raw_input("Cluster Size [3] :"))
    if(clustersize==None):
        clustersize=3
    if(not isinstance(clustersize,int)):
        print("Cluster Size not a number")

    i=0
    for instance in setup['instance_types']:
        print(str(i+1)+". "+instance+" "+setup['instance_cores'][i] +" "+setup['instance_memory'][i])
        i=i+1
    instanceType = int(raw_input("Instance Type:[1-"+str(i)+"]"))

    for x in range(0,clustersize):
        instanceId=startInstance()
        startSparkService(instanceId)

# security_groups, cluster_size, storage
def spinHdfsCluster():
    print("Provide information about the HDFS cluster")
    clustersize = int(raw_input("Cluster Size [3] :"))
    if(clustersize==None):
        clustersize=3
    if(not isinstance(clustersize,int)):
        print("Cluster Size not a number")

    i=0
    for instance in setup['instance_types']:
        print(str(i+1)+". "+instance+" "+setup['instance_cores'][i] +" "+setup['instance_memory'][i])
        i=i+1
    instanceType = int(raw_input("Instance Type:[1-"+str(i)+"]"))

    for x in range(0,clustersize):
        instanceId=startInstance()
        startSparkService(instanceId)

def spinMesosCluster():
    print("Provide information about the Mesos cluster")
    clustersize = int(raw_input("Cluster Size [3] :"))
    if(clustersize==None):
        clustersize=3
    if(not isinstance(clustersize,int)):
        print("Cluster Size not a number")

    i=0
    for instance in setup['instance_types']:
        print(str(i+1)+". "+instance+" "+setup['instance_cores'][i] +" "+setup['instance_memory'][i])
        i=i+1
    instanceType = int(raw_input("Instance Type:[1-"+str(i)+"]"))

    for x in range(0,clustersize):
        instanceId=startInstance()
        startSparkService(instanceId)

def spinHbaseCluster():
    print("spin")

def spinYarnCluster():
    print("spin")






def printConfiguration():
    instances=getInstances()
    for instance in instances:
        print instance

def startServiceOnInstance(instanceId,service):
    if(service=='S'):
        startSparkService(instanceId)
    if(service=='M'):
        startMesosService(instanceId)
    if(service=='H'):
        startHbaseService(instanceId)
    if(service=='Y'):
        startYarnService(instanceId)
    if(service=='K'):
        startKafkaService(instanceId)

def startInstance(instanceType):
    print("add instance")


if __name__ == '__main__':
    main()


def spinZooCluster():
    print("Spin CLuster Zoo")


def startSparkService(instanceId):
    print("spin")

def startKafkaService(instanceId):
    print("spin")

def startHdfsService(instanceId):
    print("spin")

def startMesosService(instanceId):
    print("spin")

def startHbaseService(instanceId):
    print("spin")

# Start service and add tag to instance
def startYarnService(instanceId):
    print("spin")

    # this is what would make an ideal configuration but i dont want to be binding to it
#==================================================================================
#machine1    ||   machine2   || machine3   ||machine4(8 core)   ||  machine5     ||
#==================================================================================
#spark-slave ||   hbase      ||kafka       || spark-master+slave||               ||
#hdfs-data   ||   hdfs-data  || hdfs-data  ||  hdfs-name+data   ||               ||
#mesos-agent ||   mesos-agent|| mesos-agent|| mesos-master+agent||               ||
#            ||              ||            ||                   ||  zookepeer    ||
#yarn-slave  ||   yarn-slave || yarn-slave || yarn-master+slave ||               ||
#==================================================================================




#Services List
# 1. Spark-Master
# 2. Spark-Slave
# 3. Mesos-Master
# 4. Mesos-Agent
# 5. Kafka
# 6. Zookeeper
# 7. Hbase
# 8. Yarn-Master
# 9. Yarn-Slave


#Pending items
#1. Check if a cluster of that type is already running
#2. Spin up/Check dependencies
#3. Startiing up instances
#4. Starting services actually with correct mode
#5. Using the correct option setup - checking if i am asking the correct options
#6. Add an UI