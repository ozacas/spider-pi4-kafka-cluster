Implement an enterprise system bus architecture on a Raspberry Pi4 cluster (with Manjaro) linux, coupled with a 
web spider which crawls for web artefacts recording them in the bus as well as MongoDB for longer term storage.

Data mining is then performed on the artefacts. 

System requirements
-------------------


* ansible for provisioning the cluster (hostnames should be pi1..pi5 with static IP addresses from 192.168.1.80 .. 84)
* manjaro linux for each pi
* Apache Kafka to implement the ESB architecture
* Python3
* Python package requirements as documented in src/requirements.txt

Basic Architecture
------------------

Each component program connects to the bus, typically to 1) read a record 2) do some computation and 3) write a record. Thats all they do.
No program has awareness of other programs on the bus, nor where the data comes from.

The bus is configured to have a 30 day data retention time, after which time records are lost permanently. Applications must persist to MongoDB or some
other data sink as required.

The ESB keeps all ongoing spider state (pending URLs, cache state). Applications are started and stopped on demand or as requirements permit.

Basic Administration
--------------------

Let ansible do the work.

### Provisioning the cluster (once ssh and linux setup)

> ansible-playbook -i cluster-inventory install-cluster.yml

Will take a long time (for system updates) and trigger a reboot to get system services incl. kafka/zookeeper sync'ed.

### Provisioning required software across the cluster (only required for each release to deploy, no reboots)

> ansible-playbook -i cluster-inventory install-software.yml

### Running core system

> vi group_vars/all # edit to control what run-system does and creds etc...
> ansible-playbook -i cluster-inventory run-system.yml

### When run done...

> ansible-playbook -i cluster-inventory stop-system.yml
> ansible-playbook -i cluster-inventory shutdown-cluster.yml

I use kafdrop (installed on the ansible control workstation) and kafka-manager (now known as CMAK) to admin the kafka cluster and keep track of things.
Kafkaspider saves to pi2:/data/kafkaspider16 to avoid pounding mongo and slowing down the crawl. etl_upload_artefacts pushes a batch at the 
end of each day to Mongo for the rest of the pipeline to read.
