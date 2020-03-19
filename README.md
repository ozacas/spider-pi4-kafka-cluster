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

