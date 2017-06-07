# Chord-DHT
A program-emulator for the Chord algorithm for a peer-to-peer distributed hash table

The emulator is implemented to use chain replications (k=replication factor) with the desired number of replicas,and the nodes can handle the insertation and deletion of <key,value> pairs and also the query of them,in the nodes that they are stored.Also it is possible to Join or Depart Nodes from the Chord Ring.

The responsible node for storing a <key,value> element is based on map reduce and calculated with a hash function.

Each Node in this Distributed emulator is a thread and communication is established through sockets.

Requests.txt is an example document with sequential commands that can be inserted directly to the program.
