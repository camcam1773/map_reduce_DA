HOW TO RUN?
-----------
You can start the task by using the module "yolo".
yolo:cr/0 starts the crawler and saves the output to "web.dat" file.
yolo:start/0 initiates distributed page rank module provided you've set up the connection between nodes first.
During testing, I've started up nodes on the same machine with localhost IP addresses(127.0.0.x).

Before starting the main process, the command "dets:open_file(web,[{file,"web.dat"}])."  needs to be executed at each node.
Otherwise dets will throw an exception and the every other node except the main will crash.
Alternatively, dist mapper can replaced with the function below.
However, this will result in reduced performance since main process will be handling the most resource intensive task on its own.

dist_mapper(Parent,Map,R,Split) ->
  Mapped = [{erlang:phash2(K2,R),{K2,V2}} || {K,V} <- Split, {K2,V2} <- Map (K,V) ],
  fun() ->
    Parent ! {self(),group(lists:sort(Mapped))}
  end.

Also, if you experience "badfun" exceptions on the nodes, restarting the nodes will most likely solve the issue.

HOW DOES IT WORK?
-----------------
The Distributed Map-Reduce is mostly the same as the parallel one. 
The main difference is instead of spawning processes on same node, they are distributed among the available nodes as one per node. 
Additionally, if for some reason a node does not finish its assigned task in time, that task will be assigned to another node. 
The default timeout value is 5 seconds. Lower timeout values will most likely result in same chunk of data being processed twice or more.
The default number of chunks is 16.

PERFORMANCE DIFFERENCE
----------------------
The parallel map-reduce implementation takes about 75 seconds to complete, while distributed implementation with 3 nodes finishes in 25 seconds.
