-module(yolo).
-compile(export_all).

cr() ->
  dets:open_file(web,[{file,"web.dat"}]),
  inets:start(),
  dets:insert(web, crawl:crawl("http://www.hh.se",3)),
  inets:stop(),
  dets:close(web).

start() ->
  %erlang:set_cookie(node(),'yim'),
  %net_adm:ping('n1@127.0.0.1'),
  %net_adm:ping('n2@127.0.0.2'),
  io:format("Nodes: ~p.\n", [[node()|nodes()]]),
  page_rank:page_rank_dist().
