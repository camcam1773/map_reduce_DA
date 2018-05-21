-module(yolo).
-compile(export_all).

cr() ->
  dets:open_file(web,[{file,"web.dat"}]),
  inets:start(),
  dets:insert(web, crawl:crawl("http://www.hh.se",3)),
  inets:stop(),
  dets:close(web).

%erl -name n1@127.0.0.1 -setcookie "yim"

hello() ->
  io:fwrite("Hello World!\n").
  %erlang:set_cookie(node(),'yim'),
  %net_adm:ping('n1@127.0.0.1'),
  %net_adm:ping('n2@127.0.0.2'),
  %io:format("~p.\n", [nodes()]),
  page_rank:page_rank_dist().

hello2() ->
  erlang:set_cookie(node(),'yim'),
  %pool:start(mel,[]),
  %pool:attach('n1@127.0.0.1'),
  %pool:attach('n2@127.0.0.2'),
  %io:format("~p.\n",[pool:get_nodes()]),
  %pool:pspawn(page_rank,page_rank_par,[]).
  %pool:attach('n3@127.0.0.3').
  page_rank:page_rank_dist().