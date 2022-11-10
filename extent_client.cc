// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int rpc_stat = cl->call(extent_protocol::create, type, id);
  ret = (rpc_stat == 0) ? ret : extent_protocol::RPCERR;
  if (rpc_stat != 0) printf("RPC Error: %d\n", rpc_stat);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int rpc_stat = cl->call(extent_protocol::get, eid, buf);
  ret = (rpc_stat == 0) ? ret : extent_protocol::RPCERR;
  if (rpc_stat != 0) printf("RPC Error: %d\n", rpc_stat);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int rpc_stat = cl->call(extent_protocol::getattr, eid, attr);
  ret = (rpc_stat == 0) ? ret : extent_protocol::RPCERR;
  if (rpc_stat != 0) printf("RPC Error: %d\n", rpc_stat);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int rpc_stat = cl->call(extent_protocol::put, eid, buf, ret);
  ret = (rpc_stat == 0) ? ret : extent_protocol::RPCERR;
  if (rpc_stat != 0) printf("RPC Error: %d\n", rpc_stat);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int rpc_stat = cl->call(extent_protocol::remove, eid, ret);
  ret = (rpc_stat == 0) ? ret : extent_protocol::RPCERR;
  if (rpc_stat != 0) printf("RPC Error: %d\n", rpc_stat);
  return ret;
}

extent_protocol::status
extent_client::beginTX()
{
  extent_protocol::status ret = extent_protocol::OK;
  int rpc_stat = cl->call(extent_protocol::begin, 0, ret);
  ret = (rpc_stat == 0) ? ret : extent_protocol::RPCERR;
  if (rpc_stat != 0) printf("RPC Error: %d\n", rpc_stat);
  return ret;
}

extent_protocol::status
extent_client::commitTX()
{
  extent_protocol::status ret = extent_protocol::OK;
  int rpc_stat = cl->call(extent_protocol::commit, 0, ret);
  ret = (rpc_stat == 0) ? ret : extent_protocol::RPCERR;
  if (rpc_stat != 0) printf("RPC Error: %d\n", rpc_stat);
  return ret;
}


