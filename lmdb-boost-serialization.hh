#pragma once

#include "./lmdb-safe.hh"

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#include <boost/serialization/vector.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/utility.hpp>

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/stream_buffer.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

namespace LMDBSafe {

template<typename T>
std::string serToString(const T& t)
{
  auto ret = std::string();
  auto inserter = boost::iostreams::back_insert_device<std::string>(ret);
  auto stream = boost::iostreams::stream<boost::iostreams::back_insert_device<std::string>>(inserter);
  auto oa = boost::archive::binary_oarchive(stream, boost::archive::no_header | boost::archive::no_codecvt);
  oa << t;
  return ret;
}

template<typename T>
void serFromString(string_view str, T& ret)
{
  auto source = boost::iostreams::array_source(str.data(), str.size());
  auto stream = boost::iostreams::stream<boost::iostreams::array_source>(source);
  auto ia = boost::archive::binary_iarchive(stream, boost::archive::no_header|boost::archive::no_codecvt);
  ia >> ret;
}

}
