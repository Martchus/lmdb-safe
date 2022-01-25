#pragma once
// Example for plugging a (de)serialization implementation using the binary
// (de)serializer provided by https://github.com/Martchus/reflective-rapidjson.

#include "lmdb-safe.hh"

#include <reflective-rapidjson/lib/binary/reflector.h>

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
  auto deserializer = ReflectiveRapidJSON::BinaryReflector::BinarySerializer(&stream);
  deserializer.write(t);
  return ret;
}

template<typename T>
void serFromString(string_view str, T& ret)
{
  auto source = boost::iostreams::array_source(str.data(), str.size());
  auto stream = boost::iostreams::stream<boost::iostreams::array_source>(source);
  auto serializer = ReflectiveRapidJSON::BinaryReflector::BinaryDeserializer(&stream);
  ret = T();
  serializer.read(ret);
}

}
