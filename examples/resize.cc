#include "../lmdb-safe.hh"

#include <cstring>

using namespace std;
using namespace LMDBSafe;

template <typename First, typename Second>
struct Pair {
    First first;
    Second second;
};

int main(int argc, char**)
{
  auto env = getMDBEnv("resize",  MDB_NOSUBDIR | MDB_NOSYNC, 0600);
  auto main = env->openDB("ahu",  MDB_CREATE );

  MDBInVal key("counter");

  auto rwtxn = env->getRWTransaction();
  rwtxn->put(main, "counter", "1234");
  rwtxn->put(main, MDBInVal::fromStruct(Pair<int, int>{12,13}), "hoi dan 12,13");

  rwtxn->put(main, MDBInVal::fromStruct(Pair<int, int>{14,15}),
             MDBInVal::fromStruct(Pair<int, int>{20,23}));

  
  MDBOutVal out;
  if(!rwtxn->get(main, MDBInVal::fromStruct(Pair<int, int>{12,13}), out))
    cout << "Got: " << out.get<string_view>() << endl;
  else
    cout << "Got nothing!1"<<endl;
  
  if(!rwtxn->get(main, MDBInVal::fromStruct(Pair<int, int>{14,15}), out)) {
    auto res = out.get_struct<Pair<int,int>>();
    cout << "Got: " << res.first<<", "<<res.second << endl;
  }
  else
    cout << "Got nothing!1"<<endl;

  rwtxn->put(main, 12.12, 7.3);
  if(!rwtxn->get(main, 12.12, out)) {
    cout<<"Got: "<< out.get<double>() <<endl;
  }
  else
    cout << "Got nothing!1"<<endl;

  
  rwtxn->commit();
  return 0;
  
  if(argc==1) {
    for(;;) {
      auto rotxn = env->getROTransaction();
      MDBOutVal data;
      if(!rotxn->get(main, key, data))  {
        cout<<"Counter is "<<data.get<unsigned int>() << endl;
        cout <<data.get<string>() << endl;
        cout<<data.get<string_view>() << endl;

        struct Bert
        {
          uint16_t x,y;
        };
        auto b = data.get_struct<Bert>();
        cout<<b.x<<" "<<b.y<<endl;
        cout<<data.get<unsigned long>() << endl;
      }
      else
        cout<<"Didn't find it"<<endl;
      exit(1);
    }
  }
  else {
    size_t size = 1ULL*4096*244140ULL;
    for(unsigned int n=0;;++n) {
      if(!(n%16384)) {
        size += 16384;
        if(int rc=mdb_env_set_mapsize(*env.get(), size))
          throw std::runtime_error("Resizing: "+string(mdb_strerror(rc)));
        cout<<"Did resize"<<endl;
      }
      auto txn = env->getRWTransaction();
      txn->put(main, key, MDBInVal(n));
      for(unsigned int k=0; k < 100; ++k)
        txn->put(main, MDBInVal(n+1000*k), MDBInVal(n+1000*k));
      txn->commit();
    }
  }
}
