#include "../lmdb-safe.hh"

#include <c++utilities/conversion/stringconversion.h>

#include <iostream>

using namespace std;
using namespace LMDBSafe;

struct MDBVal {
    MDBVal(unsigned int v)
        : d_v(v)
    {
        d_mdbval.mv_size = sizeof(d_v);
        d_mdbval.mv_data = &d_v;
    }
    operator const MDB_val &()
    {
        return d_mdbval;
    }
    unsigned int d_v;
    MDB_val d_mdbval;
};

int main(int argc, char **argv)
{
    auto env = getMDBEnv("./scale.lmdb", MDB_NOSUBDIR, 0600);
    auto dbi = env->openDB(std::string_view(), MDB_CREATE | MDB_INTEGERKEY);
    auto txn = env->getRWTransaction();

    unsigned int limit = 20;
    if (argc > 1)
        limit = CppUtilities::stringToNumber<unsigned int>(argv[1]);

    cout << "Counting records.. ";
    cout.flush();
    auto cursor = txn->getCursor(dbi);
    MDBOutVal key, data;
    int count = 0;
    while (!cursor.get(key, data, count ? MDB_NEXT : MDB_FIRST)) {
        auto d = data.get<unsigned long>();
        if (d == 17)
            cout << "Got 17!" << endl;
        count++;
    }
    cout << "Have " << count << "!" << endl;

    cout << "Clearing records.. ";
    cout.flush();
    mdb_drop(*txn, dbi, 0); // clear records
    cout << "Done!" << endl;

    cout << "Adding " << limit << " values  .. ";
    cout.flush();
    for (unsigned long n = 0; n < limit; ++n) {
        txn->put(dbi, n, n, MDB_APPEND);
    }
    cout << "Done!" << endl;
    cout << "Calling commit.. ";
    cout.flush();
    txn->commit();
    cout << "Done!" << endl;
}
