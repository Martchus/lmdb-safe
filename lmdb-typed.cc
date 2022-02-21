#include "lmdb-typed.hh"

namespace LMDBSafe {

IDType MDBGetMaxID(MDBRWTransaction &txn, MDBDbi &dbi)
{
    auto cursor = txn->getRWCursor(dbi);
    MDBOutVal maxidval, maxcontent;
    auto maxid = IDType(0);
    if (!cursor.get(maxidval, maxcontent, MDB_LAST)) {
        maxid = maxidval.get<IDType>();
    }
    return maxid;
}

} // namespace LMDBSafe
