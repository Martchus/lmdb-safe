#include "../lmdb-boost-serialization.hh"
#include "../lmdb-typed.hh"

#define CATCH_CONFIG_MAIN // for catch2 < 3
#ifdef CATCH2_SPLIT_HEADERS
#include <catch2/catch_all.hpp>
#else
#include <catch2/catch.hpp>
#endif

#include <c++utilities/application/global.h>

#include <ctime>

#include <unistd.h>

using namespace std;
using namespace LMDBSafe;

struct Member {
    std::string firstName;
    std::string lastName;
    std::time_t enrolled;
};

template <class Archive> void serialize(Archive &ar, Member &g, const unsigned int version)
{
    CPP_UTILITIES_UNUSED(version)
    ar & g.firstName & g.lastName & g.enrolled;
}

TEST_CASE("Basic typed tests", "[basictyped]")
{
    unlink("./tests-typed");
    using tmembers_t = TypedDBI<Member, index_on<Member, string, &Member::firstName>, index_on<Member, string, &Member::lastName>,
        index_on<Member, time_t, &Member::enrolled>>;
    auto tmembers = tmembers_t(getMDBEnv("./tests-typed.lmdb", MDB_CREATE | MDB_NOSUBDIR, 0600), "members");

    // insert three rows
    auto txn = tmembers.getRWTransaction();
    auto m = Member{ "bert", "hubert", std::time_t() };
    txn.put(m);
    m.firstName = "bertus";
    m.lastName = "testperson";
    m.enrolled = time(0);
    txn.put(m);
    m.firstName = "other";
    txn.put(m);

    // find and read back inserted rows via ID
    auto out = Member{};
    REQUIRE(txn.get(1, out));
    REQUIRE(out.firstName == "bert");
    REQUIRE(out.lastName == "hubert");
    REQUIRE(out.enrolled == std::time_t());
    REQUIRE(txn.get(2, out));
    REQUIRE(out.firstName == "bertus");
    REQUIRE(out.lastName == "testperson");
    REQUIRE(out.enrolled == m.enrolled);
    REQUIRE(txn.get(3, out));
    REQUIRE(out.firstName == "other");
    REQUIRE(out.lastName == "testperson");
    REQUIRE(out.enrolled == m.enrolled);
    REQUIRE(!txn.get(4, out));
    REQUIRE(txn.size<0>() == txn.size());
    REQUIRE(txn.size<1>() == txn.size());
    REQUIRE(txn.size<2>() == txn.size());

    // find rows by prefix via index
    auto range = txn.prefix_range<0>("bert");
    auto names = std::vector<std::string>();
    for (auto &iter = range.first; iter != range.second; ++iter) {
        names.push_back(iter->firstName);
    }
    REQUIRE(names == std::vector<std::string>{ "bert", "bertus" });
    auto range2 = txn.prefix_range<0>("nosuchperson");
    REQUIRE(range2.first != range2.second);

    // override existing row
    m.firstName = "another";
    m.enrolled += 1;
    txn.put(m, 3);
    REQUIRE(txn.get(3, out));
    REQUIRE(out.firstName == "another");
    REQUIRE(out.lastName == "testperson");
    REQUIRE(out.enrolled == m.enrolled);
    REQUIRE(txn.size<0>() == txn.size());
    REQUIRE(txn.size<1>() == txn.size());
    REQUIRE(txn.size<2>() == txn.size());

    // row only retrievable via index with updated field value
    REQUIRE(!txn.get<0>("other", out));
    out.firstName.clear();
    out.lastName.clear();
    REQUIRE(txn.get<0>("another", out));
    REQUIRE(out.firstName == "another");
    REQUIRE(out.lastName == "testperson");

    // modify existing row via modify function
    txn.modify(3, [](Member &member) { member.firstName = "yetanother"; });
    REQUIRE(!txn.get<0>("another", out));
    REQUIRE(txn.get<0>("yetanother", out));
    REQUIRE(out.firstName == "yetanother");
    REQUIRE(out.lastName == "testperson");
    REQUIRE(out.enrolled == m.enrolled);

    // rebuild the database
    txn.rebuild([](IDType, Member *member) { return member->firstName != "bertus"; });
    REQUIRE(txn.size() == 2);
    REQUIRE(txn.size<0>() == txn.size());
    REQUIRE(!txn.get<0>("bertus", out));

    txn.abort();
}
