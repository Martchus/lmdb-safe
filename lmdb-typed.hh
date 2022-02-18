#pragma once

#include "./lmdb-safe.hh"

#include <c++utilities/conversion/binaryconversion.h>
#include <c++utilities/conversion/conversionexception.h>
#include <c++utilities/conversion/stringbuilder.h>

namespace LMDBSafe {

/* 
   Open issues:

   What is an error? What is an exception?
   could id=0 be magic? ('no such id')
     yes
   Perhaps use the separate index concept from multi_index
   perhaps get eiter to be of same type so for(auto& a : x) works
     make it more value "like" with unique_ptr
*/

/** Return the highest ID used in a database. Returns 0 for an empty DB.
    This makes us start everything at ID=1, which might make it possible to 
    treat id 0 as special
*/
LMDB_SAFE_EXPORT unsigned int MDBGetMaxID(MDBRWTransaction &txn, MDBDbi &dbi);

/** This is the serialization interface.
    You need to define your these functions for the types you'd like to store.
*/
template <typename T> std::string serToString(const T &t);

template <typename T> void serFromString(string_view str, T &ret);

// define some "shortcuts" (to avoid full-blown serialization stuff for trivial cases)
template <> inline std::string serToString(const std::string_view &t)
{
    return std::string(t);
}

template <> inline std::string serToString(const std::string &t)
{
    return t;
}

template <> inline std::string serToString(const std::uint8_t &t)
{
    return std::string(reinterpret_cast<const std::string::value_type *>(&t), sizeof(t));
}

template <> inline std::string serToString(const std::uint16_t &t)
{
    auto str = std::string(sizeof(t), '\0');
    CppUtilities::LE::getBytes(t, str.data());
    return str;
}

template <> inline std::string serToString(const std::uint32_t &t)
{
    auto str = std::string(sizeof(t), '\0');
    CppUtilities::LE::getBytes(t, str.data());
    return str;
}

template <> inline std::string serToString(const std::uint64_t &t)
{
    auto str = std::string(sizeof(t), '\0');
    CppUtilities::LE::getBytes(t, str.data());
    return str;
}

template <> inline void serFromString<std::string>(string_view str, std::string &ret)
{
    ret = std::string(str);
}

template <> inline void serFromString<>(string_view str, std::uint8_t &ret)
{
    if (str.size() != sizeof(ret)) {
        throw CppUtilities::ConversionException(CppUtilities::argsToString("value not 8-bit, got ", str.size(), " bytes instead"));
    }
    ret = static_cast<std::uint8_t>(*str.data());
}

template <> inline void serFromString<>(string_view str, std::uint16_t &ret)
{
    if (str.size() != sizeof(ret)) {
        throw CppUtilities::ConversionException(CppUtilities::argsToString("value not 16-bit, got ", str.size(), " bytes instead"));
    }
    ret = CppUtilities::LE::toUInt16(str.data());
}

template <> inline void serFromString<>(string_view str, std::uint32_t &ret)
{
    if (str.size() != sizeof(ret)) {
        throw CppUtilities::ConversionException(CppUtilities::argsToString("value not 32-bit, got ", str.size(), " bytes instead"));
    }
    ret = CppUtilities::LE::toUInt32(str.data());
}

template <> inline void serFromString<>(string_view str, std::uint64_t &ret)
{
    if (str.size() != sizeof(ret)) {
        throw CppUtilities::ConversionException(CppUtilities::argsToString("value not 64-bit, got ", str.size(), " bytes instead"));
    }
    ret = CppUtilities::LE::toUInt64(str.data());
}

/** This is the serialization interface for keys.
    You need to define your these functions for the types you'd like to use as keys.
*/
template <class T, class Enable> inline std::string keyConv(const T &t);

template <class T, typename std::enable_if<std::is_arithmetic<T>::value, T>::type * = nullptr> inline string_view keyConv(const T &t)
{
    return string_view(reinterpret_cast<const char *>(&t), sizeof(t));
}

template <class T, typename std::enable_if<std::is_same<T, std::string>::value, T>::type * = nullptr> inline string_view keyConv(const T &t)
{
    return t;
}

template <class T, typename std::enable_if<std::is_same<T, string_view>::value, T>::type * = nullptr> inline string_view keyConv(string_view t)
{
    return t;
}

/*!
 * \brief The LMDBIndexOps struct implements index operations, but only the operations that
 *        are broadcast to all indexes.
 *
 * Specifically, to deal with databases with less than the maximum number of interfaces, this
 * only includes calls that should be ignored for empty indexes.
 *
 * This class only needs methods that must happen for all indexes at once. So specifically, *not*
 * size<t> or get<t>. People ask for those themselves, and should no do that on indexes that
 * don't exist.
 */
template <class Class, typename Type, typename Parent> struct LMDB_SAFE_EXPORT LMDBIndexOps {
    explicit LMDBIndexOps(Parent *parent)
        : d_parent(parent)
    {
    }
    void put(MDBRWTransaction &txn, const Class &t, std::uint32_t id, unsigned int flags = 0)
    {
        txn->put(d_idx, keyConv(d_parent->getMember(t)), id, flags);
    }

    void del(MDBRWTransaction &txn, const Class &t, std::uint32_t id)
    {
        if (const auto rc = txn->del(d_idx, keyConv(d_parent->getMember(t)), id)) {
            throw LMDBError("Error deleting from index: ", rc);
        }
    }

    void clear(MDBRWTransaction &txn)
    {
        if (const auto rc = mdb_drop(*txn, d_idx, 0)) {
            throw LMDBError("Error clearing index: ", rc);
        }
    }

    void openDB(std::shared_ptr<MDBEnv> &env, string_view str, unsigned int flags)
    {
        d_idx = env->openDB(str, flags);
    }
    MDBDbi d_idx;
    Parent *d_parent;
};

/** This is an index on a field in a struct, it derives from the LMDBIndexOps */

template <class Class, typename Type, Type Class::*PtrToMember> struct index_on : LMDBIndexOps<Class, Type, index_on<Class, Type, PtrToMember>> {
    index_on()
        : LMDBIndexOps<Class, Type, index_on<Class, Type, PtrToMember>>(this)
    {
    }
    static Type getMember(const Class &c)
    {
        return c.*PtrToMember;
    }

    typedef Type type;
};

/** This is a calculated index */
template <class Class, typename Type, class Func> struct index_on_function : LMDBIndexOps<Class, Type, index_on_function<Class, Type, Func>> {
    index_on_function()
        : LMDBIndexOps<Class, Type, index_on_function<Class, Type, Func>>(this)
    {
    }
    static Type getMember(const Class &c)
    {
        Func f;
        return f(c);
    }

    typedef Type type;
};

/*!
 * \brief The TypedDBI class is the main class.
 * \tparam T Specifies the type to store within the database.
 * \tparam I Specifies an index, should be an instantiation of index_on.
 */
template <typename T, typename... I> class LMDB_SAFE_EXPORT TypedDBI {
public:
    // declare tuple for indexes
    using tuple_t = std::tuple<I...>;
    template <std::size_t N> using index_t = typename std::tuple_element_t<N, tuple_t>::type;

private:
    tuple_t d_tuple;

    template <class Tuple, std::size_t N> struct IndexIterator {
        static inline void apply(Tuple &tuple, auto &&func)
        {
            IndexIterator<Tuple, N - 1>::apply(tuple, std::forward<decltype(func)>(func));
            func(std::get<N - 1>(tuple));
        }
    };
    template <class Tuple> struct IndexIterator<Tuple, 1> {
        static inline void apply(Tuple &tuple, auto &&func)
        {
            func(std::get<0>(tuple));
        }
    };
    template <class Tuple> struct IndexIterator<Tuple, 0> {
        static inline void apply(Tuple &tuple, auto &&func)
        {
            CPP_UTILITIES_UNUSED(tuple)
            CPP_UTILITIES_UNUSED(func)
        }
    };
    void forEachIndex(auto &&func)
    {
        IndexIterator<tuple_t, std::tuple_size_v<tuple_t>>::apply(d_tuple, std::forward<decltype(func)>(func));
    }

public:
    TypedDBI(std::shared_ptr<MDBEnv> env, string_view name)
        : d_env(env)
        , d_name(name)
    {
        d_main = d_env->openDB(name, MDB_CREATE | MDB_INTEGERKEY);
        std::size_t index = 0;
        forEachIndex([&](auto &&i) { i.openDB(d_env, CppUtilities::argsToString(name, '_', index++), MDB_CREATE | MDB_DUPFIXED | MDB_DUPSORT); });
    }

    // We support readonly and rw transactions. Here we put the Readonly operations
    // which get sourced by both kinds of transactions
    template <class Parent> struct ReadonlyOperations {
        ReadonlyOperations(Parent &parent)
            : d_parent(parent)
        {
        }

        //! Number of entries in main database
        std::size_t size()
        {
            MDB_stat stat;
            mdb_stat(**d_parent.d_txn, d_parent.d_parent->d_main, &stat);
            return stat.ms_entries;
        }

        //! Number of entries in the various indexes - should be the same
        template <std::size_t N> std::size_t size()
        {
            MDB_stat stat;
            mdb_stat(**d_parent.d_txn, std::get<N>(d_parent.d_parent->d_tuple).d_idx, &stat);
            return stat.ms_entries;
        }

        //! Get item with id, from main table directly
        bool get(std::uint32_t id, T &t)
        {
            MDBOutVal data;
            if ((*d_parent.d_txn)->get(d_parent.d_parent->d_main, id, data))
                return false;

            serFromString(data.get<string_view>(), t);
            return true;
        }

        //! Get item through index N, then via the main database
        template <std::size_t N> std::uint32_t get(const index_t<N> &key, T &out)
        {
            MDBOutVal id;
            if (!(*d_parent.d_txn)->get(std::get<N>(d_parent.d_parent->d_tuple).d_idx, keyConv(key), id)) {
                if (get(id.get<std::uint32_t>(), out))
                    return id.get<std::uint32_t>();
            }
            return 0;
        }

        //! Cardinality of index N
        template <std::size_t N> std::uint32_t cardinality()
        {
            auto cursor = (*d_parent.d_txn)->getCursor(std::get<N>(d_parent.d_parent->d_tuple).d_idx);
            bool first = true;
            MDBOutVal key, data;
            std::uint32_t count = 0;
            while (!cursor.get(key, data, first ? MDB_FIRST : MDB_NEXT_NODUP)) {
                ++count;
                first = false;
            }
            return count;
        }

        //! End iderator type
        struct eiter_t {
        };

        //! Store the object as immediate member of iter_t (as opposed to using an std::unique_ptr or std::shared_ptr)
        template <typename> struct DirectStorage {
        };

        // can be on main, or on an index
        // when on main, return data directly
        // when on index, indirect
        // we can be limited to one key, or iterate over entire database
        // iter requires you to put the cursor in the right place first!
        template <template <typename> class StorageType, typename ElementType = T> struct iter_t {
            using UsingDirectStorage = CppUtilities::Traits::IsSpecializationOf<StorageType<ElementType>, DirectStorage>;

            explicit iter_t(Parent *parent, typename Parent::cursor_t &&cursor, bool on_index, bool one_key, bool end = false)
                : d_parent(parent)
                , d_cursor(std::move(cursor))
                , d_on_index(on_index) // is this an iterator on main database or on index?
                , d_one_key(one_key) // should we stop at end of key? (equal range)
                , d_end(end)
                , d_deserialized(false)
            {
                if (d_end)
                    return;
                if (d_cursor.get(d_key, d_id, MDB_GET_CURRENT))
                    d_end = true;
                else if (d_on_index && (*d_parent->d_txn)->get(d_parent->d_parent->d_main, d_id, d_data))
                    throw LMDBError("Missing id in constructor");
            }

            explicit iter_t(Parent *parent, typename Parent::cursor_t &&cursor, string_view prefix)
                : d_parent(parent)
                , d_cursor(std::move(cursor))
                , d_prefix(prefix)
                , d_on_index(true) // is this an iterator on main database or on index?
                , d_one_key(false)
                , d_end(false)
                , d_deserialized(false)
            {
                if (d_cursor.get(d_key, d_id, MDB_GET_CURRENT))
                    d_end = true;
                else if (d_on_index && (*d_parent->d_txn)->get(d_parent->d_parent->d_main, d_id, d_data))
                    throw LMDBError("Missing id in constructor");
            }

            std::function<bool(const MDBOutVal &)> filter;
            void del()
            {
                d_cursor.del();
            }

            bool operator!=(const eiter_t &) const
            {
                return !d_end;
            }

            bool operator==(const eiter_t &) const
            {
                return d_end;
            }

            string_view getRawData()
            {
                return d_on_index ? d_data.get<string_view>() : d_id.get<string_view>();
            }

            StorageType<ElementType> &allocatePointer()
            {
                static_assert(!UsingDirectStorage::value, "Cannot call getPointer() when using direct storage.");
                static_assert(CppUtilities::Traits::IsSpecializingAnyOf<StorageType<ElementType>, std::unique_ptr, std::shared_ptr>(),
                    "Pointer type not supported.");
                if (d_t != nullptr) {
                    return d_t;
                }
                if constexpr (CppUtilities::Traits::IsSpecializationOf<StorageType<ElementType>, std::unique_ptr>()) {
                    return d_t = std::make_unique<T>();
                } else if constexpr (CppUtilities::Traits::IsSpecializationOf<StorageType<ElementType>, std::shared_ptr>()) {
                    return d_t = std::make_shared<T>();
                }
            }

            StorageType<ElementType> &getPointer()
            {
                static_assert(!UsingDirectStorage::value, "Cannot call getPointer() when using direct storage.");
                if (!d_deserialized) {
                    allocatePointer();
                    serFromString(getRawData(), *d_t);
                    d_deserialized = true;
                }
                return d_t;
            }

            ElementType &derefValue()
            {
                if constexpr (UsingDirectStorage::value) {
                    return d_t;
                } else {
                    allocatePointer();
                    return *d_t;
                }
            }

            ElementType &value()
            {
                auto &res = derefValue();
                if (!d_deserialized) {
                    serFromString(getRawData(), res);
                    d_deserialized = true;
                }
                return res;
            }

            const ElementType &operator*()
            {
                return value();
            }

            const ElementType *operator->()
            {
                return &value();
            }

            // implements generic ++ or --
            iter_t &genoperator(MDB_cursor_op dupop, MDB_cursor_op op)
            {
                d_deserialized = false;
            next:;
                const auto rc = d_cursor.get(d_key, d_id, d_one_key ? dupop : op);
                if (rc == MDB_NOTFOUND) {
                    d_end = true;
                } else if (rc) {
                    throw LMDBError("Unable to get in genoperator: ", rc);
                } else if (!d_prefix.empty() && d_key.get<std::string_view>().rfind(d_prefix, 0) != 0) {
                    d_end = true;
                } else {
                    if (d_on_index && (*d_parent->d_txn)->get(d_parent->d_parent->d_main, d_id, d_data))
                        throw LMDBError("Missing id field in genoperator");
                    if (filter && !filter(d_data))
                        goto next;
                }
                return *this;
            }

            iter_t &operator++()
            {
                return genoperator(MDB_NEXT_DUP, MDB_NEXT);
            }
            iter_t &operator--()
            {
                return genoperator(MDB_PREV_DUP, MDB_PREV);
            }

            // get ID this iterator points to
            std::uint32_t getID()
            {
                return d_on_index ? d_id.get<std::uint32_t>() : d_key.get<std::uint32_t>();
            }

            const MDBOutVal &getKey()
            {
                return d_key;
            }

        private:
            // transaction we are part of
            Parent *d_parent;
            typename Parent::cursor_t d_cursor;

            // gcc complains if I don't zero-init these, which is worrying XXX
            std::string d_prefix;
            MDBOutVal d_key{ { 0, 0 } }, d_data{ { 0, 0 } }, d_id{ { 0, 0 } };
            bool d_on_index;
            bool d_one_key;
            bool d_end;
            bool d_deserialized;
            CppUtilities::Traits::Conditional<UsingDirectStorage, ElementType, StorageType<ElementType>> d_t;
        };

        template <std::size_t N, template <typename> class StorageType = DirectStorage> iter_t<StorageType> genbegin(MDB_cursor_op op)
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(std::get<N>(d_parent.d_parent->d_tuple).d_idx);

            MDBOutVal out, id;

            if (cursor.get(out, id, op)) {
                // on_index, one_key, end
                return iter_t<StorageType>{ &d_parent, std::move(cursor), true, false, true };
            }

            return iter_t<StorageType>{ &d_parent, std::move(cursor), true, false };
        };

        template <std::size_t N, template <typename> class StorageType = DirectStorage> iter_t<StorageType> begin()
        {
            return genbegin<N, StorageType>(MDB_FIRST);
        }

        template <std::size_t N, template <typename> class StorageType = DirectStorage> iter_t<StorageType> rbegin()
        {
            return genbegin<N, StorageType>(MDB_LAST);
        }

        template <template <typename> class StorageType = DirectStorage> iter_t<StorageType> begin()
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(d_parent.d_parent->d_main);

            MDBOutVal out, id;

            if (cursor.get(out, id, MDB_FIRST)) {
                // on_index, one_key, end
                return iter_t<StorageType>{ &d_parent, std::move(cursor), false, false, true };
            }

            return iter_t<StorageType>{ &d_parent, std::move(cursor), false, false };
        };

        template <std::size_t N, template <typename> class StorageType = DirectStorage> iter_t<DirectStorage, std::uint32_t> begin_idx()
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(std::get<N>(d_parent.d_parent->d_tuple).d_idx);

            MDBOutVal out, id;

            if (cursor.get(out, id, MDB_FIRST)) {
                // on_index, one_key, end
                return iter_t<DirectStorage, std::uint32_t>{ &d_parent, std::move(cursor), false, false, true };
            }

            return iter_t<DirectStorage, std::uint32_t>{ &d_parent, std::move(cursor), false, false };
        }

        eiter_t end()
        {
            return eiter_t();
        }

        // basis for find, lower_bound
        template <std::size_t N, template <typename> class StorageType>
        iter_t<StorageType> genfind(const typename std::tuple_element<N, tuple_t>::type::type &key, MDB_cursor_op op)
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(std::get<N>(d_parent.d_parent->d_tuple).d_idx);

            const auto keystr = keyConv(key);
            MDBInVal in(keystr);
            MDBOutVal out, id;
            out.d_mdbval = in.d_mdbval;

            if (cursor.get(out, id, op)) {
                // on_index, one_key, end
                return iter_t<StorageType>{ &d_parent, std::move(cursor), true, false, true };
            }

            return iter_t<StorageType>{ &d_parent, std::move(cursor), true, false };
        };

        template <std::size_t N, template <typename> class StorageType = DirectStorage> iter_t<StorageType> find(const index_t<N> &key)
        {
            return genfind<N, StorageType>(key, MDB_SET);
        }

        template <std::size_t N, template <typename> class StorageType = DirectStorage> iter_t<StorageType> lower_bound(const index_t<N> &key)
        {
            return genfind<N, StorageType>(key, MDB_SET_RANGE);
        }

        //! equal range - could possibly be expressed through genfind
        template <std::size_t N, template <typename> class StorageType = DirectStorage>
        std::pair<iter_t<StorageType>, eiter_t> equal_range(const index_t<N> &key)
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(std::get<N>(d_parent.d_parent->d_tuple).d_idx);

            const auto keyString = keyConv(key);
            MDBInVal in(keyString);
            MDBOutVal out, id;
            out.d_mdbval = in.d_mdbval;

            if (cursor.get(out, id, MDB_SET)) {
                // on_index, one_key, end
                return { iter_t<StorageType>{ &d_parent, std::move(cursor), true, true, true }, eiter_t() };
            }

            return { iter_t<StorageType>{ &d_parent, std::move(cursor), true, true }, eiter_t() };
        };

        //! equal range - could possibly be expressed through genfind
        template <std::size_t N, template <typename> class StorageType = DirectStorage>
        std::pair<iter_t<StorageType>, eiter_t> prefix_range(const index_t<N> &key)
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(std::get<N>(d_parent.d_parent->d_tuple).d_idx);

            const auto keyString = keyConv(key);
            MDBInVal in(keyString);
            MDBOutVal out, id;
            out.d_mdbval = in.d_mdbval;

            if (cursor.get(out, id, MDB_SET_RANGE)) {
                // on_index, one_key, end
                return { iter_t<StorageType>{ &d_parent, std::move(cursor), true, true, true }, eiter_t() };
            }

            return { iter_t<StorageType>(&d_parent, std::move(cursor), keyString), eiter_t() };
        };

        Parent &d_parent;
    };

    class LMDB_SAFE_EXPORT ROTransaction : public ReadonlyOperations<ROTransaction> {
    public:
        explicit ROTransaction()
            : ReadonlyOperations<ROTransaction>(*this)
            , d_parent(nullptr)
            , d_txn(nullptr)
        {
        }

        explicit ROTransaction(TypedDBI *parent)
            : ReadonlyOperations<ROTransaction>(*this)
            , d_parent(parent)
            , d_txn(std::make_shared<MDBROTransaction>(d_parent->d_env->getROTransaction()))
        {
        }

        explicit ROTransaction(TypedDBI *parent, const std::shared_ptr<MDBROTransaction> &txn)
            : ReadonlyOperations<ROTransaction>(*this)
            , d_parent(parent)
            , d_txn(txn)
        {
        }

        ROTransaction(ROTransaction &&rhs)
            : ReadonlyOperations<ROTransaction>(*this)
            , d_parent(rhs.d_parent)
            , d_txn(std::move(rhs.d_txn))

        {
            rhs.d_parent = nullptr;
        }

        ROTransaction &operator=(ROTransaction &&rhs)
        {
            d_parent = rhs.d_parent;
            d_txn = std::move(rhs.d_txn);
            rhs.d_parent = nullptr;
            return *this;
        }

        operator bool() const
        {
            return d_txn != nullptr;
        }

        std::shared_ptr<MDBROTransaction> getTransactionHandle()
        {
            return d_txn;
        }

        void close()
        {
            d_txn.reset();
        }

        typedef MDBROCursor cursor_t;

        TypedDBI *d_parent;
        std::shared_ptr<MDBROTransaction> d_txn;
    };

    class LMDB_SAFE_EXPORT RWTransaction : public ReadonlyOperations<RWTransaction> {
    public:
        explicit RWTransaction()
            : ReadonlyOperations<RWTransaction>(*this)
            , d_parent(nullptr)
            , d_txn(nullptr)
        {
        }

        explicit RWTransaction(TypedDBI *parent)
            : ReadonlyOperations<RWTransaction>(*this)
            , d_parent(parent)
        {
            d_txn = std::make_shared<MDBRWTransaction>(d_parent->d_env->getRWTransaction());
        }

        explicit RWTransaction(TypedDBI *parent, std::shared_ptr<MDBRWTransaction> txn)
            : ReadonlyOperations<RWTransaction>(*this)
            , d_parent(parent)
            , d_txn(txn)
        {
        }

        RWTransaction(RWTransaction &&rhs)
            : ReadonlyOperations<RWTransaction>(*this)
            , d_parent(rhs.d_parent)
            , d_txn(std::move(rhs.d_txn))
        {
            rhs.d_parent = 0;
        }

        RWTransaction &operator=(RWTransaction &&rhs)
        {
            d_parent = rhs.d_parent;
            d_txn = std::move(rhs.d_txn);
            rhs.d_parent = nullptr;
            return *this;
        }

        operator bool() const
        {
            return d_txn != nullptr;
        }

        // insert something, with possibly a specific id
        std::uint32_t put(const T &t, std::uint32_t id = 0)
        {
            unsigned int flags = 0;
            if (!id) {
                id = MDBGetMaxID(*d_txn, d_parent->d_main) + 1;
                flags = MDB_APPEND;
            }
            (*d_txn)->put(d_parent->d_main, id, serToString(t), flags);
            d_parent->forEachIndex([&](auto &&i) { i.put(*d_txn, t, id); });
            return id;
        }

        // modify an item 'in place', plus update indexes
        void modify(std::uint32_t id, std::function<void(T &)> func)
        {
            T t;
            if (!this->get(id, t))
                throw LMDBError("Could not modify id " + std::to_string(id));
            func(t);

            del(id); // this is the lazy way. We could test for changed index fields
            put(t, id);
        }

        //! delete an item, and from indexes
        void del(std::uint32_t id)
        {
            T t;
            if (!this->get(id, t))
                return;

            (*d_txn)->del(d_parent->d_main, id);
            clearIndex(id, t);
        }

        //! clear database & indexes
        void clear()
        {
            if (const auto rc = mdb_drop(**d_txn, d_parent->d_main, 0)) {
                throw LMDBError("Error database: ", rc);
            }
            d_parent->forEachIndex([&](auto &&i) { i.clear(*d_txn); });
        }

        //! commit this transaction
        void commit()
        {
            (*d_txn)->commit();
        }

        //! abort this transaction
        void abort()
        {
            (*d_txn)->abort();
        }

        typedef MDBRWCursor cursor_t;

        std::shared_ptr<MDBRWTransaction> getTransactionHandle()
        {
            return d_txn;
        }

    private:
        // clear this ID from all indexes
        void clearIndex(std::uint32_t id, const T &t)
        {
            d_parent->forEachIndex([&](auto &&i) { i.del(*d_txn, t, id); });
        }

    public:
        TypedDBI *d_parent;
        std::shared_ptr<MDBRWTransaction> d_txn;
    };

    //! Get an RW transaction
    RWTransaction getRWTransaction()
    {
        return RWTransaction(this);
    }

    //! Get an RO transaction
    ROTransaction getROTransaction()
    {
        return ROTransaction(this);
    }

    //! Get an RW transaction
    RWTransaction getRWTransaction(std::shared_ptr<MDBRWTransaction> txn)
    {
        return RWTransaction(this, txn);
    }

    //! Get an RO transaction
    ROTransaction getROTransaction(std::shared_ptr<MDBROTransaction> txn)
    {
        return ROTransaction(this, txn);
    }

    std::shared_ptr<MDBEnv> getEnv()
    {
        return d_env;
    }

private:
    std::shared_ptr<MDBEnv> d_env;
    MDBDbi d_main;
    std::string d_name;
};

} // namespace LMDBSafe
