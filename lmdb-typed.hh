#pragma once

#include "./lmdb-safe.hh"

#include <c++utilities/conversion/binaryconversion.h>
#include <c++utilities/conversion/conversionexception.h>
#include <c++utilities/conversion/stringbuilder.h>

#include <functional>

namespace LMDBSafe {

/*!
 * \brief The type used to store IDs. "0" indicates "no such ID".
 */
using IDType = std::uint32_t;

/*!
 * \brief Converts \a t to an std::string.
 *
 * This is the serialization interface. You need to define this function for the
 * types you'd like to store.
 */
template <typename T> std::string serToString(const T &t);

/*!
 * \brief Initializes \a ret from \a str.
 *
 * This is the deserialization interface. You need to define this function for the
 * types you'd like to store.
 */
template <typename T> void serFromString(string_view str, T &ret);

/// \cond
/// Define some "shortcuts" (to avoid full-blown serialization stuff for trivial cases):
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

/// \endcond

/*!
 * \brief Converts \a t to an std::string.
 *
 * This is the serialization interface for keys. You need to define your this function
 * for the types you'd like to use as keys.
 */
template <class T, class Enable> inline std::string keyConv(const T &t);

template <class T, typename std::enable_if<std::is_arithmetic<T>::value, T>::type * = nullptr> inline string_view keyConv(const T &t)
{
    return string_view(reinterpret_cast<const char *>(&t), sizeof(t));
}

/// \cond
/// Define keyConv for trivial cases:
template <class T, typename std::enable_if<std::is_same<T, std::string>::value, T>::type * = nullptr> inline string_view keyConv(const T &t)
{
    return t;
}

template <class T, typename std::enable_if<std::is_same<T, string_view>::value, T>::type * = nullptr> inline string_view keyConv(string_view t)
{
    return t;
}
/// \endcond

/*!
 * \brief The LMDBIndexOps struct implements index operations, but only the operations that
 *        are broadcast to all indexes.
 *
 * Specifically, to deal with databases with less than the maximum number of interfaces, this
 * only includes calls that should be ignored for empty indexes.
 *
 * This class only needs methods that must happen for all indexes at once. So specifically, *not*
 * size<t> or get<t>. People ask for those themselves, and should not do that on indexes that
 * don't exist.
 */
template <class Class, typename Type, typename Parent> struct LMDB_SAFE_EXPORT LMDBIndexOps {
    explicit LMDBIndexOps(Parent *parent)
        : d_parent(parent)
    {
    }
    void put(MDBRWTransaction &txn, const Class &t, IDType id, unsigned int flags = 0)
    {
        txn->put(d_idx, keyConv(d_parent->getMember(t)), id, flags);
    }

    void del(MDBRWTransaction &txn, const Class &t, IDType id)
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

/*!
 * \brief The index_on struct is used to declare an index on a member variable of a particular type.
 */
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

/*!
 * \brief The index_on_function struct is used to declare an index which is dynamically computed via
 *        a function.
 */
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
 *
 * Open issues:
 * - Perhaps use the separate index concept from multi_index.
 * - Perhaps get either to be of same type so for(auto& a : x) works
 *   make it more value "like" with unique_ptr.
 */
template <typename T, typename... I> class LMDB_SAFE_EXPORT TypedDBI {
public:
    // declare tuple for indexes
    using tuple_t = std::tuple<I...>;
    template <std::size_t N> using index_t = typename std::tuple_element_t<N, tuple_t>::type;

private:
    tuple_t d_tuple;

    /// \cond
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
    /// \endcond

public:
    TypedDBI(std::shared_ptr<MDBEnv> env, string_view name)
        : d_env(env)
        , d_name(name)
    {
        d_main = d_env->openDB(name, MDB_CREATE | MDB_INTEGERKEY);
        std::size_t index = 0;
        forEachIndex([&](auto &&i) { i.openDB(d_env, CppUtilities::argsToString(name, '_', index++), MDB_CREATE | MDB_DUPFIXED | MDB_DUPSORT); });
    }

    /*!
     * \brief The ReadonlyOperations struct defines read-only operations.
     */
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

        /*!
         * \brief Returns the highest ID or 0 if the database is empty.
         */
        IDType maxID()
        {
            auto cursor = (*d_parent.d_txn)->getCursor(d_parent.d_parent->d_main);
            MDBOutVal idval, maxcontent;
            auto id = IDType(0);
            if (!cursor.get(idval, maxcontent, MDB_LAST)) {
                id = idval.get<IDType>();
            }
            return id;
        }

        /*!
         * \brief Returns the next highest ID in the database.
         * \remarks Never returns 0 so it can be used as special "no such ID" value.
         * \throws Throws LMDBError when running out of IDs.
         */
        IDType nextID()
        {
            const auto id = maxID();
            if (id < std::numeric_limits<IDType>::max()) {
                return id + 1;
            }
            throw LMDBError("Running out of IDs");
        }

        /*!
         * \brief Returns an ID not used in the database so far.
         * \remarks
         * - Lower IDs are reused but an extensive search for "gabs" is avoided.
         * - Never returns 0 so it can be used as special "no such ID" value.
         * \throws Throws LMDBError when running out of IDs.
         */
        IDType newID()
        {
            auto cursor = (*d_parent.d_txn)->getCursor(d_parent.d_parent->d_main);
            MDBOutVal idval, maxcontent;
            auto id = IDType(1);
            if (!cursor.get(idval, maxcontent, MDB_FIRST)) {
                id = idval.get<IDType>();
            }
            if (id > 1) {
                return id - 1;
            }
            if (!cursor.get(idval, maxcontent, MDB_LAST)) {
                id = idval.get<IDType>();
            } else {
                id = 0;
            }
            if (id < std::numeric_limits<IDType>::max()) {
                return id + 1;
            }
            throw LMDBError("Running out of IDs");
        }

        //! Get item with id, from main table directly
        bool get(IDType id, T &t)
        {
            MDBOutVal data;
            if ((*d_parent.d_txn)->get(d_parent.d_parent->d_main, id, data))
                return false;

            serFromString(data.get<string_view>(), t);
            return true;
        }

        //! Get item through index N, then via the main database
        template <std::size_t N> IDType get(const index_t<N> &key, T &out)
        {
            MDBOutVal id;
            if (!(*d_parent.d_txn)->get(std::get<N>(d_parent.d_parent->d_tuple).d_idx, keyConv(key), id)) {
                if (get(id.get<IDType>(), out))
                    return id.get<IDType>();
            }
            return 0;
        }

        //! Cardinality of index N
        template <std::size_t N> IDType cardinality()
        {
            auto cursor = (*d_parent.d_txn)->getCursor(std::get<N>(d_parent.d_parent->d_tuple).d_idx);
            bool first = true;
            MDBOutVal key, data;
            IDType count = 0;
            while (!cursor.get(key, data, first ? MDB_FIRST : MDB_NEXT_NODUP)) {
                ++count;
                first = false;
            }
            return count;
        }

        /*!
         * \brief The eiter_t struct is the end iterator.
         */
        struct eiter_t {
        };

        //! Store the object as immediate member of iter_t (as opposed to using an std::unique_ptr or std::shared_ptr)
        template <typename> struct DirectStorage {
        };

        /*!
         * \brief The iter_t struct is the iterator type for walking through the database rows.
         * \remarks
         * - The iterator can be on the main database or on an index. It returns the data directly on
         *   the main database and indirectly when on an index.
         * - An iterator can be limited to one key or iterate over the entire database.
         * - The iter_t struct requires you to put the cursor in the right place first.
         * - The object can be stored as direct member of iter_t or as std::unique_ptr or std::shared_ptr
         *   by specifying the corresponding template as \tp ElementType. The pointer can then be accessed
         *   via getPointer(). Note that the returned pointer object is re-used when the iterator is incremented
         *   or decremented unless the owned object is moved into another pointer object.
         */
        template <template <typename...> class StorageType, typename ElementType = T> struct iter_t {
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
                , d_on_index(true)
                , d_one_key(false)
                , d_end(false)
                , d_deserialized(false)
            {
                if (d_cursor.get(d_key, d_id, MDB_GET_CURRENT))
                    d_end = true;
                else if ((*d_parent->d_txn)->get(d_parent->d_parent->d_main, d_id, d_data))
                    throw LMDBError("Missing id in constructor");
            }

            void del()
            {
                auto id = this->getID();
                auto &value = this->value();
                if (d_on_index) {
                    (*d_parent->d_txn)->del(d_parent->d_parent->d_main, d_data);
                } else {
                    d_cursor.del();
                }
                d_parent->d_parent->forEachIndex([&](auto &&i) { i.del(*d_parent->d_txn, value, id); });
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

            //! Implements generic ++ or --.
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

            //! Returns the ID this iterator points to.
            IDType getID()
            {
                return d_on_index ? d_id.get<IDType>() : d_key.get<IDType>();
            }

            const MDBOutVal &getKey()
            {
                return d_key;
            }

            //! A filter to allow skipping rows by their raw value.
            std::function<bool(const MDBOutVal &)> filter;

        private:
            //! The transaction the iterator is part of.
            Parent *d_parent;
            typename Parent::cursor_t d_cursor;

            std::string d_prefix;
            MDBOutVal d_key{ { 0, 0 } }, d_data{ { 0, 0 } }, d_id{ { 0, 0 } };
            //! Whether it is an iterator on the main database or an index.
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
        }

        template <template <typename> class StorageType = DirectStorage> iter_t<StorageType> rbegin()
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(d_parent.d_parent->d_main);

            MDBOutVal out, id;

            if (cursor.get(out, id, MDB_LAST)) {
                // on_index, one_key, end
                return iter_t<StorageType>{ &d_parent, std::move(cursor), false, false, true };
            }

            return iter_t<StorageType>{ &d_parent, std::move(cursor), false, false };
        }

        template <template <typename> class StorageType = DirectStorage> iter_t<StorageType> lower_bound(IDType id)
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(d_parent.d_parent->d_main);

            MDBInVal in(id);
            MDBOutVal out, id2;
            out.d_mdbval = in.d_mdbval;

            if (cursor.get(out, id2, MDB_SET_RANGE)) {
                // on_index, one_key, end
                return iter_t<StorageType>{ &d_parent, std::move(cursor), false, false, true };
            }

            return iter_t<StorageType>{ &d_parent, std::move(cursor), false, false };
        }

        template <std::size_t N, template <typename> class StorageType = DirectStorage> iter_t<DirectStorage, IDType> begin_idx()
        {
            typename Parent::cursor_t cursor = (*d_parent.d_txn)->getCursor(std::get<N>(d_parent.d_parent->d_tuple).d_idx);

            MDBOutVal out, id;

            if (cursor.get(out, id, MDB_FIRST)) {
                // on_index, one_key, end
                return iter_t<DirectStorage, IDType>{ &d_parent, std::move(cursor), false, false, true };
            }

            return iter_t<DirectStorage, IDType>{ &d_parent, std::move(cursor), false, false };
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

        //! Returns the range matching the specified \a key.
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

        //! Returns the range where the key starts with the specified \a key.
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

    /*!
     * \brief The ROTransaction class represents a read-only transaction.
     */
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

    /*!
     * \brief The RWTransaction class represents a read-write transaction.
     */
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

        //! Inserts something, with possibly a specific id.
        IDType put(const T &t, IDType id = 0)
        {
            unsigned int flags = 0;
            if (!id) {
                id = this->nextID();
                flags = MDB_APPEND;
            }
            (*d_txn)->put(d_parent->d_main, id, serToString(t), flags);
            d_parent->forEachIndex([&](auto &&i) { i.put(*d_txn, t, id); });
            return id;
        }

        //! Modifies an item "in place" updating indexes.
        void modify(IDType id, const std::function<void(T &)> &func)
        {
            T t;
            if (!this->get(id, t))
                throw LMDBError("Could not modify id " + std::to_string(id));
            func(t);

            del(id); // this is the lazy way. We could test for changed index fields
            put(t, id);
        }

        //! Deletes an item from the main database and from indexes.
        void del(IDType id)
        {
            T t;
            if (!this->get(id, t))
                return;

            (*d_txn)->del(d_parent->d_main, id);
            clearIndex(id, t);
        }

        //! Clears the database and indexes.
        void clear()
        {
            if (const auto rc = mdb_drop(**d_txn, d_parent->d_main, 0)) {
                throw LMDBError("Error database: ", rc);
            }
            d_parent->forEachIndex([&](auto &&i) { i.clear(*d_txn); });
        }

        //! \brief Rebuilds the database, possibly throwing out invalid objects.
        //! \param func Specifies a function which is supposed to return whether an object is still valid.
        //!             It might modify the passed object in order to "fix" it.
        void rebuild(const std::function<bool(IDType id, T *obj)> &func)
        {
            // clear all indexes to get rid of invalid entries
            d_parent->forEachIndex([&](auto &&i) { i.clear(*d_txn); });
            // check all objects via func
            for (auto i = this->begin(), end = this->end(); i != end; ++i) {
                T *val = nullptr;
                try {
                    val = &i.value();
                } catch (...) { // catch possible deserialization errors
                }
                if (func(i.getID(), val)) {
                    if (val) {
                        put(*val, i.getID());
                    }
                } else {
                    i.del();
                }
            }
        }

        //! Commits this transaction.
        void commit()
        {
            (*d_txn)->commit();
        }

        //! Aborts this transaction.
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
        void clearIndex(IDType id, const T &t)
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
