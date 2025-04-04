// 0 base
#include <memory>
#include <vector>
#include <string>
#include <queue>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <variant>
#include <chrono>

#include <cassert>
#include <atomic>




// // 注册类型
// enum class ObjectType : uint8_t
// {
//     PiStrock = 0xDA,
//     NaStrock = 0xDB,
// };

// // 注册ID
// struct ObjectID
// {
//     ObjectType type;
//     int value;

//     explicit operator size_t() const noexcept
//     {
//         return (static_cast<size_t>(type) << 32) | value;
//     }
// };


// // 辅助哈希
// namespace std
// {
//     template <>
//     struct hash<ObjectID>
//     {
//         size_t operator()(const ObjectID &id) const noexcept
//         {
//             return static_cast<size_t>(id);
//         }
//     };
// }


// // 占用管理器
// template <auto PiStrock, auto NaStrock>
// class Occupy
// {
//     using timestamp = std::chrono::system_clock::time_point;

//     // 占用记录
//     struct OccupationRecord
//     {

//         ObjectID occupier;
//         std::chrono::system_clock::time_point start_time;
//         uint32_t access_count = 0;
//     };


//     // 存储类型
//     using StorageType = std::variant<
//         std::unique_ptr<PiStrock>,
//         std::unique_ptr<NaStrock>,
//         std::vector<ObjectID>>;

    
    
//     // 存储对象
//     std::unordered_map<ObjectID, StorageType> _objects_;
//     // 占用记录
//     std::unordered_multimap<ObjectID, OccupationRecord> _occupation_log_;
//     // ID计数器
//     std::atomic<int> _id_counter_{0};

    



// public:
//     template <typename T>
//     ObjectID register_object(T &&obj)
//     {
//         constexpr auto type = std::is_same_v<T, PiStrock>
//                                   ? ObjectType::PiStrock
//                                   : ObjectType::NaStrock;

//         ObjectID new_id{type, _id_counter_.fetch_add(1)};
//         _objects_.emplace(new_id,
//                           std::make_unique<std::decay_t<T>>(std::forward<T>(obj)));
//         return new_id;
//     }

//     std::vector<ObjectID> get_dependents(ObjectID id) const
//     {
//         std::vector<ObjectID> result;
//         if (auto it = _objects_.find(id); it != _objects_.end())
//         {
//             if (auto vec = std::get_if<std::vector<ObjectID>>(&it->second))
//             {
//                 result = *vec;
//             }
//         }
//         return result;
//     }


//     void answer()

//     void deregister_object(ObjectID id)
//     {
//         _objects_.erase(id);
//         _occupation_log_.erase(id);
//     }

//     void preallocate(size_t count)
//     {
//         _objects_.reserve(count);
//         _occupation_log_.reserve(count * 2);
//     }
// };


template <typename PiStrock, typename NaStrock>
class Occupy
{
    using timestamp = std::chrono::system_clock::time_point;


    struct PiONa
    {
        std::unordered_map<int, std::vector<int>> naTars;
    };

    struct NaOPi
    {
        std::unordered_map<int, std::vector<int>> piTars;
    };

    std::unordered_map<int, std::unique_ptr<PiStrock>> _pi_stocks_;
    std::unordered_map<int, std::unique_ptr<NaStrock>> _na_stocks_;
    std::unordered_map<int, PiONa> _pi_na_;
    std::unordered_map<int, NaOPi> _na_pi_;
    std:atomic<int> _idm_pi_ = 0;
    std:atomic<int> _idm_na_ = 0;

    int register(PiStrock &pi)
    {
        auto id = _idm_pi_++;
        if (_pi_stocks_.find(id) != _pi_stocks_.end())
        {
            return -1;
        }
        else
        {
            _pi_stocks_.emplace(id, std::make_unique<PiStrock>(pi));
        }
        return id;
    }

    int register(NaStrock &na)
    {
        auto id = _idm_na_++;
        if (_na_stocks_.find(id) != _na_stocks_.end())
        {
            return false;
        }
        else
        {
            _na_stocks_.emplace(id, std::make_unique<NaStrock>(na));
        }
    }

    
};