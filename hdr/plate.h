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


template <auto objType, auto storeType>
class Plate
{

    // struct Request
    // {
    //     int obj_id;
    // };
    enum class RequestType
    {
        FINISH,
        REJECT,
        PROCESS,
        ERROR
    };
    std::atomic<int> _store_id_ = 0;
    std::atomic<int> _obj_id_ = 0;
    std::atomic<int> _req_id_ = 0;
    std::unordered_map<int, std::shared_ptr<storeType>> _stores_;
    std::unordered_map<int, std::shared_ptr<objType>> _objs_;
    std::unordered_map<int, int> _requests_;
    std::unordered_map<int, int> _obj_link_store_;
    std::unordered_map<int, int> _store_link_obj_;

public:
    
    int request(int& obj_id)
    {
        if (_objs_.find(obj_id) == _objs_.end())
        {
            return -1;
        }
        else if (_obj_link_store_.find(obj_id) == _obj_link_store_.end())
        {
            return -1;
        }
        for (auto& [id, store]: _stores_)
        {
            if (store -> satisfy(obj))
            {
                _obj_link_store_[obj_id] = id;
                _store_link_obj_[id] = obj_id;
                return RequestType::PROCESS;
            }
        }
        return RequestType::FINISH;
    }

    RequestType check_request(int& req_id)
    {
        if (_requests_.find(req_id) == _requests_.end())
        {
            return RequestType::FINISH;
        }
        return RequestType::PROCESS;
    }

    void erase_request(int& req_id)
    {
        _requests_.erase(req_id);
    }



    int register(objType& obj)
    {
        int id = _obj_id_++;
        if (_objs_.find(id) != _objs_.end())
        {
            return -1;
        }
        _objs_[id] = std::make_shared<objType>(obj);
        return id;
    }

    int register(storeType& store)
    {
        int id = _store_id_++;
        if (_stores_.find(id) != _stores_.end())
        {
            return -1;
        }
        _stores_[id] = std::make_shared<storeType>(store);
        return id;
    }
};