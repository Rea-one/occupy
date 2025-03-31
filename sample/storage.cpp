
// 目前不允许拆分存储

#include <iostream>
#include <array>
#include <vector>
#include <array>
#include <stack>
#include <queue>
#include <map>
#include <set>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <memory>
#include <fstream>
#include <cfloat>
#include <cmath>
#include <climits>
#include <numeric>

// 系统常量定义
constexpr int MAX_DISK_NUM = 10 + 1;     // 最大硬盘数量
constexpr int MAX_DISK_SIZE = 16384 + 1; // 单盘最大块数
constexpr int MAX_TAG_NUM = 16 + 1;
constexpr int FRE_PER_SLICING = 1800;
constexpr int REP_NUM = 3;        // 对象副本数
constexpr int MAX_DEADLINE = 105; // 请求最大截止时间

constexpr int ACITVE_LIMIT_SHIFT = 2;
constexpr int PRE_RESET_TOKEN = 80;
// 新增：最大重试机制
constexpr int MAX_RETRIES = 3;

// 权重配置
static constexpr std::array weights{0.3f, 0.6f, 0.2f};

static const std::array<std::array<float, 3>, 3> WEIGHTS = {
    {{/*high*/ 0.3f, 0.6f, 0.2f},
     {/*middle*/ 0.2f, 0.65f, 0.15f},
     {/*low*/ 0.1f, 0.8f, 0.1f}}};

int the_max(int a, int b);
int the_ceil(float a);

enum class ForeTask
{
    Empty,
    Pass,
    Jump,
    Read
};

enum class TagPreState
{
    hot,
    cold
};

enum class TagState
{
    high,
    middle,
    low
};

struct CandidateWrite
{
    int disk_id = 0;
    float score = 0;
    int write_pos = 0;
};

struct CandidateRead
{
    int disk_id = 0;
    float score = 0;
    int cost = 0;
};

class ReadRequest
{
public:
    int req_id = 0; // 请求ID
    int obj_id = 0; // 对象ID
    int occupy_id = 0;
    int rec_time = 0;                   // 到来时间
    std::vector<bool> request_extent{}; // 待读取块列表
    int left_cursor = 0;
    int right_cursor = 0;
    std::pair<ForeTask, std::pair<int, int>> fore_task = {ForeTask::Empty, {0, 0}};

    int readed = 0;
    int max_extent{};

    bool finished() const;
    bool requesting();
    bool reject() const;
    bool reg_block(int order);
    bool operator<(const ReadRequest &other) const;
    void init(const int &request_id, const int &object_id, const int &reach_time, const int &request_size);
    void reset_task();
    void reset();
};

class Tag
{
public:
    int id = 0;
    int slice_size = 0;
    int count = 0;
    TagState state = TagState::low;
    TagPreState pre_state = TagPreState::cold;
    int the_dels{};
    int the_writes{};
    int the_reads{};
    std::vector<int> pre_del;
    std::vector<int> pre_write;
    std::vector<int> pre_read;
    void reg();
    void init();
};

class Object
{
public:
    int id{};                                                         // 对象唯一标识
    int size;                                                         // 对象占用的块数
    std::shared_ptr<Tag> tag{};                                       // 分类标签（预留字段）
    std::array<std::pair<int, std::vector<int>>, REP_NUM> replicas{}; // {磁盘号，{块号}}
    // 建立反向映射表（可在对象构造/更新时维护）
    std::vector<std::weak_ptr<ReadRequest>> read_requests{};
    Object();

    bool has_located(const int &current_disk_id) const;
    // const std::pair<int, std::vector<int>> &get_best_replica(const std::vector<Disk> &disks) const;
    void continue_write(CandidateWrite focus, const int &disk_size);
    int get_order(int &disk_id, int &location) const;
    // void clean_block_mappings();
    void clean();
};

class Disk
{
public:
    int id{}; // 硬盘ID
    int max_token{};
    int token{};
    int pre_token = PRE_RESET_TOKEN;
    int head_pos = 1;             // 磁头当前位置
    int capacity{};               // 总块容量
    int free_blocks{};            // 剩余可用块数
    float fragmentation{};        // 碎片率（0-1）
    std::vector<int> block_map{}; // 块占用状态表
    std::vector<std::pair<int, int>> block_idx{};
    std::unordered_set<int> obj_id_set{}; // 对象ID池
    std::weak_ptr<ReadRequest> occupy{};
    std::string actions{};

    Disk(int id, int size);
    int now();
    int get_pass_cost(int target) const;
    int tag_count(const std::unordered_map<int, Object> &obj_pool, int tag_id);
    float tag_rate(std::unordered_map<int, Object> &obj_pool, int tag_id);
    int find_available_position(int num) const;
    int find_near_write_avaliable(int num) const;
    void reg_obj(const int &obj_id);
    std::vector<int> write_blocks(const int &data, const int &target_position, const int &num);
    bool have_obj(int obj_id) const;
    bool can_react_req(const ReadRequest &req) const;
    void answer_request(std::unordered_map<int, Object> &objects, ReadRequest &req);
    int read_up(Object &obj, ReadRequest &req);
    bool try_read_block(Object &obj, ReadRequest &req);
    bool readable(int &obj_id);
    int get_read_cost();
    bool jump(const int &target_position);
    bool try_read();
    void pass_to_end();
    void pass_to_target(const int &target_position);
    void so_jump(const int &target_position);
    void so_pass();
    void so_read();
    void read_actions(std::ostream &os);
    bool current_satisfy(Object &obj, ReadRequest &req);
    bool erase_obj(Object &obj);
};

class StorageSystem
{
    std::vector<Disk> disks;                 // 硬盘集合
    std::unordered_map<int, Object> objects; // 对象仓库
    int current_time = 0;                    // 模拟时钟
    std::string current{};
    int stamp{};        // 时间戳
    int tag_num{};      // 标签数量
    int disk_num{};     // 硬盘数量
    int disk_size{};    // 单盘容量
    int max_token{};    // 令牌桶容量
    int active_limit{}; // 活动请求限制

    std::vector<Tag> tags{};

    std::priority_queue<std::shared_ptr<ReadRequest>> read_queue{};          // 优先队列
    std::unordered_map<int, std::shared_ptr<ReadRequest>> active_requests{}; // 活动请求表
    std::vector<int> finished_requests{};

    bool time_left();
    std::vector<int> watch_disk();
    float calculate_locality(Disk &disk, int obj_id);
    void activate_operations();
    void calm_requests();
    void update_active_requests();

public:
    int patience; // 任务时间

    StorageSystem(std::istream &is, std::ostream &os);
    void write_object(int obj_id, int size, int tag);
    // void process_reads(std::ostream &os);
    std::vector<int> process_delete(int obj_id);
    void process_delete(int obj_id, std::ostream &os);
    void act_time(std::istream &is, std::ostream &os);
    void act_write(std::istream &is, std::ostream &os);
    void act_read(std::istream &is, std::ostream &os);
    void act_delete(std::istream &is, std::ostream &os);
};

// 函数定义
int the_max(int a, int b)
{
    return a > b ? a : b;
}

int the_ceil(float a)
{
    int result = static_cast<int>(a);
    if (a - result == 0)
    {
        return result;
    }
    else
    {
        return result + 1;
    }
}

void ReadRequest::init(const int &request_id, const int &object_id, const int &reach_time, const int &request_size)
{
    request_extent.resize(request_size, false);
    req_id = request_id;
    obj_id = object_id;
    rec_time = reach_time;
    max_extent = request_size;
    right_cursor = request_size - 1;
}

void ReadRequest::reset_task()
{
    fore_task = {ForeTask::Empty, {0, 0}};
}

void ReadRequest::reset()
{
    req_id = 0;
    obj_id = 0;
    rec_time = 0;
    request_extent.resize(0);
    int first_order = -1;
    int last_order = -1;
    fore_task = {ForeTask::Empty, {0, 0}};

    int readed = 0;
    int max_extent = 0;
}

void Tag::reg()
{

    if (count > 0.8 * the_writes)
    {
        state = TagState::low;
    }
    else if (count > 0.5 * the_writes)
    {
        state = TagState::middle;
    }
    else if (count > 0.2 * the_writes)
    {
        state = TagState::high;
    }

    count++;
}

void Tag::init()
{
    the_dels = std::accumulate(pre_del.begin(), pre_del.end(), 0);
    the_writes = std::accumulate(pre_write.begin(), pre_write.end(), 0);
    the_reads = std::accumulate(pre_read.begin(), pre_read.end(), 0);
}

bool ReadRequest::finished() const
{
    return readed == max_extent;
}

bool ReadRequest::requesting()
{
    if (reject())
    {
        fore_task = {ForeTask::Empty, {0, 0}};
        return false;
    }
    return true;
}

bool ReadRequest::reject() const
{
    return finished() || fore_task.first != ForeTask::Empty;
}

bool ReadRequest::reg_block(int order)
{
    if (order < 0 || order >= max_extent || request_extent[order])
        return false;

    request_extent[order] = true;
    readed++;

    // 精确更新游标：仅当当前块是边界时才重新计算
    if (order == left_cursor)
    {
        while (left_cursor <= right_cursor && request_extent[left_cursor])
        {
            left_cursor++;
        }
    }
    if (order == right_cursor)
    {
        while (right_cursor >= left_cursor && request_extent[right_cursor])
        {
            right_cursor--;
        }
    }

    // 完成判断必须严格检查所有块
    if (readed == max_extent)
    { // 正确完成条件
        left_cursor = right_cursor = -1;
    }
    return true;
}

bool ReadRequest::operator<(const ReadRequest &other) const
{
    return rec_time > other.rec_time; // 最小堆实现
}

Disk::Disk(int id, int size) : id(id), capacity(size), free_blocks(size),
                               block_map(size + 1, 0), fragmentation(0) {}

int Disk::now()
{
    return block_map[head_pos];
}

int Disk::get_pass_cost(int target) const
{
    return (target >= head_pos) ? target - head_pos : (capacity - head_pos) + target;
}

int Disk::tag_count(const std::unordered_map<int, Object> &obj_pool, int tag_id)
{
    int count = 0;
    for (auto &tar : obj_id_set)
    {
        auto it = obj_pool.find(tar);
        if (it == obj_pool.end())
            continue;

        if (it->second.tag && it->second.tag->id == tag_id)
        { // 添加空指针检查
            count++;
        }
    }
    return count;
}

float Disk::tag_rate(std::unordered_map<int, Object> &obj_pool, int tag_id)
{
    int matching = 0;
    int total_valid = 0;

    for (auto &tar : obj_id_set)
    {
        auto it = obj_pool.find(tar);
        if (it == obj_pool.end())
            continue;

        total_valid++;
        if (it->second.tag && it->second.tag->id == tag_id)
        {
            matching++;
        }
    }
    return total_valid > 0 ? static_cast<float>(matching) / total_valid : 0.0f;
}

// 统一查找函数
int Disk::find_available_position(int num) const
{
    if (num <= 0 || num > capacity)
        return 0;

    int cut_count = -1;
    int count = 0;
    int start = head_pos;
    for (int step = 0; step < capacity; ++step)
    {
        int current_pos = (head_pos + step - 1) % capacity + 1;
        // 移除冗余边界检查：(current_pos == 0) ? capacity : current_pos;

        if (block_map[current_pos] == 0)
        {
            if (count == 0)
            {
                start = current_pos;
                if (cut_count == -1)
                    cut_count = current_pos;
            }
            if (++count == num)
                return start;
        }
        else
        {
            count = 0;
        }
    }
    for (int step = capacity; step > 0; --step)
    {
        int current_pos = (head_pos + step - 1) % capacity + 1;
        if (block_map[current_pos] == 0)
        {
            start = current_pos;
            if (cut_count == 0)
                break;
            if (++cut_count == num)
                return start;
        }
        else
        {
            cut_count = 0;
        }
    }
    return 0;
}

int Disk::find_near_write_avaliable(int num) const
{
    if (num <= 0 || num > capacity)
        return 0;

    int count = 0;
    int start = head_pos;
    for (int step = 0; step < capacity; ++step)
    {
        int current_pos = (head_pos + step - 1) % capacity + 1;
        // 移除冗余边界检查：(current_pos == 0) ? capacity : current_pos;

        if (block_map[current_pos] == 0)
        {
            if (count == 0)
                start = current_pos;
            if (++count == num)
                return start;
        }
        else
        {
            count = 0;
        }
    }
    return 0;
}

void Disk::reg_obj(const int &obj_id)
{ // 改用const引用
    if (obj_id_set.count(obj_id))
        return; // 改用unordered_set
    obj_id_set.insert(obj_id);
}

std::vector<int> Disk::write_blocks(const int &data, const int &target_position, const int &num)
{
    std::vector<int> written_blocks;
    if (target_position < 1 || target_position > capacity)
        return written_blocks;
    if (num <= 0 || num > free_blocks)
        return written_blocks;

    written_blocks.reserve(num); // 预分配空间提升性能

    for (int i = 0; i < num; i++)
    {
        // 优化环形计算逻辑
        int pos = (target_position + i - 1) % capacity + 1;
        block_map[pos] = data;
        written_blocks.push_back(pos);
    }

    free_blocks -= num;
    return written_blocks;
}

bool Disk::have_obj(int obj_id) const
{
    return obj_id_set.find(obj_id) != obj_id_set.end();
}

bool Disk::can_react_req(const ReadRequest &req) const
{
    return have_obj(req.obj_id); // 复用优化后的方法
}

void Disk::answer_request(std::unordered_map<int, Object> &objects, ReadRequest &req)
{
    if (token >= max_token || !can_react_req(req) || req.reject())
        return;

    auto &obj = objects[req.obj_id];
    auto &replicas = obj.replicas;
    auto it = std::find_if(replicas.begin(), replicas.end(),
                           [this](const auto &replica)
                           { return replica.first == this->id; });

    if (it == replicas.end())
        return;

    occupy = std::make_shared<ReadRequest>(req);
    req.occupy_id = id;

    int target_pos = it->second[req.left_cursor]; // 获取目标块位置

    // 当前是否可读？
    if (readable(req.obj_id))
    {
        read_up(obj, req);
        return;
    }

    int pass_cost = get_pass_cost(target_pos);
    int remaining = max_token - token;

    // 情况1：当前token足够直接移动到目标位置
    if (pass_cost <= remaining)
    {
        pass_to_target(target_pos);
        read_up(obj, req); // 移动后尝试读取
    }
    // 情况2：需要规划跨时间片动作
    else
    {
        if (token == 0)
        { // 时间片开始时可执行Jump
            // 比较两种方案的可行性
            if (pass_cost <= max_token)
            { // 下一时间片可完成Pass
                pass_to_target(target_pos);
                read_up(obj, req);
            }
            else
            { // 需要Jump
                so_jump(target_pos);
            }
        }
        else
        {
            // 比较两种方案的可行性
            if (pass_cost <= max_token + remaining)
            { // 下一时间片可完成Pass
                pass_to_target(target_pos);
                req.fore_task = {ForeTask::Pass, {it->first, target_pos}};
            }
            else
            { // 需要Jump
                req.fore_task = {ForeTask::Jump, {it->first, target_pos}};
            }
        }
    }
}

// 已完成
int Disk::read_up(Object &obj, ReadRequest &req)
{
    int count = 0;
    std::shared_ptr<ReadRequest> req_ptr = std::make_shared<ReadRequest>(req);
    while (token < max_token)
    {
        // 每次循环前检查磁头位置是否在请求范围内
        int current_order = obj.get_order(id, head_pos);
        if (current_order < req.left_cursor || current_order > req.right_cursor)
        {
            break;
        }

        if (!try_read_block(obj, req))
        {
            break;
        }
        count++;
    }

    // 最终确认磁头位置是否在请求范围内
    const int final_order = obj.get_order(id, head_pos);
    if (final_order >= req.left_cursor && final_order <= req.right_cursor)
    {
        occupy = req_ptr;   // 使用原请求的 shared_ptr
        req.occupy_id = id; // 直接更新原请求的 occupy_id
    }
    else
    {
        occupy.reset();
        req.occupy_id = 0;
        req.fore_task = {ForeTask::Empty, {0, 0}};
    }
    return count;
}

bool Disk::try_read_block(Object &obj, ReadRequest &req)
{
    // 条件检查
    if (!current_satisfy(obj, req) || get_read_cost() > (max_token - token))
    {
        return false;
    }

    // 获取块顺序
    int disk_id = this->id;
    int location = head_pos;
    int order = obj.get_order(disk_id, location);

    // 仅当块顺序有效且注册成功时执行读取
    if (order >= 0 && req.reg_block(order))
    {
        so_read();
        return true;
    }
    return false;
}

bool Disk::readable(int &obj_id)
{
    return block_map[head_pos] == obj_id;
}

int Disk::get_read_cost()
{
    return the_max(16, the_ceil(pre_token * 0.8f));
}

bool Disk::jump(const int &target_position)
{
    if (token != 0)
    {
        return false;
    }
    so_jump(target_position);
    return true;
}

bool Disk::try_read()
{
    if (get_read_cost() <= max_token - token)
    {
        so_read();
        return true;
    }
    return false;
}

void Disk::pass_to_end()
{
    pre_token = PRE_RESET_TOKEN;
    int num = max_token - token;
    head_pos = (head_pos + num - 1) % capacity + 1;
    actions += std::string(num, 'p');
}

void Disk::pass_to_target(const int &target_position)
{

    pre_token = PRE_RESET_TOKEN;
    int cost = get_pass_cost(target_position);
    int actual_cost = std::min(cost, max_token - token);

    token += actual_cost;
    head_pos = (head_pos + actual_cost - 1) % capacity + 1;
    actions.append(actual_cost, 'p'); // 更高效的字符串操作
}

void Disk::so_jump(const int &target_position)
{
    pre_token = PRE_RESET_TOKEN;
    token = max_token;
    head_pos = target_position;
    actions += "j " + std::to_string(target_position);
}

void Disk::so_pass()
{
    pre_token = PRE_RESET_TOKEN;
    token++;
    head_pos = (head_pos % capacity) + 1;
    actions += "p";
}

void Disk::so_read()
{
    pre_token = get_read_cost();
    token += pre_token;
    head_pos = (head_pos % capacity) + 1;
    actions += "r";
}

void Disk::read_actions(std::ostream &os)
{
    if (actions.empty())
    {
        os << "#" << '\n';
    }
    else
    {
        if (actions.find("j ") != std::string::npos)
        {
            os << actions << '\n';
        }
        else
        {
            os << actions << "#" << '\n';
        }
    }
    actions.clear();
}
// bool Disk::max_write_able(int &obj_id, int &size)
// {
//     if (free_blocks < size && obj_id_set.size() == 1 && obj_id_set[0] == obj_id)
//         return true;
//     return false;
// }

bool Disk::current_satisfy(Object &obj, ReadRequest &req)
{
    int order = obj.get_order(id, head_pos);
    // 如果请求完成后也会自动拒绝读取
    if (req.left_cursor <= order && order <= req.right_cursor && order >= 0)
        return true;
    return false;
}

bool Disk::erase_obj(Object &obj)
{
    bool erased = false;

    // 遍历副本时使用引用避免拷贝
    for (const auto &[disk_id, blocks] : obj.replicas)
    { // C++17 结构化绑定
        if (disk_id != id)
            continue;

        // 释放块并验证有效性
        for (int block : blocks)
        {
            if (block < 1 || block > capacity || block_map[block] != obj.id)
            {
                continue; // 跳过无效块或非本对象块
            }
            block_map[block] = 0;
            free_blocks++;
        }

        // 仅删除一次对象ID
        if (!erased)
        {
            erased = obj_id_set.erase(obj.id) > 0;
        }
    }

    return erased;
}

Object::Object()
    : id(0),
      size(0),
      tag(std::make_shared<Tag>()), // 创建默认Tag对象
      replicas{}                    // 显式初始化数组
{
    // 确保replicas中每个元素的first初始化为0
    for (auto &rep : replicas)
    {
        rep.first = 0;
        rep.second.clear();
    }
}

bool Object::has_located(const int &current_disk_id) const
{
    // 若需高频调用，可预先生成磁盘ID集合（在对象构造/更新时维护）
    static std::unordered_set<int> disk_ids;
    if (disk_ids.empty())
    {
        for (const auto &[id, _] : replicas)
        { // 只需提取磁盘ID
            if (id != 0)
                disk_ids.insert(id); // 过滤无效副本
        }
    }
    return disk_ids.count(current_disk_id) > 0;
}

// const std::pair<int, std::vector<int>> &Object::get_best_replica(const std::vector<Disk> &disks) const
// {
//     int min_cost = INT_MAX;
//     size_t best_index = 0;

//     for (size_t i = 0; i < REP_NUM; ++i)
//     {
//         const auto &replica = replicas[i];
//         int disk_id = replica.first;
//         const auto &blocks = replica.second;

//         if (disk_id >= 0 && disk_id < disks.size() && !blocks.empty())
//         {
//             int cost = disks[disk_id].movement_cost(blocks[0]);
//             if (cost < min_cost)
//             {
//                 min_cost = cost;
//                 best_index = i;
//             }
//         }
//     }

//     return replicas[best_index];
// }


int Object::get_order(int &disk_id, int &location) const
{
    for(auto& target: replicas)
    {
        if (disk_id == target.first)
        {
            for (int i = 0; i < target.second.size(); i++)
            {
                if (target.second[i] == location)
                {
                    return i;
                }
            }
        }
    }
    return -1;
}
void Object::clean()
{
    // 使用 erase-remove 惯用法清理过期weak_ptr
    read_requests.erase(
        std::remove_if(read_requests.begin(), read_requests.end(),
                       [](const std::weak_ptr<ReadRequest> &weak_req)
                       {
                           return weak_req.expired(); // 正确检查weak_ptr有效性
                       }),
        read_requests.end());
}

// void Object::clean_block_mappings()
// {
//     for (const auto &[disk_id, blocks] : replicas)
//     {
//         auto &disk_map = block_order_map[disk_id];
//         for (int block : blocks)
//         {
//             disk_map.erase(block);
//         }
//     }
// }

bool StorageSystem::time_left()
{
    for (const auto &disk : disks)
    {
        if (disk.token < disk.max_token) // 使用磁盘自己的 max_token
            return true;
    }
    return false;
}

std::vector<int> StorageSystem::watch_disk()
{
    std::vector<int> result(disk_num + 1, 0); // 索引 0 保留不用
    for (int disk_id = 1; disk_id <= disk_num; ++disk_id)
    {
        result[disk_id] = disks[disk_id].now();
    }
    return result;
}

float StorageSystem::calculate_locality(Disk &disk, int obj_id)
{
    if (!disk.have_obj(obj_id))
        return 0.0f;

    const auto &obj = objects[obj_id];
    float max_score = 0.0f;

    for (const auto &[rep_disk_id, blocks] : obj.replicas)
    {
        if (rep_disk_id != disk.id)
            continue;

        int min_distance = INT_MAX;
        for (int block : blocks)
        {
            int dist = abs(disk.head_pos - block);
            min_distance = std::min(min_distance, dist);
        }
        max_score = std::max(max_score, 1.0f - static_cast<float>(min_distance) / disk.capacity);
    }
    return max_score;
}

void StorageSystem::activate_operations()
{
    while (active_requests.size() < active_limit && !read_queue.empty())
    {
        auto req_ptr = read_queue.top(); // 获取 shared_ptr

        if (!req_ptr)
            continue; // 防止空指针

        const int req_id = req_ptr->req_id;

        // 检查是否已激活
        if (active_requests.find(req_id) != active_requests.end())
        {
            read_queue.pop();
            continue;
        }

        // 初始化 request_extent（直接修改原对象）
        req_ptr->request_extent = std::vector<bool>(objects[req_ptr->obj_id].size, false);

        // 转移所有权到 active_requests
        active_requests.emplace(req_id, std::move(req_ptr));
        read_queue.pop();
    }
}

void StorageSystem::calm_requests()
{
    // 1. 批量转移请求到临时队列
    std::priority_queue<std::shared_ptr<ReadRequest>> temp_queue;
    for (auto &pair : active_requests)
    {
        temp_queue.push(std::move(pair.second));
    }

    // 2. 合并回主队列
    while (!temp_queue.empty())
    {
        read_queue.push(std::move(temp_queue.top()));
        temp_queue.pop();
    }

    // 3. 清空活动请求表
    active_requests.clear();
}

void StorageSystem::update_active_requests()
{
    auto it = active_requests.begin();
    while (it != active_requests.end())
    {
        if (it->second->finished()) // 直接通过 shared_ptr 调用成员函数
        {
            const auto &req = it->second;
            const int req_id = req->req_id;
            const int obj_id = req->obj_id;
            const int occupy_disk_id = req->occupy_id;

            // 清理对象关联的请求
            if (auto obj_it = objects.find(obj_id); obj_it != objects.end())
            {
                auto &requests = obj_it->second.read_requests;
                requests.erase(
                    std::remove_if(requests.begin(), requests.end(),
                                   [req_id](const std::weak_ptr<ReadRequest> &weak_req)
                                   {
                                       if (auto req_ptr = weak_req.lock())
                                       {
                                           return req_ptr->req_id == req_id;
                                       }
                                       return false;
                                   }),
                    requests.end());
                obj_it->second.clean(); // 清理过期 weak_ptr
            }

            // 安全置空磁盘占用指针
            if (occupy_disk_id > 0 && occupy_disk_id <= disk_num) // 增加 disk_num 边界检查
            {
                disks[occupy_disk_id].occupy.reset();
            }

            // 记录完成请求
            finished_requests.push_back(req_id);

            // 安全移除活动请求
            it = active_requests.erase(it);
        }
        else
        {
            ++it;
        }
    }
    activate_operations(); // 补充新请求
}

StorageSystem::StorageSystem(std::istream &is, std::ostream &os)
{

    is >> patience >> tag_num >> disk_num >> disk_size >> max_token;
    // 初始化标签容器
    tags.resize(tag_num + 1); // 索引0保留不用

    // 计算时间片大小
    const int slice_size = the_ceil(static_cast<float>(patience) / FRE_PER_SLICING);

    // 初始化标签数据
    for (int i = 1; i <= tag_num; ++i)
    {
        tags[i].id = i;
        tags[i].slice_size = slice_size;
        tags[i].pre_del.resize(slice_size);
        tags[i].pre_write.resize(slice_size);
        tags[i].pre_read.resize(slice_size);

        // 读取pre_del数据
        for (int j = 0; j < slice_size; ++j)
        {
            is >> tags[i].pre_del[j];
        }

        // 读取pre_write数据
        for (int j = 0; j < slice_size; ++j)
        {
            is >> tags[i].pre_write[j];
        }

        // 读取pre_read数据
        for (int j = 0; j < slice_size; ++j)
        {
            is >> tags[i].pre_read[j];
        }

        tags[i].init(); // 初始化统计值
    }

    // 计算活动请求限制
    active_limit = disk_num * ACITVE_LIMIT_SHIFT;

    // 初始化磁盘
    disks.clear();
    disks.reserve(disk_num + 1);
    disks.emplace_back(0, 0); // 占位磁盘0

    for (int i = 1; i <= disk_num; ++i)
    {
        disks.emplace_back(i, disk_size);
        disks.back().free_blocks = disk_size;
        disks.back().capacity = disk_size;
        disks.back().token = 0;
        disks.back().max_token = max_token; // 明确设置令牌容量
    }

    os << "OK" << std::endl;
}

void StorageSystem::write_object(int obj_id, int size, int tag_id)
{
    // 创建对象
    auto [iter, success] = objects.emplace(obj_id, Object());
    auto &obj = iter->second;
    obj.id = obj_id;
    obj.size = size;
    obj.tag = std::make_shared<Tag>(tags[tag_id]);

    // 候选磁盘预筛选
    std::vector<CandidateWrite> candidates;
    candidates.reserve(disk_num);
    for (int i = 1; i <= disk_num; ++i)
    {
        Disk &disk = disks[i];
        const int target_pos = disk.find_available_position(size);
        if (target_pos == 0)
            continue;

        // 计算评分因子
        const float free_score = static_cast<float>(disk.free_blocks) / disk.capacity;
        const float tag_score = disk.tag_rate(objects, tag_id);
        const float cost_score = 1.0f - (disk.get_pass_cost(target_pos) / disk.capacity);

        const auto &w = WEIGHTS[static_cast<int>(tags[tag_id].state)];

        candidates.push_back({.disk_id = i,
                              .score = w[0] * free_score + w[1] * tag_score + w[2] * cost_score,
                              .write_pos = target_pos});
    }

    // 副本分配
    for (int rep = 0; rep < REP_NUM && !candidates.empty(); rep++)
    {
        auto best = std::max_element(candidates.begin(), candidates.end(),
                                     [](const auto &a, const auto &b)
                                     { return a.score < b.score; });

        if (std::none_of(obj.replicas.begin(), obj.replicas.end(),
                         [&](const auto &r)
                         { return r.first == best->disk_id; }))
        {
            Disk &disk = disks[best->disk_id];

            // 写入磁盘
            auto written = disk.write_blocks(obj.id, best->write_pos, size);
            if (written.size() != size)
            { // 添加写入验证
                continue;
            }

            disk.reg_obj(obj.id);
            obj.replicas[rep] = {best->disk_id, written};

            // // 更新块映射表
            // for (size_t i = 0; i < written.size(); ++i)
            // {
            //     block_order_map[best->disk_id][written[i]] = i;
            // }
        }
        candidates.erase(best);
    }
}

// void StorageSystem::process_reads(std::ostream &os)
// {
//     update_active_requests();

//     for (auto &disk_target : disks)
//     {
//         if (disk_target.occupy == nullptr)
//             continue;

//         auto req_ptr = disk_target.occupy;
//         if (!req_ptr)
//             continue;

//         auto &req = *req_ptr;
//         auto &task_pack = req.fore_task;
//         auto task_type = task_pack.first;

//         if (task_type == ForeTask::Pass)
//         {
//             disk_target.pass_to_target(task_pack.second.second);
//         }
//         else if (task_type == ForeTask::Jump)
//         {
//             disk_target.jump(task_pack.second.second);
//         }

//         disk_target.read_up(objects[req.obj_id], req);
//     }

//     update_active_requests();

//     // 针对磁头，立刻尝试读取
//     bool go_on = true;
//     while (go_on)
//     {
//         go_on = false;
//         // 硬盘匹配请求
//         for (int disk_id = 1; disk_id <= disk_num; ++disk_id)
//         {
//             if (active_requests.empty())
//                 break;
//             auto &disk = disks[disk_id];
//             if (disk.occupy != nullptr)
//                 continue;
//             int cur_pos = disk.head_pos;
//             int obj_id = disk.block_map[cur_pos];
//             if (obj_id == 0)
//                 continue;

//             for (auto &req_pair : active_requests)
//             {
//                 auto &req = req_pair.second;
//                 if (req.obj_id != obj_id)
//                     continue;
//                 // 这里能会尝试读取，只要读取到，就会直接执行
//                 else if (disk.block_map[disk.head_pos] == req.obj_id)
//                 {
//                     // 需要修复读取完成还在读的问题
//                     if (disk.read_up(objects[req.obj_id], req) > 0)
//                         go_on = true;
//                 }
//             }
//         }
//     }

//     update_active_requests();

//     // 利用剩余的token容量
//     for (auto &req_pair : active_requests)
//     {
//         auto &req = req_pair.second;
//         for (int disk_id = 1; disk_id <= disk_num; ++disk_id)
//         {
//             disks[disk_id].answer_request(objects, req);
//         }
//     }

//     update_active_requests();

//     for (int disk_id = 1; disk_id <= disk_num; ++disk_id)
//     {
//         disks[disk_id].read_actions(os);
//     }

//     os << finished_requests.size() << '\n';
//     for (auto req_id : finished_requests)
//     {
//         os << req_id << '\n';
//     }
//     finished_requests.clear();
// }

// void StorageSystem::process_reads(std::ostream &os)
// {
//     update_active_requests();

//     // 阶段1：处理已有占用的磁盘
//     for (auto &disk : disks)
//     {
//         auto req_ptr = disk.occupy.lock();
//         if (req_ptr->finished())
//         {
//             disk.occupy.reset();
//             continue;
//         }
//         const auto &[task_type, params] = req_ptr->fore_task;

//         switch (task_type)
//         {
//         case ForeTask::Pass:
//             disk.pass_to_target(params.second);
//             break;
//         case ForeTask::Jump:
//             disk.jump(params.second);
//             break;
//         default:
//             disk.read_up(objects[req_ptr->obj_id], *req_ptr);
//         }

//         if (req_ptr->finished())
//         {
//             finished_requests.push_back(req_ptr->req_id);
//             active_requests.erase(req_ptr->req_id);
//         }
//     }

//     // 阶段2：改用weak_ptr存储
//     std::unordered_map<int, std::vector<std::weak_ptr<ReadRequest>>> obj_requests;

//     // 填充方式改为：
//     for (const auto &[req_id, req] : active_requests)
//     {
//         if (!req->finished())
//         {
//             obj_requests[req->obj_id].emplace_back(req); // 隐式转为weak_ptr
//         }
//     }

//     // 处理磁盘占用请求需要同步修改Disk类定义（见下文）
//     for (const auto &disk : disks)
//     {
//         auto req_ptr = disk.occupy.lock();
//         if (!req_ptr->finished())
//         {
//             obj_requests[req_ptr->obj_id].push_back(disk.occupy);
//         }
//     }

//     // 阶段3：优化扫描处理
//     bool changed;
//     do
//     {
//         changed = false;
//         for (auto &disk : disks)
//         {
//             // 条件检查具体化
//             const int obj_id = disk.now();
//             if (disk.occupy.lock() ||
//                 disk.token >= disk.max_token ||
//                 obj_id == 0 ||
//                 objects.find(obj_id) == objects.end())
//             {
//                 continue;
//             }

//             // 安全访问请求列表
//             auto it = obj_requests.find(obj_id);
//             if (it == obj_requests.end())
//                 continue;

//             for (auto &weak_req : it->second)
//             {
//                 if (auto req = weak_req.lock())
//                 {
//                     if (req->finished() || req->occupy_id != 0)
//                         continue;

//                     // 执行读取操作
//                     const int read_count = disk.read_up(objects[obj_id], *req);
//                     if (read_count > 0)
//                         changed = true;

//                     // 更新完成状态
//                     if (req->finished())
//                     {
//                         finished_requests.push_back(req->req_id);
//                         active_requests.erase(req->req_id);
//                     }
//                 }
//             }
//         }
//     } while (changed);

//     // 阶段4：最终清理
//     update_active_requests();

//     for (int disk_id = 1; disk_id <= disk_num; ++disk_id)
//     {
//         disks[disk_id].read_actions(os);
//     }

//     os << finished_requests.size() << '\n';
//     for (auto req_id : finished_requests)
//     {
//         os << req_id << '\n';
//     }
//     finished_requests.clear();
// }

std::vector<int> StorageSystem::process_delete(int obj_id)
{
    calm_requests();
    std::vector<int> res;

    if (auto obj_it = objects.find(obj_id); obj_it != objects.end())
    {
        // obj_it->second.clean_block_mappings();
        // 收集有效请求ID
        for (const auto &weak_req : obj_it->second.read_requests)
        {
            if (auto req_ptr = weak_req.lock())
            {
                res.push_back(req_ptr->req_id);
            }
        }

        // 安全移除活动请求
        for (int req_id : res)
        {
            if (auto it = active_requests.find(req_id);
                it != active_requests.end() && it->second->obj_id == obj_id)
            {
                // 清理磁盘占用
                if (int disk_id = it->second->occupy_id;
                    disk_id > 0 && disk_id <= disk_num) // 明确范围
                {
                    disks[disk_id].occupy.reset();
                }
                active_requests.erase(it);
            }
        }

        // 重建优先队列（修复类型问题）
        using RequestPtr = std::shared_ptr<ReadRequest>;
        std::priority_queue<RequestPtr> new_queue;
        std::priority_queue<RequestPtr> temp;

        std::swap(read_queue, temp);
        while (!temp.empty())
        {
            auto req = temp.top();
            temp.pop();
            if (req->obj_id != obj_id)
            {
                new_queue.push(std::move(req));
            }
        }
        read_queue = std::move(new_queue);

        // 清理磁盘状态
        for (auto &disk : disks)
        {
            disk.obj_id_set.erase(obj_id);
            // 安全访问weak_ptr
            if (auto req_ptr = disk.occupy.lock();
                req_ptr && req_ptr->obj_id == obj_id)
            {
                disk.occupy.reset();
            }
        }

        objects.erase(obj_it);
    }
    return res;
}

void StorageSystem::process_delete(int obj_id, std::ostream &os)
{
    calm_requests();

    int removed_count = 0;
    std::vector<int> related_requests;

    // 安全查找对象
    if (auto it = objects.find(obj_id); it != objects.end())
    {
        auto &obj = it->second;

        // 阶段1: 收集相关请求ID
        for (auto &weak_req : obj.read_requests)
        {
            if (auto req = weak_req.lock())
            {
                related_requests.push_back(req->req_id);
            }
        }

        // 阶段2: 清理活动请求
        for (auto it = active_requests.begin(); it != active_requests.end();)
        {
            if (it->second->obj_id == obj_id)
            {
                // 清理磁盘占用
                if (int disk_id = it->second->occupy_id;
                    disk_id > 0 && disk_id <= disk_num)
                {
                    disks[disk_id].occupy.reset();
                }
                it = active_requests.erase(it);
                ++removed_count;
            }
            else
            {
                ++it;
            }
        }

        // 阶段3: 重建优先队列（修复类型问题）
        using RequestPtr = std::shared_ptr<ReadRequest>;
        std::priority_queue<RequestPtr> new_queue;
        std::priority_queue<RequestPtr> temp;

        std::swap(read_queue, temp);
        while (!temp.empty())
        {
            auto req = temp.top();
            temp.pop();
            if (req->obj_id != obj_id)
            {
                new_queue.push(req);
            }
            else
            {
                ++removed_count;
            }
        }
        read_queue = std::move(new_queue);

        // 阶段4: 清理磁盘状态
        for (auto &disk : disks)
        {
            disk.obj_id_set.erase(obj_id);
            // 安全访问weak_ptr
            if (auto req_ptr = disk.occupy.lock();
                req_ptr && req_ptr->obj_id == obj_id)
            {
                disk.occupy.reset();
            }
        }

        objects.erase(it);
    }

    // 结构化输出
    os << obj_id << "\n"
       << removed_count;
    for (auto id : related_requests)
    {
        os << " " << id;
    }
    os << "\n";
}

void StorageSystem::act_time(std::istream &is, std::ostream &os)
{
    for (int i = 1; i <= disk_num; ++i)
    {
        disks[i].token = 0;
    }
    is >> current >> stamp;
    os << current << ' ' << stamp << '\n';
}

void StorageSystem::act_write(std::istream &is, std::ostream &os)
{
    int write_request_num;
    is >> write_request_num;

    // 预声明局部变量减少重复构造
    int current_obj_id, current_obj_size, current_obj_tag;

    for (int i = 0; i < write_request_num; ++i)
    {
        // 批量读取参数
        is >> current_obj_id >> current_obj_size >> current_obj_tag;
        write_object(current_obj_id, current_obj_size, current_obj_tag);

        // 使用对象引用避免多次查找map
        auto &obj = objects[current_obj_id]; // 关键优化点

        os << current_obj_id << std::endl; // 换行符优化

        // 遍历副本时使用引用
        for (int rep_id = 0; rep_id < REP_NUM; ++rep_id)
        {
            // 获取副本引用
            const auto &replica = obj.replicas[rep_id]; // 关键优化点

            os << replica.first; // 磁盘ID

            // 使用范围循环避免迭代器操作
            for (const int block : replica.second) // 范围for优化
            {
                os << ' ' << block;
            }
            os << '\n'; // 换行符优化
        }
    }
}

// 修改后的act_read函数
void StorageSystem::act_read(std::istream &is, std::ostream &os)
{
    // [1] 接收新请求
    int read_request_num;
    is >> read_request_num;
    for (int i = 0; i < read_request_num; ++i)
    {
        int req_id, obj_id;
        is >> req_id >> obj_id;

        auto req_ptr = std::make_shared<ReadRequest>();
        const int obj_size = objects[obj_id].size;
        req_ptr->init(req_id, obj_id, stamp, obj_size);

        objects[obj_id].read_requests.emplace_back(req_ptr);
        read_queue.emplace(req_ptr);
    }

    update_active_requests();

    std::vector<int> disk_ids{};

    // [2] 处理遗留的预置任务
    for (auto &req_pair : active_requests)
    {
        auto &req = *req_pair.second;
        auto &[task_type, params] = req.fore_task;

        if (task_type == ForeTask::Empty)
        {
            continue;
        }

        Disk &disk = disks[params.first];
        if (task_type == ForeTask::Pass)
        {
            disk.pass_to_target(params.second);
        }
        else if (task_type == ForeTask::Jump)
        {
            disk.jump(params.second);
        }
        disk.read_up(objects[req.obj_id], req);
        req.reset_task();
    }

    update_active_requests();

    // [3] 磁头即时读取
    bool go_on = true;
    while (go_on && time_left())
    {
        go_on = false;

        for (int disk_id = 1; disk_id <= disk_num; ++disk_id)
        {
            Disk &disk = disks[disk_id];
            const int obj_id = disk.now();

            // 跳过无效对象和已被占据的磁盘
            if (obj_id == 0 || !disk.occupy.expired())
            {
                continue;
            }
            // 记录可用盘
            disk_ids.push_back(disk_id);
            // 查找关联请求
            for (auto &req_pair : active_requests)
            {
                auto &req = *req_pair.second;

                // 新增三个过滤条件
                if (req.obj_id != obj_id || // 对象不匹配
                    req.finished() ||       // 请求已完成
                    req.occupy_id != 0)
                { // 请求已被其他磁盘占据
                    continue;
                }

                // 执行连续读取
                if (disk.read_up(objects[obj_id], req) > 0)
                {
                    go_on = true;

                    // 设置请求的占据标记（已在read_up中设置）
                    // req.occupy_id = disk.id;
                }
            }
        }
    }
    update_active_requests();
    // [4] 利用剩余令牌处理请求
    for (auto &req_pair : active_requests)
    {
        auto &req = *req_pair.second;
        if (req.finished())
            continue;

        for (auto &disk_id : disk_ids)
        {
            Disk &disk = disks[disk_id];
            if (disk.have_obj(req.obj_id))
            {
                disk.answer_request(objects, req);
                if (req.finished())
                    break;
            }
        }
    }
    update_active_requests();

    // [5] 输出结果
    for (int disk_id = 1; disk_id <= disk_num; ++disk_id)
    {
        disks[disk_id].read_actions(os);
    }
    os << finished_requests.size() << '\n';
    for (auto req_id : finished_requests)
    {
        os << req_id << '\n';
    }
    finished_requests.clear();
}

void StorageSystem::act_delete(std::istream &is, std::ostream &os)
{
    int delete_count;
    is >> delete_count;

    std::vector<int> res;
    res.reserve(delete_count * 10); // 预分配空间优化

    for (int i = 0; i < delete_count; ++i)
    {
        int delete_obj_id;
        is >> delete_obj_id;

        // 新增对象存在性检查
        if (objects.find(delete_obj_id) == objects.end())
        {
            continue;
        }

        auto temp = process_delete(delete_obj_id);
        if (!temp.empty())
        {
            res.insert(res.end(), temp.begin(), temp.end());
        }
        objects.erase(delete_obj_id);
    }
    os << res.size() << '\n';
    for (auto &temp : res)
    {
        os << temp << '\n';
    }
}

int main()
{
    StorageSystem sys(std::cin, std::cout);

    for (int current = 1; current <= sys.patience + MAX_DEADLINE; ++current)
    {
        sys.act_time(std::cin, std::cout);
        sys.act_delete(std::cin, std::cout);
        sys.act_write(std::cin, std::cout);
        sys.act_read(std::cin, std::cout);
        std::cout.flush();
    }
    return 0;
}