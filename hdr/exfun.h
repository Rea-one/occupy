#include <vector>
#include <type_traits>

class ExFun
{
private:
    size_t _process_extent_ = 0; // 使用局部变量避免状态残留
    size_t _process_ceiling_ = 0;

public:
    // 基础版本：处理单个元素
    template <typename T>
    void act(const T &elem)
    {
        // 实现具体元素处理逻辑，例如输出或计算
        static_assert(!std::is_same_v<T, T>, "Unimplemented act() for this type");
    }

    // 特化版本：处理 std::vector
    template <typename T>
    void act(const std::vector<T> &container)
    {
        _process_ceiling_ = container.size();
        for (_process_extent_ = 0; _process_extent_ < _process_ceiling_; ++_process_extent_)
        {
            act(container[_process_extent_]); // 递归调用基础版本
        }
    }

    // 可选：支持其他容器（如 std::list）
    template <template <typename...> class Container, typename T>
    void act(const Container<T> &container)
    {
        _process_ceiling_ = container.size();
        _process_extent_ = 0;
        for (const auto &elem : container)
        {
            act(elem);
            ++_process_extent_;
        }
    }
};
