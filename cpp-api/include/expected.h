#pragma once

#include <initializer_list>
#include <type_traits>

namespace scalaris {

  // p0323r4

  struct in_place_t {};

  // �.�.6, unexpect tag
  struct unexpect_t {
    unexpect_t() = default;
  };
  inline constexpr unexpect_t unexpect{};

  template <class E> class unexpected {
  public:
    unexpected() = delete;
    constexpr explicit unexpected(const E& val) : val(val) {}
    constexpr explicit unexpected(E&&);
    constexpr const E& value() const&;
    constexpr E& value() &;
    constexpr E&& value() &&;
    constexpr E const&& value() const&&;

  private:
    E val; // exposition only
  };

  template <class T, class E> class expected {
  public:
    typedef T value_type;
    typedef E error_type;
    typedef unexpected<E> unexpected_type;

    template <class U> struct rebind { using type = expected<U, error_type>; };

    // �.�.4.1, constructors
    constexpr expected();
    constexpr expected(const expected&);
    constexpr expected(expected&&) noexcept(std::is_nothrow_move_constructible_v<T>);
    template <class U, class G>
    explicit constexpr expected(const expected<U, G>&);
    template <class U, class G> explicit constexpr expected(expected<U, G>&&);

    template <class U = T> explicit constexpr expected(U&& v);

    template <class... Args> constexpr explicit expected(in_place_t, Args&&...);
    template <class U, class... Args>
      constexpr explicit expected(in_place_t, std::initializer_list<U>, Args&&...);
    template <class G = E> constexpr expected(unexpected<G> const&);
    template <class G = E> constexpr expected(unexpected<G>&&);
    template <class... Args> constexpr explicit expected(unexpect_t, Args&&...);
    template <class U, class... Args>
      constexpr explicit expected(unexpect_t, std::initializer_list<U>, Args&&...);

    // �.�.4.2, destructor
    ~expected();

    // �.�.4.3, assignment
    expected& operator=(const expected&);
    expected& operator=(expected&&) noexcept(std::is_nothrow_move_constructible_v<T>);
    template <class U = T> expected& operator=(U&&);
    template <class G = E> expected& operator=(const unexpected<G>&);
    template <class G = E>
      expected& operator=(unexpected<G>&&) noexcept(std::is_nothrow_move_constructible_v<T>);

    template <class... Args> void emplace(Args&&...);
    template <class U, class... Args>
      void emplace(std::initializer_list<U>, Args&&...);

    // �.�.4.4, swap
    void swap(expected&) noexcept(std::is_nothrow_move_constructible_v<T>);

    // �.�.4.5, observers
    constexpr const T* operator->() const;
    constexpr T* operator->();
    constexpr const T& operator*() const&;
    constexpr T& operator*() &;
    constexpr const T&& operator*() const&&;
    constexpr T&& operator*() &&;
    constexpr explicit operator bool() const noexcept;
    constexpr bool has_value() const noexcept;
    constexpr const T& value() const&;
    constexpr T& value() &;
    constexpr const T&& value() const&&;
    constexpr T&& value() &&;
    constexpr const E& error() const&;
    constexpr E& error() &;
    constexpr const E&& error() const&&;
    constexpr E&& error() &&;
    template <class U> constexpr T value_or(U&&) const&;
    template <class U> T value_or(U&&) &&;

  private:
    bool has_val; // exposition only
    union {
      value_type val;           // exposition only
      unexpected_type unexpect; // exposition only
    };
  };
} // namespace scalaris
