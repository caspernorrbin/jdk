/*
 * Copyright (c) 2025, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 *
 */

#ifndef SHARE_UTILITIES_RBTREE_HPP
#define SHARE_UTILITIES_RBTREE_HPP

#include "nmt/memTag.hpp"
#include "runtime/os.hpp"
#include "utilities/globalDefinitions.hpp"
#include "utilities/growableArray.hpp"
#include <type_traits>

struct Empty {};
class RBTreeNoopAllocator;

// COMPARATOR must have a static function `cmp(a,b)` which returns:
//     - an int < 0 when a < b
//     - an int == 0 when a == b
//     - an int > 0 when a > b
// ALLOCATOR must check for oom and exit, as RBTree currently does not handle the
// allocation failing.
// Key needs to be of a type that is trivially destructible.
// The tree will call a value's destructor when its node is removed.
// Nodes are address stable and will not change during its lifetime.
template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
class RBTree {
  friend class RBTreeTest;

private:
  ALLOCATOR _allocator;
  size_t _num_nodes;

public:
  class RBNode {
    friend RBTree;
    friend class RBTreeTest;

  private:
    RBNode* _parent;
    RBNode* _left;
    RBNode* _right;

    const K _key;
    V _value;

    enum Color : uint8_t { BLACK, RED };
    Color _color;

  public:
    const K& key() const { return _key; }
    V& val() { return _value; }

    RBNode(const K& k, const V& v)
        : _parent(nullptr), _left(nullptr), _right(nullptr),
          _key(k), _value(v), _color(RED) {}
      RBNode(const K& k)
        : _parent(nullptr), _left(nullptr), _right(nullptr),
          _key(k), _value(Empty()), _color(RED) {}

  private:
    bool is_black() const { return _color == BLACK; }
    bool is_red() const { return _color == RED; }

    void set_black() { _color = BLACK; }
    void set_red() { _color = RED; }


    bool is_right_child() const {
      return _parent != nullptr && _parent->_right == this;
    }

    bool is_left_child() const {
      return _parent != nullptr && _parent->_left == this;
    }

    void replace_child(RBNode* old_child, RBNode* new_child);

    // Move node down to the left, and right child up
    RBNode* rotate_left();

    // Move node down to the right, and left child up
    RBNode* rotate_right();

    RBNode* prev();

    RBNode* next();

  #ifdef ASSERT
    bool is_correct(unsigned int num_blacks, unsigned int maximum_depth, unsigned int current_depth, RBNode* first) const;
    size_t count_nodes() const;
  #endif // ASSERT
  };

  // Represents the location of a (would be) node in the tree
  // If a cursor is valid (valid() == true) it points somewhere in the tree.
  // If the cursor points to an existing node (found() == true), node() can be used to access that node,
  // Otherwise nullptr is returned, regardless if the node is valid or not.
  class Cursor {
    friend RBTree<K, V, COMPARATOR, ALLOCATOR>;
    RBNode** _insert_location;
    RBNode* _parent;
    Cursor() : _insert_location(nullptr), _parent(nullptr) {}
    Cursor(RBNode** insert_location, RBNode* parent) : _insert_location(insert_location), _parent(parent) {}

  public:
    bool valid() { return _insert_location != nullptr; }
    bool found() { return *_insert_location != nullptr; }
    RBNode* node() { return _insert_location == nullptr ? nullptr : *_insert_location; }
    const RBNode* node() const { return _insert_location == nullptr ? nullptr : *_insert_location; }
  };

private:
  RBNode* _root;
  RBNode* _first;

  RBNode* allocate_node(const K& k, const V& v) {
    void* node_place = _allocator.allocate(sizeof(RBNode));
    assert(node_place != nullptr, "rb-tree allocator must exit on failure");
    return new (node_place) RBNode(k, v);
  }

  void free_node(RBNode* node) {
    node->_value.~V();
    _allocator.free(node);
  }

  static inline bool is_black(RBNode* node) {
    return node == nullptr || node->is_black();
  }

  static inline bool is_red(RBNode* node) {
    return node != nullptr && node->is_red();
  }

  void fix_insert_violations(RBNode* node);

  void remove_black_leaf(RBNode* node);

  // Assumption: node has at most one child. Two children is handled in `remove_at_cursor()`
  void remove_from_tree(RBNode* node);

public:
  NONCOPYABLE(RBTree);

  RBTree() : _allocator(), _num_nodes(0), _root(nullptr), _first(nullptr) {
    static_assert(std::is_trivially_destructible<K>::value, "key type must be trivially destructable");
  }
  ~RBTree() { if (!std::is_same<ALLOCATOR, RBTreeNoopAllocator>::value) this->remove_all(); }

  size_t size() { return _num_nodes; }
  RBNode* first() { return _first; }

  // Gets the cursor to the given node.
  Cursor get_cursor(RBNode* node);

  // Moves to the next valid node.
  // If no next node exist, the cursor becomes invalid.
  Cursor next(Cursor& cursor);

  // Moves to the previous valid node.
  // If no previous node exist, the cursor becomes invalid.
  Cursor prev(Cursor& cursor);

  // Finds the cursor to the node associated with the given key.
  Cursor cursor_find(const K& key);

  // Inserts the given node at the cursor location
  // The cursor must not point to an existing node
  void insert_at_cursor(RBNode* node, Cursor& cursor);

  // Removes the node referenced by the cursor
  // The cursor must point to a valid existing node
  void remove_at_cursor(Cursor& cursor);

  // Replace the node referenced by the cursor with a new node
  // The cursor must point to a valid existing node
  // The old is be destroyed
  // The user must ensure that no tree properties are broken:
  // There must not exist any node with the same key
  // For all nodes with key < old_node, must also have key < new_node
  // For all nodes with key > old_node, must also have key > new_node
  void replace_at_cursor(RBNode *new_node, Cursor &cursor);

  // Finds the value of the node associated with the given key.
  V* find(const K& key) {
    Cursor cursor = cursor_find(key);
    return cursor.found() ? &cursor.node()->_value : nullptr;
  }

  // Finds the node associated with the given key.
  RBNode* find_node(const K& key) {
    Cursor cursor = cursor_find(key);
    return cursor.node();
  }

  // Inserts a node with the given k/v into the tree,
  // if the key already exist, the value is updated instead.
  void upsert(const K& k, const V& v) {
    Cursor cursor = cursor_find(k);
    RBNode* node = cursor.node();
    if (node != nullptr) {
      node->_value = v;
      return;
    }

    node = allocate_node(k, v);
    insert_at_cursor(node, cursor);
  }

  // Removes the node with the given key from the tree if it exists.
  // Returns true if the node was successfully removed, false otherwise.
  bool remove(const K& k) {
    Cursor cursor = cursor_find(k);
    if (!cursor.found()) {
      return false;
    }
    RBNode* node = cursor.node();
    remove_at_cursor(cursor);
    free_node(node);
    return true;
  }

  void remove(RBNode* node) {
    Cursor cursor = get_cursor(node);
    remove_at_cursor(cursor);
    free_node(node);
  }

  // Removes all existing nodes from the tree.
  void remove_all() {
    GrowableArrayCHeap<RBNode*, mtInternal> to_delete(2 * log2i(_num_nodes + 1));
    to_delete.push(_root);

    while (!to_delete.is_empty()) {
      RBNode* head = to_delete.pop();
      if (head == nullptr) continue;
      to_delete.push(head->_left);
      to_delete.push(head->_right);
      free_node(head);
    }
    _num_nodes = 0;
    _root = nullptr;
    _first = nullptr;
  }

  // Finds the node with the closest key <= the given key
  RBNode* closest_leq(const K& key) {
    Cursor cursor = cursor_find(key);
    return cursor.found() ? cursor.node() : prev(cursor).node();
  }

  // Finds the node with the closest key > the given key
  RBNode* closest_gt(const K& key) {
    Cursor cursor = cursor_find(key);
    return cursor.found() ? cursor.node()->next() : cursor._parent;
  }

  // Visit all RBNodes in ascending order, calling f on each node.
  template <typename F>
  void visit_in_order(F f);

  // Visit all RBNodes in ascending order whose keys are in range [from, to), calling f on each node.
  template <typename F>
  void visit_range_in_order(const K& from, const K& to, F f);

#ifdef ASSERT
  // Verifies that the tree is correct and holds rb-properties
  void verify_self();
#endif // ASSERT

};

template <MemTag mt>
class RBTreeCHeapAllocator {
public:
  void* allocate(size_t sz) {
    void* allocation = os::malloc(sz, mt);
    if (allocation == nullptr) {
      vm_exit_out_of_memory(sz, OOM_MALLOC_ERROR,
                            "red-black tree failed allocation");
    }
    return allocation;
  }

  void free(void* ptr) { os::free(ptr); }
};

class RBTreeNoopAllocator {
public:
  void* allocate(size_t sz) {
    assert(false, "intrusive tree should not use rbtree allocator");
    return nullptr;
  }

  void free(void* ptr) {
    assert(false, "intrusive tree should not use rbtree allocator");
  }
};

template <typename K, typename V, typename COMPARATOR, MemTag mt>
using RBTreeCHeap = RBTree<K, V, COMPARATOR, RBTreeCHeapAllocator<mt>>;

template <typename K, typename COMPARATOR>
using IntrusiveRBTree = RBTree<K, Empty, COMPARATOR, RBTreeNoopAllocator>;

#endif // SHARE_UTILITIES_RBTREE_HPP
