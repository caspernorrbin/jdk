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

#ifndef SHARE_UTILITIES_RBTREE_INLINE_HPP
#define SHARE_UTILITIES_RBTREE_INLINE_HPP

#include "utilities/debug.hpp"
#include "utilities/powerOfTwo.hpp"
#include "utilities/rbTree.hpp"

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode::replace_child(
    RBNode* old_child, RBNode* new_child) {
  if (_left == old_child) {
    _left = new_child;
  } else if (_right == old_child) {
    _right = new_child;
  }
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline typename RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode*
RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode::rotate_left() {
  // Move node down to the left, and right child up
  RBNode* old_right = _right;

  _right = old_right->_left;
  if (old_right->_left != nullptr) {
    old_right->_left->_parent = this;
  }

  old_right->_parent = _parent;
  if (is_left_child()) {
    _parent->_left = old_right;
  } else if (is_right_child()) {
    _parent->_right = old_right;
  }

  old_right->_left = this;
  _parent = old_right;

  return old_right;
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline typename RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode*
RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode::rotate_right() {
  // Move node down to the right, and left child up
  RBNode* old_left = _left;

  _left = old_left->_right;
  if (old_left->_right != nullptr) {
    old_left->_right->_parent = this;
  }

  old_left->_parent = _parent;
  if (is_left_child()) {
    _parent->_left = old_left;
  } else if (is_right_child()) {
    _parent->_right = old_left;
  }

  old_left->_right = this;
  _parent = old_left;

  return old_left;
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline typename RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode*
RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode::prev() {
  RBNode* node = this;
  if (_left != nullptr) { // right subtree exists
    node = _left;
    while (node->_right != nullptr) {
      node = node->_right;
    }
    return node;
  }

  while (node != nullptr && node->is_left_child()) {
    node = node->_parent;
  }
  return node->_parent;
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline typename RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode*
RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode::next() {
  RBNode* node = this;
  if (_right != nullptr) { // right subtree exists
    node = _right;
    while (node->_left != nullptr) {
      node = node->_left;
    }
    return node;
  }

  while (node != nullptr && node->is_right_child()) {
    node = node->_parent;
  }
  return node->_parent;
}

#ifdef ASSERT
template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline bool RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode::is_correct(
  unsigned int num_blacks, unsigned int maximum_depth, unsigned int current_depth, RBNode* first) const {
  if (current_depth > maximum_depth) {
    return false;
  }

  if (this != first && COMPARATOR::cmp(first->key(), _key) > 0) {
    return false;
  }

  if (is_black()) {
    num_blacks--;
  }

  bool left_is_correct = num_blacks == 0;
  bool right_is_correct = num_blacks == 0;
  if (_left != nullptr) {
    if (COMPARATOR::cmp(_left->key(), _key) >= 0 || // left >= root, or
        (is_red() && _left->is_red()) ||            // 2 red nodes, or
        (_left->_parent != this)) {                 // Pointer mismatch,
      return false;                                 // all incorrect.
    }
    left_is_correct = _left->is_correct(num_blacks, maximum_depth, current_depth++, first);
  }
  if (_right != nullptr) {
    if (COMPARATOR::cmp(_right->key(), _key) <= 0 || // right <= root, or
        (is_red() && _left->is_red()) ||             // 2 red nodes, or
        (_right->_parent != this)) {                 // Pointer mismatch,
      return false;                                  // all incorrect.
    }
    right_is_correct = _right->is_correct(num_blacks, maximum_depth, current_depth++, first);
  }
  return left_is_correct && right_is_correct;
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline size_t RBTree<K, V, COMPARATOR, ALLOCATOR>::RBNode::count_nodes() const {
  size_t left_nodes = _left == nullptr ? 0 : _left->count_nodes();
  size_t right_nodes = _right == nullptr ? 0 : _right->count_nodes();
  return 1 + left_nodes + right_nodes;
}

#endif // ASSERT

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline typename RBTree<K, V, COMPARATOR, ALLOCATOR>::Cursor
RBTree<K, V, COMPARATOR, ALLOCATOR>::get_cursor(RBNode* node) {
  if (node == nullptr) {
    return Cursor();
  }

  if (node->_parent == nullptr) {
    return Cursor(&_root, nullptr);
  }

  RBNode* parent = node->_parent;
  RBNode** insert_location =
      node->is_left_child() ? &parent->_left : &parent->_right;
  return Cursor(insert_location, parent);
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline typename RBTree<K, V, COMPARATOR, ALLOCATOR>::Cursor
RBTree<K, V, COMPARATOR, ALLOCATOR>::next(Cursor& cursor) {
  if (cursor.found()) {
    return get_cursor(cursor.node()->next());
  }

  if (cursor._parent == nullptr) { // Tree is empty
    return Cursor();
  }

  // Pointing to non-existant node
  if (&cursor._parent->_left == cursor._insert_location) { // Left child, parent is next
    return get_cursor(cursor._parent);
  }

  return get_cursor(cursor._parent->next()); // Right child, parent's next is also node's next
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline typename RBTree<K, V, COMPARATOR, ALLOCATOR>::Cursor
RBTree<K, V, COMPARATOR, ALLOCATOR>::prev(Cursor& cursor) {
  if (cursor.found()) {
    return get_cursor(cursor.node()->prev());
  }

  if (cursor._parent == nullptr) { // Tree is empty
    return Cursor();
  }

  // Pointing to non-existant node
  if (&cursor._parent->_right == cursor._insert_location) { // Right child, parent is prev
    return get_cursor(cursor._parent);
  }

  return get_cursor(cursor._parent->prev()); // Left child, parent's prev is also node's prev
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline typename RBTree<K, V, COMPARATOR, ALLOCATOR>::Cursor
RBTree<K, V, COMPARATOR, ALLOCATOR>::cursor_find(const K& key) {
  RBNode* parent = nullptr;
  RBNode** insert_location = &_root;
  while (*insert_location != nullptr) {
    const int key_cmp_k = COMPARATOR::cmp(key, (*insert_location)->key());

    if (key_cmp_k == 0) {
      break;
    }

    parent = *insert_location;
    if (key_cmp_k < 0) {
      insert_location = &((*insert_location)->_left);
    } else {
      insert_location = &((*insert_location)->_right);
    }
  }
  return Cursor(insert_location, parent);
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::insert_at_cursor(RBNode* node, Cursor& cursor) {
  assert(cursor.valid() && !cursor.found(), "must be");
  _num_nodes++;

  if (_first == nullptr || cursor._insert_location == &_first->_left) {
    _first = node;
  }

  node->_parent = cursor._parent;
  *cursor._insert_location = node;

  if (cursor._parent == nullptr) {
    return;
  }

  fix_insert_violations(node);
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::remove_at_cursor(Cursor& cursor) {
  assert(cursor.valid() && cursor.found(), "must be");
  _num_nodes--;

  RBNode* node = cursor.node();
  if (node == _first) {
    _first = node->next();
  }

  if (node->_left != nullptr && node->_right != nullptr) { // node has two children
    // Swap place with the in-order successor and delete there instead
    RBNode* curr = node->_right;
    while (curr->_left != nullptr) {
      curr = curr->_left;
    }

    if (_root == node)
      _root = curr;

    std::swap(curr->_left, node->_left);
    std::swap(curr->_color, node->_color);

    // If node is curr's parent, swapping right/parent severs the node connection
    if (node->_right == curr) {
      node->_right = curr->_right;
      curr->_parent = node->_parent;
      node->_parent = curr;
      curr->_right = node;
    } else {
      std::swap(curr->_right, node->_right);
      std::swap(curr->_parent, node->_parent);
      node->_parent->replace_child(curr, node);
      curr->_right->_parent = curr;
    }

    if (curr->_parent != nullptr) curr->_parent->replace_child(node, curr);
    curr->_left->_parent = curr;

    if (node->_left != nullptr) node->_left->_parent = node;
    if (node->_right != nullptr) node->_right->_parent = node;
  }

  remove_from_tree(node);
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::replace_at_cursor(RBNode* new_node, Cursor& cursor) {
  assert(cursor.valid() && cursor.found(), "must be");
  RBNode* old_node = cursor.node();
  if (old_node == new_node) {
    return;
  }

  *cursor._insert_location = new_node;
  new_node->_parent = cursor._parent;
  new_node->_color = old_node->_color;

  new_node->_left = old_node->_left;
  new_node->_right = old_node->_right;
  if (new_node->_left != nullptr) {
    new_node->_left->_parent = new_node;
  } else if (new_node->_right != nullptr) {
    new_node->_right->_parent = new_node;
  }

  if (old_node == _first) {
    _first = new_node;
  }

  free_node(old_node);

#ifdef ASSERT
  verify_self(); // Dangerous operation, should verify no tree properties were broken
#endif // ASSERT
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::fix_insert_violations(RBNode* node) {
  if (node->is_black()) { // node's value was updated
    return;               // Tree is already correct
  }

  RBNode* parent = node->_parent;
  while (parent != nullptr && parent->is_red()) {
    // Node and parent are both red, creating a red-violation

    RBNode* grandparent = parent->_parent;
    if (grandparent == nullptr) { // Parent is the tree root
      assert(parent == _root, "parent must be root");
      parent->set_black(); // Color parent black to eliminate the red-violation
      return;
    }

    RBNode* uncle = parent->is_left_child() ? grandparent->_right : grandparent->_left;
    if (is_black(uncle)) { // Parent is red, uncle is black
      // Rotate the parent to the position of the grandparent
      if (parent->is_left_child()) {
        if (node->is_right_child()) { // Node is an "inner" node
          // Rotate and swap node and parent to make it an "outer" node
          parent->rotate_left();
          parent = node;
        }
        grandparent->rotate_right(); // Rotate the parent to the position of the grandparent
      } else if (parent->is_right_child()) {
        if (node->is_left_child()) { // Node is an "inner" node
          // Rotate and swap node and parent to make it an "outer" node
          parent->rotate_right();
          parent = node;
        }
        grandparent->rotate_left(); // Rotate the parent to the position of the grandparent
      }

      // Swap parent and grandparent colors to eliminate the red-violation
      parent->set_black();
      grandparent->set_red();

      if (_root == grandparent) {
        _root = parent;
      }

      return;
    }

    // Parent and uncle are both red
    // Paint both black, paint grandparent red to not create a black-violation
    parent->set_black();
    uncle->set_black();
    grandparent->set_red();

    // Move up two levels to check for new potential red-violation
    node = grandparent;
    parent = grandparent->_parent;
  }
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::remove_black_leaf(RBNode* node) {
  // Black node removed, balancing needed
  RBNode* parent = node->_parent;
  while (parent != nullptr) {
    // Sibling must exist. If it did not, node would need to be red to not break
    // tree properties, and could be trivially removed before reaching here
    RBNode* sibling = node->is_left_child() ? parent->_right : parent->_left;
    if (is_red(sibling)) { // Sibling red, parent and nephews must be black
      assert(is_black(parent), "parent must be black");
      assert(is_black(sibling->_left), "nephew must be black");
      assert(is_black(sibling->_right), "nephew must be black");
      // Swap parent and sibling colors
      parent->set_red();
      sibling->set_black();

      // Rotate parent down and sibling up
      if (node->is_left_child()) {
        parent->rotate_left();
        sibling = parent->_right;
      } else {
        parent->rotate_right();
        sibling = parent->_left;
      }

      if (_root == parent) {
        _root = parent->_parent;
      }
      // Further balancing needed
    }

    RBNode* close_nephew = node->is_left_child() ? sibling->_left : sibling->_right;
    RBNode* distant_nephew = node->is_left_child() ? sibling->_right : sibling->_left;
    if (is_red(distant_nephew) || is_red(close_nephew)) {
      if (is_black(distant_nephew)) { // close red, distant black
        // Rotate sibling down and inner nephew up
        if (node->is_left_child()) {
          sibling->rotate_right();
        } else {
          sibling->rotate_left();
        }

        distant_nephew = sibling;
        sibling = close_nephew;

        distant_nephew->set_red();
        sibling->set_black();
      }

      // Distant nephew red
      // Rotate parent down and sibling up
      if (node->is_left_child()) {
        parent->rotate_left();
      } else {
        parent->rotate_right();
      }
      if (_root == parent) {
        _root = sibling;
      }

      // Swap parent and sibling colors
      if (parent->is_black()) {
        sibling->set_black();
      } else {
        sibling->set_red();
      }
      parent->set_black();

      // Color distant nephew black to restore black balance
      distant_nephew->set_black();
      return;
    }

    if (is_red(parent)) { // parent red, sibling and nephews black
      // Swap parent and sibling colors to restore black balance
      sibling->set_red();
      parent->set_black();
      return;
    }

    // Parent, sibling, and both nephews black
    // Color sibling red and move up one level
    sibling->set_red();
    node = parent;
    parent = node->_parent;
  }
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::remove_from_tree(RBNode* node) {
  RBNode* parent = node->_parent;
  RBNode* left = node->_left;
  RBNode* right = node->_right;
  if (left != nullptr) { // node has a left only-child
    // node must be black, and child red, otherwise a black-violation would
    // exist Remove node and color the child black.
    assert(right == nullptr, "right must be nullptr");
    assert(is_black(node), "node must be black");
    assert(is_red(left), "child must be red");
    left->set_black();
    left->_parent = parent;
    if (parent == nullptr) {
      assert(node == _root, "node must be root");
      _root = left;
    } else {
      parent->replace_child(node, left);
    }
  } else if (right != nullptr) { // node has a right only-child
    // node must be black, and child red, otherwise a black-violation would
    // exist Remove node and color the child black.
    assert(left == nullptr, "left must be nullptr");
    assert(is_black(node), "node must be black");
    assert(is_red(right), "child must be red");
    right->set_black();
    right->_parent = parent;
    if (parent == nullptr) {
      assert(node == _root, "node must be root");
      _root = right;
    } else {
      parent->replace_child(node, right);
    }
  } else {               // node has no children
    if (node == _root) { // Tree empty
      _root = nullptr;
    } else {
      if (is_black(node)) {
        // Removed node is black, creating a black imbalance
        remove_black_leaf(node);
      }
      parent->replace_child(node, nullptr);
    }
  }
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
template <typename F>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::visit_in_order(F f) {
  RBNode* node = _first;
  while (node != nullptr) {
    f(node);
    node = node->next();
  }
}

template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
template <typename F>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::visit_range_in_order(const K& from, const K& to, F f) {
  assert(COMPARATOR::cmp(from, to) <= 0, "from must be less or equal to to");
  if (_root == nullptr) {
    return;
  }

  Cursor cursor_start = cursor_find(from);
  Cursor cursor_end = cursor_find(to);
  RBNode* start = cursor_start.found() ? cursor_start.node() : next(cursor_start).node();
  RBNode* end = cursor_end.found() ? cursor_end.node() : next(cursor_end).node();

  while (start != end) {
    f(start);
    start = start->next();
  }
}

#ifdef ASSERT
template <typename K, typename V, typename COMPARATOR, typename ALLOCATOR>
inline void RBTree<K, V, COMPARATOR, ALLOCATOR>::verify_self() {
  if (_root == nullptr) {
    assert(_num_nodes == 0, "rbtree has " SIZE_FORMAT " nodes but no root", _num_nodes);
    return;
  }

  assert(_root->_parent == nullptr, "root of rbtree has a parent");

  unsigned int black_nodes = 0;
  RBNode* node = _root;
  while (node != nullptr) {
    if (node->is_black()) {
      black_nodes++;
    }
    node = node->_left;
  }

  const size_t actual_num_nodes = _root->count_nodes();
  const size_t expected_num_nodes = size();
  const unsigned int maximum_depth = log2i(size() + 1) * 2;

  assert(expected_num_nodes == actual_num_nodes,
         "unexpected number of nodes in rbtree. expected: " SIZE_FORMAT
         ", actual: " SIZE_FORMAT, expected_num_nodes, actual_num_nodes);
  assert(2 * black_nodes <= maximum_depth,
         "rbtree is too deep for its number of nodes. can be at "
         "most: " INT32_FORMAT ", but is: " UINT32_FORMAT, maximum_depth, 2 * black_nodes);
  assert(_root->is_correct(black_nodes, maximum_depth, 1, _first), "rbtree does not hold rb-properties");
}
#endif // ASSERT

#endif // SHARE_UTILITIES_RBTREE_INLINE_HPP
