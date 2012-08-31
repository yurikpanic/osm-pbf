(defpackage :b-tree
  (:use :cl)
  (:export :binsert
           :bsearch
           :make-empty-btree
           :btree-print-level
           :btree-size
           :btree-nodes-count
           :btree-data-list))

(in-package :b-tree)

(defstruct bnode
  (kind :leaf :type keyword)
  (keys (make-array 0 :element-type '(unsigned-byte 64)) :type (simple-array (unsigned-byte 64) (*)))
  (size 0 :type number)
  (pointers (make-array 0 :element-type 'list :initial-element nil) :type (simple-array list (*))))

(defstruct btree
  (max-children 6 :type (unsigned-byte 16))
  (root nil :type list))

(defun make-empty-bnode (max-children &optional (kind :leaf))
  (make-bnode :kind kind
              :keys (make-array (1+ max-children) :element-type '(unsigned-byte 64))
              :pointers (make-array (+ 2 max-children) :element-type 'list :initial-element nil)))

(defun make-empty-btree (&optional (max-children 6))
  (make-btree :max-children max-children :root (list (make-empty-bnode max-children))))

(defun shift-bnode-data (node from-pos)
  (loop for i from (bnode-size node) downto (1+ from-pos)
       do (setf (aref (bnode-pointers node) (1+ i))
                (aref (bnode-pointers node) i)
                (aref (bnode-keys node) i)
                (aref (bnode-keys node) (1- i)))))

(defun put-bnode-data (node pos key data)
  (setf (aref (bnode-keys node) pos) key
        (aref (bnode-pointers node) (1+ pos)) (cons data (aref (bnode-pointers node) (1+ pos))))
  (unless (zerop pos)
    (setf (cdr (aref (bnode-pointers node) pos)) (aref (bnode-pointers node) (1+ pos)))))

(defun add-data-to-bnode (node key data)
  (dotimes (i (bnode-size node))
    (when (= (aref (bnode-keys node) i) key)
      (setf (car (aref (bnode-pointers node) (1+ i))) data)
      (return-from add-data-to-bnode node))
    (when (> (aref (bnode-keys node) i) key)
      (shift-bnode-data node i)
      (put-bnode-data node i key data)
      (incf (bnode-size node))
      (return-from add-data-to-bnode node)))
  (put-bnode-data node (bnode-size node) key data)
  (incf (bnode-size node))
  node)

(defun bnode-move (snode dnode from-pos count)
  (setf (aref (bnode-pointers dnode) 0) (aref (bnode-pointers snode) from-pos))
  (loop for si from from-pos
       for di from 0
       repeat count
       do (setf (aref (bnode-keys dnode) di) (aref (bnode-keys snode) si)
                (aref (bnode-pointers dnode) (1+ di)) (aref (bnode-pointers snode) (1+ si)))))

(defun bnode-trunc (node from-pos)
  (loop for i from from-pos to (1- (array-dimension (bnode-keys node) 0))
       do (setf (aref (bnode-keys node) i) 0
                (aref (bnode-pointers node) (1+ i)) nil))
  (setf (aref (bnode-pointers node) (1- (array-dimension (bnode-pointers node) 0))) nil)
  node)

(defun bnode-split (node)
  (let* ((split-point (ash (1- (bnode-size node)) -1))
         (move-up (aref (bnode-keys node) split-point))
         (new-right (make-empty-bnode (1- (array-dimension (bnode-keys node) 0)) (bnode-kind node))))
    (setf (bnode-size new-right) (- (bnode-size node) split-point (if (eq (bnode-kind node) :node) 1 0))
          (bnode-size node) split-point)
    (bnode-move node new-right (+ split-point (if (eq (bnode-kind node) :node) 1 0)) (bnode-size new-right))
    (bnode-trunc node split-point)
    (values node new-right move-up)))

(defun bnode-find (node key)
  (when (zerop (bnode-size node))
    (return-from bnode-find nil))
  (if (eq (bnode-kind node) :node)
      (if (< key (aref (bnode-keys node) 0))
          (return-from bnode-find (values (aref (bnode-pointers node) 0) 0))
          (if (>= key (aref (bnode-keys node) (1- (bnode-size node))))
              (return-from bnode-find (values (aref (bnode-pointers node) (bnode-size node)) (bnode-size node)))
              (loop for i from 0 to (- (bnode-size node) 2)
                 do (when (and (>= key (aref (bnode-keys node) i))
                               (< key (aref (bnode-keys node) (1+ i))))
                      (return-from bnode-find (values (aref (bnode-pointers node) (1+ i)) (1+ i)))))))
      (when (eq (bnode-kind node) :leaf)
        (loop for i from 0 to (1- (bnode-size node))
             do (when (= key (aref (bnode-keys node) i))
                  (return-from bnode-find (values (aref (bnode-pointers node) (1+ i)) (1+ i)))))))
  nil)

(defun tree-insert (node max-children key data)
  (if (eq (bnode-kind node) :node)
      (multiple-value-bind (node-to-descend desc-ptr-num) (bnode-find node key)
        (multiple-value-bind (node-left node-right add-key)
            (tree-insert (car node-to-descend) max-children key data)
          (setf (car node-to-descend) node-left)
          (if node-right
              (progn
                (shift-bnode-data node desc-ptr-num)
                (setf (aref (bnode-keys node) desc-ptr-num) add-key
                      (aref (bnode-pointers node) (1+ desc-ptr-num)) (list node-right)
                      (bnode-size node) (1+ (bnode-size node)))
                (if (<= (bnode-size node) max-children)
                    node
                    (bnode-split node)))
              node)))
      (when (eq (bnode-kind node) :leaf)
        (add-data-to-bnode node key data)
        (if (<= (bnode-size node) max-children)
            node
            (bnode-split node)))))

(defun binsert (tree key data)
  (declare (type btree tree)
           (type (unsigned-byte 64) key))
  (multiple-value-bind (node-left node-right add-key)
      (tree-insert (car (btree-root tree)) (btree-max-children tree) key data)
    (when node-right
      (let ((new-root (make-empty-bnode (btree-max-children tree) :node)))
        (setf (bnode-size new-root) 1
              (aref (bnode-keys new-root) 0) add-key
              (aref (bnode-pointers new-root) 0) (list node-left)
              (aref (bnode-pointers new-root) 1) (list node-right)
              (btree-root tree) (list new-root))))
    tree))

(defun tree-search (node key)
  (if (eq (bnode-kind node) :node)
      (tree-search (car (bnode-find node key)) key)
      (car (bnode-find node key))))

(defun bsearch (tree key)
  (declare (type btree tree)
           (type (unsigned-byte 64) key))
  (tree-search (car (btree-root tree)) key))

(defun bnode-print-level (node level &optional (stream t))
  (if (zerop level)
      (format stream "~A " (bnode-keys node))
      (when (eq (bnode-kind node) :node)
        (let ((new-level (1- level)))
          (loop for ptr across (bnode-pointers node)
               do (when ptr (bnode-print-level (car ptr) new-level stream))))
        (when (= 1 level) (format stream "~%")))))

(defun btree-print-level (tree level &optional (stream t))
  (bnode-print-level (car (btree-root tree)) level stream))

(defun bnode-subtree-size (node)
  (if (eq (bnode-kind node) :leaf)
      (bnode-size node)
      (loop for child across (bnode-pointers node)
           for i from 0 to (bnode-size node)
           summing (if child (bnode-subtree-size (car child)) 0))))

(defun btree-size (tree)
  (bnode-subtree-size (car (btree-root tree))))

(defun bnode-subtree-nodes-count (node)
  (if (eq (bnode-kind node) :leaf)
      (values 0 1)
      (let ((nodes 1)
            (leaves 0))
        (loop for child across (bnode-pointers node)
           for i from 0 to (bnode-size node)
           do (when child
                (multiple-value-bind (nn ll)
                    (bnode-subtree-nodes-count (car child))
                  (incf nodes nn)
                  (incf leaves ll))))
        (values nodes leaves))))

(defun btree-nodes-count (tree)
  (bnode-subtree-nodes-count (car (btree-root tree))))

(defun bnode-data-list (node)
  (if (eq (bnode-kind node) :leaf)
      (aref (bnode-pointers node) 1)
      (bnode-data-list (car (aref (bnode-pointers node) 0)))))

(defun btree-data-list (tree)
  (bnode-data-list (car (btree-root tree))))
