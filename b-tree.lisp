(defpackage :b-tree
  (:use :cl)
  (:export :binsert
           :bsearch
           :btree))

(in-package :b-tree)

(defstruct btree
  (max-children 6 :type (unsigned-byte 16))
  (root nil :type list))

(defstruct bnode
  (keys (make-array 0 :element-type '(unsigned-byte 64)) :type (simple-array (unsigned-byte 64) (*)))
  (size 0 :type number)
  (pointers (make-array 0 :element-type 'list :initial-element nil) :type (simple-array list (*))))

;;6
;;                                                            14
;;                                       4 8 11                                    17 20 23
;;      1 2 3      4 5 6 7          8 9 10    11 12 13             14 15 16    17 18 19    20 21 22      23 25 26 27 28

;; (defparameter *test-tree-root*
;;   '(
;;     (:less .
;;      ((:less . ((1 . :d1) (2 . :d2) (3 . :d3)))
;;       (4 . ((4 . :d4) (5 . :d5) (6 . :d6) (7 . :d7)))
;;       (8 . ((8 . :d8) (9 . :d9) (10 . :d10)))
;;       (11 . ((11 . :d11) (12 . :d12) (13 . :d13)))))
;;     (14 .
;;      ((:less . ((14 . :d14) (15 . :d15) (16 . :d16)))
;;       (17 . ((17 . :d17) (18 . :d18) (19 . :d19)))
;;       (20 . ((20 . :d20) (21 . :d21) (22 . :d22)))
;;       (23 . ((23 . :d23) (25 . :d25) (26 . :d26) (27 . :d27) (28 . :d28)))))))

(defun empty-bnode (max-children)
  (make-bnode :keys (make-array (1+ max-children) :element-type '(unsigned-byte 64))
              :pointers (make-array (+ 2 max-children) :element-type 'list :initial-element nil)))

(defun shift-bnode-data (node from-pos)
  (setf (aref (bnode-pointers node) (1+ (bnode-size node)))
        (aref (bnode-pointers node) (bnode-size node)))
  (loop for i from (bnode-size node) downto (1+ from-pos)
       do (setf (aref (bnode-pointers node) i)
                (aref (bnode-pointers node) (1- i))
                (aref (bnode-keys node) i)
                (aref (bnode-keys node) (1- i)))))

(defun put-bnode-data (node pos key data)
  (setf (aref (bnode-keys node) pos) key
        (aref (bnode-pointers node) pos) (cons data (aref (bnode-pointers node) pos)))
  (unless (zerop pos)
    (setf (cdr (aref (bnode-pointers node) (1- pos))) (aref (bnode-pointers node) pos))))

(defun add-data-to-bnode (node key data)
  (dotimes (i (bnode-size node))
    (when (= (aref (bnode-keys node) i) key)
      (setf (car (aref (bnode-pointers node) i)) data)
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
  (loop for si from from-pos
       for di from 0
       repeat count
       do (setf (aref (bnode-keys dnode) di) (aref (bnode-keys snode) si)
                (aref (bnode-pointers dnode) di) (aref (bnode-pointers snode) si))))

(defun bnode-trunc (node from-pos)
  (loop for i from from-pos to (1- (array-dimension (bnode-keys node) 0))
       do (setf (aref (bnode-keys node) i) 0
                (aref (bnode-pointers node) i) nil))
  (setf (aref (bnode-pointers node) (1- (array-dimension (bnode-pointers node) 0))) nil)
  node)

(defun bnode-split (node)
  (let ((split-point (ash (1- (bnode-size node)) -1))
        (new-root (empty-bnode (1- (array-dimension (bnode-keys node) 0))))
        (new-right (empty-bnode (1- (array-dimension (bnode-keys node) 0)))))
    (setf (bnode-size new-root) 1
          (aref (bnode-keys new-root) 0) (aref (bnode-keys node) split-point)
          (aref (bnode-pointers new-root) 0) (list node)
          (aref (bnode-pointers new-root) 1) (list new-right)

          (bnode-size new-right) (- (1- (bnode-size node)) split-point)
          (bnode-size node) split-point)
    (bnode-move node new-right (1+ split-point) (bnode-size new-right))
    (bnode-trunc node split-point)
    new-root))

(defun tree-insert (node-list max-children key data)
  (if (eq (caar node-list) :less)
      (let ((node-to-descend
             (block find-descend-node
               (if (< key (car (second node-list)))
                (car node-list)
                (dolist (node (reverse (cdr node-list)))
                  (when (>= key (car node)) (return-from find-descend-node node)))))))
        (multiple-value-bind (new-node-list new-leaf add-key)
            (tree-insert (cdr node-to-descend) max-children key data)
          (setf (cdr node-to-descend) new-node-list)
          (if new-leaf
              (let ((new-node-list (cons (car node-list)
                                         (sort 
                                          (cons (cons add-key new-leaf)
                                                (cdr node-list))
                                          #'(lambda (x y) (< (car x) (car y)))))))
                (if (<= (length new-node-list) (1+ max-children))
                    new-node-list
                    (let* ((split-point (1+ (ash max-children -1)))
                           (new-leaf (nthcdr split-point new-node-list))
                           (add-to-root-key (caar new-leaf)))
                      (setf (cdr (nthcdr (1- split-point) new-node-list)) nil
                            (caar new-leaf) :less)
                      (values new-node-list new-leaf add-to-root-key))))
              node-list)))
      (progn
        ;; (dolist (node node-list)
        ;;   (when (= (car node) key)
        ;;     (setf (cdr node) data)
        ;;     (return-from tree-insert node-list)))
        (let ((new-node-list (sort (cons (cons key data) node-list) #'(lambda (x y) (< (car x) (car y))))))
          (if (< (length new-node-list) max-children)
              new-node-list
              (let* ((split-point (ash max-children -1))
                     (new-leaf (nthcdr split-point new-node-list)))
                (setf (cdr (nthcdr (1- split-point) new-node-list)) nil)
                (values new-node-list new-leaf (caar new-leaf))))))))

;; (defun tree-insert (node-list max-children key data)
;;   (if (eq (caar node-list) :less)
;;       (let ((node-to-descend
;;              (block find-descend-node
;;                (if (< key (car (second node-list)))
;;                 (car node-list)
;;                 (dolist (node (reverse (cdr node-list)))
;;                   (when (>= key (car node)) (return-from find-descend-node node)))))))
;;         (multiple-value-bind (new-node-list new-leaf add-key)
;;             (tree-insert (cdr node-to-descend) max-children key data)
;;           (setf (cdr node-to-descend) new-node-list)
;;           (if new-leaf
;;               (let ((new-node-list (cons (car node-list)
;;                                          (sort 
;;                                           (cons (cons add-key new-leaf)
;;                                                 (cdr node-list))
;;                                           #'(lambda (x y) (< (car x) (car y)))))))
;;                 (if (<= (length new-node-list) (1+ max-children))
;;                     new-node-list
;;                     (let* ((split-point (1+ (ash max-children -1)))
;;                            (new-leaf (nthcdr split-point new-node-list))
;;                            (add-to-root-key (caar new-leaf)))
;;                       (setf (cdr (nthcdr (1- split-point) new-node-list)) nil
;;                             (caar new-leaf) :less)
;;                       (values new-node-list new-leaf add-to-root-key))))
;;               node-list)))
;;       (progn
;;         (dolist (node node-list)
;;           (when (= (car node) key)
;;             (setf (cdr node) data)
;;             (return-from tree-insert node-list)))
;;         (let ((new-node-list (sort (cons (cons key data) node-list) #'(lambda (x y) (< (car x) (car y))))))
;;           (if (< (length new-node-list) max-children)
;;               new-node-list
;;               (let* ((split-point (ash max-children -1))
;;                      (new-leaf (nthcdr split-point new-node-list)))
;;                 (setf (cdr (nthcdr (1- split-point) new-node-list)) nil)
;;                 (values new-node-list new-leaf (caar new-leaf))))))))

(defun binsert (tree key data)
  (declare (type btree tree)
           (type (unsigned-byte 64) key))
  (multiple-value-bind (new-node-list new-leaf add-key)
      (tree-insert (btree-root tree) (btree-max-children tree) key data)
    (if new-leaf
        (setf (btree-root tree) (list (cons :less new-node-list)
                                      (cons add-key new-leaf)))
        (setf (btree-root tree) new-node-list))
    tree))


(defun tree-search (node-list key)
  (if (eq (caar node-list) :less)
      (if (< key (car (second node-list)))
          (tree-search (cdar node-list) key)
          (dolist (node (reverse (cdr node-list)))
            (when (>= key (car node)) (return-from tree-search (tree-search (cdr node) key)))))
      (progn
        (dolist (node node-list)
          (when (= (car node) key) (return-from tree-search (cdr node))))
        nil)))

(defun bsearch (tree key)
  (declare (type btree tree)
           (type (unsigned-byte 64) key))
  (tree-search (btree-root tree) key))
