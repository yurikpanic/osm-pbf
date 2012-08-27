(defpackage :b-tree
  (:use :cl)
  (:export :insert
           :btree))

(in-package :b-tree)

(defstruct btree
  (max-children 6 :type (unsigned-byte 16))
  (root nil :type list))

;;6
;;                                                            14
;;                                       4 8 11                                    17 20 23
;;      1 2 3      4 5 6 7          8 9 10    11 12 13             14 15 16    17 18 19    20 21 22      23 24 25 26

(defparameter *test-tree-root*
  '(
    (:less .
     ((:less . ((1 . :d1) (2 . :d2) (3 . :d3)))
      (4 .((4 . :d4) (5 . :d5) (6 . :d6) (7 . :d7)))
      (8 . ((8 . :d8) (9 . :d9) (10 . :d10)))
      (11 . ((11 . :d11) (12 . :d12) (13 . :d13)))))
    (14 .
     ((:less . ((14 . :d14) (15 . :d15) (16 . :d16)))
      (17 . ((17 . :d17) (18 . :d18) (19 . :d19)))
      (20 . ((20 . :d20) (21 . :d21) (22 . :d22)))
      (23 . ((23 . :d23) (24 . :d24) (25 . :d25) (26 . :d26)))))))

(defun tree-insert (node-list max-children key data)
  )

(defun insert (tree key data)
  (declare (type btree tree)
           (type (unsigned-byte 64) key))
  (setf (btree-root tree) (tree-insert (btree-root tree) (max-children tree) key data)))
