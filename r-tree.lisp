(defpackage :r-tree
  (:use :cl
        :in-mem-str))

(in-package :r-tree)

(defstruct rnode
  (kind :leaf :type keyword)
  (bbox (make-bbox) :type bbox)
  (keys (make-array 0 :element-type 'bbox :initial-element (make-bbox)) :type (simple-array bbox (*)))
  (size 0 :type number)
  (pointers (make-array 0 :element-type 'list :initial-element nil) :type (simple-array list (*))))

(defstruct rtree
  (max-children 6 :type (unsigned-byte 16))
  (root nil :type list))

(defun make-empty-rnode (max-children &optional (kind :leaf))
  (make-rnode :kind kind
              :keys (make-array (1+ max-children) :element-type 'bbox :initial-contents (loop for i from 0 to max-children collect (make-bbox)))
              :pointers (make-array (1+ max-children) :element-type 'list :initial-element nil)))

(defun make-empty-rtree (&optional (max-children 6))
  (make-rtree :max-children max-children :root (list (make-empty-rnode max-children))))

(defun add-data-to-rnode (node key-bbox data)
  (copy-to-bbox (aref (rnode-keys node) (rnode-size node)) key-bbox)
  (if (zerop (rnode-size node))
      (copy-to-bbox (rnode-bbox node) key-bbox)
      (extend-bbox (rnode-bbox node) key-bbox))
  (setf (aref (rnode-pointers node) (rnode-size node)) (cons data (aref (rnode-pointers node) (rnode-size node)))
        (rnode-size node) (1+ (rnode-size node)))
  node)

(defun calc-group-bb (entries group-start group-end)
  (let ((bbox (make-bbox)))
    (copy-to-bbox bbox (car (aref entries group-start)))
    (loop for i from (1+ group-start) to group-end
         do (extend-bbox bbox (car (aref entries i))))
    bbox))

(defun rnode-split (node max-children)
  (let* ((bb (make-array (rnode-size node)
                         :initial-contents
                         (loop for k across (rnode-keys node)
                            for p across (rnode-pointers node)
                            collect (cons k p))))
         (bb-sorted (list
                     (sort (copy-seq bb)
                           #'(lambda (bb1 bb2)
                               (< (bbox-min-lon (car bb1)) (bbox-min-lon (car bb2)))))
                     (sort (copy-seq bb)
                           #'(lambda (bb1 bb2)
                               (< (bbox-max-lon (car bb1)) (bbox-max-lon (car bb2)))))
                     (sort (copy-seq bb)
                           #'(lambda (bb1 bb2)
                               (< (bbox-min-lat (car bb1)) (bbox-min-lat (car bb2)))))
                     (sort (copy-seq bb)
                           #'(lambda (bb1 bb2)
                               (< (bbox-max-lat (car bb1)) (bbox-max-lat (car bb2)))))))
         (min-children (truncate (* max-children 0.4)))
         (area-values nil)
         (margin-values nil)
         (overlap-values nil))
    (loop for k from 1 to (+ 2 (- max-children (* 2 min-children)))
         do (let ((first-size (+ k (1- min-children)))
                  (area-k nil)
                  (margin-k nil)
                  (overlap-k nil))
              (dolist (bb-cur bb-sorted)
                (let* ((first-bbox (calc-group-bb bb-cur 0 (1- first-size)))
                       (second-bbox (calc-group-bb bb-cur first-size (1- (rnode-size node))))
                       (area-value (+ (bbox-area first-bbox) (bbox-area second-bbox)))
                       (margin-value (+ (bbox-margin first-bbox) (bbox-margin second-bbox)))
                       (overlap (bbox-overlap first-bbox second-bbox))
                       (overlap-value (if overlap (bbox-area overlap) 0)))
                  (push area-value area-k)
                  (push margin-value margin-k)
                  (push overlap-value overlap-k)))
              (push (nreverse area-k) area-values)
              (push (nreverse margin-k) margin-values)
              (push (nreverse overlap-k) overlap-values)))
    (setf area-values (nreverse area-values)
          margin-values (nreverse margin-values)
          overlap-values (nreverse overlap-values))
    ;;(format t "~A~%~A~%~A~%" area-values margin-values overlap-values)
    (let ((s-lon 0)
          (s-lat 0)
          (sel-axes)
          (dist-start))
      (loop for mm in margin-values
           do (incf s-lon (+ (first mm) (second mm)))
           do (incf s-lat (+ (third mm) (fourth mm))))
      (if (< s-lon s-lat)
          (setf sel-axes :lon
                dist-start 0)
          (setf sel-axes :lat
                dist-start 2))
      ;;(format t "~A ~A ~A ~A~%" s-lon s-lat sel-axes dist-start)
      (let ((min-overlap-k nil)
            (min-overlap-i nil)
            (min-overlap most-positive-fixnum))
        (loop for k from 1
           for ol in overlap-values
             do (loop for i from dist-start
                   repeat 2
                   for dist-ol in (nthcdr dist-start ol)
                   do (when (< dist-ol min-overlap)
                        (setf min-overlap dist-ol
                              min-overlap-k k
                              min-overlap-i i))))
        ;;(format t "~A ~A ~A~%" min-overlap-k min-overlap-i min-overlap)
        (let ((node-1 (make-empty-rnode max-children (rnode-kind node)))
              (node-2 (make-empty-rnode max-children (rnode-kind node)))
              (bb-dist (nth min-overlap-i bb-sorted))
              (first-size (+ min-overlap-k (1- min-children))))
          (flet ((add-to-rnode (node group-start group-end)
                   (loop for i from group-start to group-end
                      for j from 0
                      do (add-data-to-rnode node (car (aref bb-dist i)) (cadr (aref bb-dist i))))))
            (add-to-rnode node-1 0 (1- first-size))
            (add-to-rnode node-2 first-size (1- (rnode-size node)))
            (values node-1 node-2)))))))

(defun choose-subtree (node key-bbox)
  "Choose subtree to insert data with key-bbox"
  (if (eq (rnode-kind (car (aref (rnode-pointers node) 0))) :leaf)
      (progn
        ;; minimal overlap enlargement
        )
      (progn
        ;; minimal area enlargement
        )))

(defun tree-insert (node max-children key-bbox data)
  (if (eq (rnode-kind node) :node)
      (let ((node-to-descend (choose-subtree node key-bbox))))
      (when (eq (rnode-kind node) :leaf)
        (add-data-to-rnode node key-bbox data)
        (if (<= (rnode-size node) max-children)
            node
            (rnode-split node max-children)))))

(defun rinsert (tree key-bbox data)
  (declare (type rtree tree)
           (type bbox key-bbox))
  (multiple-value-bind (node-1 node-2)
      (tree-insert (car (rtree-root tree)) (rtree-max-children tree) key-bbox data)
    (when node-2
      (let ((new-root (make-empty-rnode (rtree-max-children tree) :node)))
        (add-data-to-rnode new-root (rnode-bbox node-1) node-1)
        (add-data-to-rnode new-root (rnode-bbox node-2) node-2)
        (setf (rtree-root tree) (list new-root))))
    tree))


(defparameter *bbox-test-data* '((232292559 491271921 232636826 491533113)
                                 (232776909 491353818 232963375 491604037)
                                 (232559962 492151262 232968719 492436581)
                                 (232089151 492411376 232559962 492671600)
                                 (232320923 492331217 232559962 492436581)
                                 (232533811 491973017 232816143 492063659)
                                 (232577560 492063659 232816143 492108386)))

(defparameter *id-test-data* '(178489505 178489280 30701151 178488149 178488157 178488155 30696680))

;;;; New empty btree
;; (defparameter *rtree* (make-empty-rtree))
;;;; Fill it to max-children
;; (loop for bb in *bbox-test-data* for id in *id-test-data* for i from 1 to 6 do (rinsert *rtree* (bbox-from-list bb) id))
;;;; Add one more to test node-split
;; (rinsert *rtree* (bbox-from-list (nth 6 *bbox-test-data*)) (nth 6 *id-test-data*))
