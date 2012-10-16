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
    (format t "~A~%~A~%~A~%" area-values margin-values overlap-values)
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
      (format t "~A ~A ~A~%" s-lon s-lat sel-axes))))

(defun tree-insert (node max-children key-bbox data)
  (if (eq (rnode-kind node) :node)
      (progn)
      (when (eq (rnode-kind node) :leaf)
        (add-data-to-rnode node key-bbox data)
        (if (<= (rnode-size node) max-children)
            node
            (rnode-split node)))))

(defun rinsert (tree key-bbox data)
  (declare (type rtree tree)
           (type bbox key-bbox))
  (tree-insert (car (rtree-root tree)) (rtree-max-children tree) key-bbox data))


;; (defparameter *rtree*
;;   #S(RTREE
;;      :MAX-CHILDREN 6
;;      :ROOT (#S(RNODE
;;                :KIND :LEAF
;;                :BBOX #S(BBOX
;;                         :MIN-LON 232089151
;;                         :MIN-LAT 491271921
;;                         :MAX-LON 232968719
;;                         :MAX-LAT 492671600)
;;                :KEYS #(#S(BBOX
;;                           :MIN-LON 232292559
;;                           :MIN-LAT 491271921
;;                           :MAX-LON 232636826
;;                           :MAX-LAT 491533113)
;;                        #S(BBOX
;;                           :MIN-LON 232776909
;;                           :MIN-LAT 491353818
;;                           :MAX-LON 232963375
;;                           :MAX-LAT 491604037)
;;                        #S(BBOX
;;                           :MIN-LON 232559962
;;                           :MIN-LAT 492151262
;;                           :MAX-LON 232968719
;;                           :MAX-LAT 492436581)
;;                        #S(BBOX
;;                           :MIN-LON 232089151
;;                           :MIN-LAT 492411376
;;                           :MAX-LON 232559962
;;                           :MAX-LAT 492671600)
;;                        #S(BBOX
;;                           :MIN-LON 232320923
;;                           :MIN-LAT 492331217
;;                           :MAX-LON 232559962
;;                           :MAX-LAT 492436581)
;;                        #S(BBOX
;;                           :MIN-LON 232533811
;;                           :MIN-LAT 491973017
;;                           :MAX-LON 232816143
;;                           :MAX-LAT 492063659)
;;                        #S(BBOX
;;                           :MIN-LON 232577560
;;                           :MIN-LAT 492063659
;;                           :MAX-LON 232816143
;;                           :MAX-LAT 492108386))
;;                :SIZE 7
;;                :POINTERS #((178489505) (178489280) (30701151) (178488149)
;;                            (178488157) (178488155) (30696680))))))