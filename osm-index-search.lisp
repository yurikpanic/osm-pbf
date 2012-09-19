(defpackage :osm-index-search
  (:use :cl)
  (:export :init-osm-search))

(in-package :osm-index-search)

(defstruct search-data
  (file-name #p"/home/yuri/work/globus/osm/out.osm.pbf" :type pathname)
  (blob-offsets (make-array 0 :element-type '(unsigned-byte 64) :adjustable t :fill-pointer 0) :type vector)
  (node-id-btree (make-instance 'btreepbf:btree) :type btreepbf:btree)
  (deserialize-array nil :type (or null vector))
  (btree-offset 0 :type integer))

(defun get-deserialize-array (sd size)
  (let ((arr (search-data-deserialize-array sd)))
    (when (or (not arr) (< (array-dimension arr 0) size))
      (setf arr (make-array size :element-type '(unsigned-byte 8))
            (search-data-deserialize-array sd) arr))
    arr))

(defmacro read-uint32 (fs)
  (let ((buf (gensym))
        (res (gensym))
        (d (gensym)))
    `(let ((,buf (make-array 4 :element-type '(unsigned-byte 8))))
       (read-sequence ,buf ,fs)
       (let ((,res 0))
         (loop for ,d across ,buf
            do (setf ,res (logior (ash ,res 8) ,d)))
         ,res))))

(defun load-blob-header (fs &optional pos)
  (when pos
    (file-position fs pos))
  (let ((blob-header-len-buf (make-array 4 :element-type '(unsigned-byte 8))))
    (read-sequence blob-header-len-buf fs)
    (let ((blob-header-len 0))
      (loop for d across blob-header-len-buf
         do (setf blob-header-len (logior (ash blob-header-len 8) d)))
      (let ((blob-header-buf (make-array blob-header-len :element-type '(unsigned-byte 8))))
            (read-sequence blob-header-buf fs)
            (let ((blob-header (make-instance 'osmpbf:blob-header)))
              (pb:merge-from-array blob-header blob-header-buf 0 blob-header-len)
              blob-header)))))

(defun get-blob-uncompressed-data (blob)
  (if (osmpbf:has-raw blob)
      (osmpbf:raw blob)
      (coerce (zlib:uncompress (osmpbf:zlib-data blob) :uncompressed-size (osmpbf:raw-size blob)) '(simple-array (unsigned-byte 8) (*)))))

(defun map-blobs (fun fs)
  (do ((file-size (file-length fs))
       (file-pos (file-position fs) (file-position fs)))
      ((>= file-pos file-size) fs)
    (let ((blob-header (load-blob-header fs)))
      (let ((pos-before-data (file-position fs)))
        (funcall fun blob-header file-pos)
        (file-position fs (+ pos-before-data (osmpbf:datasize blob-header)))))))

(defun init-osm-search (&optional (file-name #p"/home/yuri/work/globus/osm/out.osm.pbf"))
  (let ((sd (make-search-data :file-name file-name)))
    (with-open-file (fs (search-data-file-name sd) :direction :input :element-type '(unsigned-byte 8))
      (map-blobs #'(lambda (header header-pos)
                     (vector-push-extend header-pos (search-data-blob-offsets sd))
                     (when (string= (pb:string-value (osmpbf:type header)) "btree")
                       (let* ((btree-header-len (read-uint32 fs))
                              (buf (make-array btree-header-len :element-type '(unsigned-byte 8))))
                         (read-sequence buf fs)
                         (let ((pbtree (make-instance 'btreepbf:btree)))
                           (pb:merge-from-array pbtree buf 0 btree-header-len)
                           (when (and (string= (pb:string-value (btreepbf:type pbtree)) "node")
                                      (string= (pb:string-value (btreepbf:field pbtree)) "id"))
                             (setf (search-data-node-id-btree sd) pbtree
                                   (search-data-btree-offset sd) (file-position fs)))))))
                 fs))
    sd))

(defun get-btree-node (sd offs size)
  (with-open-file (fs (search-data-file-name sd) :direction :input :element-type '(unsigned-byte 8))
    (file-position fs (+ offs (search-data-btree-offset sd)))
    (let ((arr (get-deserialize-array sd size)))
      (read-sequence arr fs :end size)
      (let ((bnode (make-instance 'btreepbf:bnode)))
        (pb:merge-from-array bnode arr 0 size)
        bnode))))

(defun search-btree (sd bnode id)
  (let ((keys-len (length (btreepbf:keys bnode))))
    (if (= (btreepbf:kind bnode) btreepbf:+bnode-kind-node+)
        (let ((descend-idx (if (< id (aref (btreepbf:keys bnode) 0))
                               0
                               (or 
                                (block k-search
                                  (dotimes (i (1- keys-len))
                                    (when (and (<= (aref (btreepbf:keys bnode) i) id) (< id (aref (btreepbf:keys bnode) (1+ i))))
                                      (return-from k-search (1+ i))))
                                  nil)
                                keys-len))))
          (search-btree
           sd
           (get-btree-node sd
                           (aref (btreepbf:pointers bnode) descend-idx)
                           (aref (btreepbf:child-sizes bnode) descend-idx))
           id))
        (progn
          (loop for key across (btreepbf:keys bnode)
             for val across (btreepbf:values bnode)
               do (when (= key id)
                    (return-from search-btree val)))
          nil))))

(defun load-node-direct (sd blob-num offs node-size)
  (with-open-file (fs (search-data-file-name sd) :element-type '(unsigned-byte 8))
    (file-position fs (aref (search-data-blob-offsets sd) blob-num))
    (let ((blob-header-len-buf (make-array 4 :element-type '(unsigned-byte 8))))
      (read-sequence blob-header-len-buf fs)
      (let ((blob-header-len 0))
        (loop for d across blob-header-len-buf
           do (setf blob-header-len (logior (ash blob-header-len 8) d)))
        (let ((blob-header-buf (make-array blob-header-len :element-type '(unsigned-byte 8))))
          (read-sequence blob-header-buf fs)
          (file-position fs (+ (file-position fs) offs))
          (let ((node-buf (make-array node-size :element-type '(unsigned-byte 8))))
            (read-sequence node-buf fs)
            node-buf))))))

(defun find-node-by-id (sd id)
  (let* ((btree (search-data-node-id-btree sd))
         (root (get-btree-node sd (btreepbf:root-offs btree) (btreepbf:root-size btree)))
         (val (search-btree sd root id)))
    (when val
      (let ((blob-idx (make-instance 'btreepbf:blob-index))
            (node (make-instance 'osmpbf:node)))
        (pb:merge-from-array blob-idx val 0 (length val))
        (pb:merge-from-array
         node
         (load-node-direct sd (btreepbf:blob-num blob-idx) (btreepbf:blob-offs blob-idx) (btreepbf:size blob-idx))
         0 (btreepbf:size blob-idx))
        node))))

;; e.g. (find-node-by-id *sd* 337732605)
;;      (find-node-by-id *sd* 337526439)
