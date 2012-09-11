(defpackage :osm-index-search
  (:use :cl)
  (:export :init-osm-search))

(in-package :osm-index-search)

(defstruct search-data
  (file-name #p"/home/yuri/work/globus/osm/out.osm.pbf" :type pathname)
  (blob-offsets (make-array 0 :element-type '(unsigned-byte 64) :adjustable t :fill-pointer 0) :type vector)
  (node-id-btree-idx (make-instance 'btreepbf:btree-index) :type btreepbf:btree-index))

(defun load-blob-and-header (fs &optional pos)
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
              (let ((blob-buf (make-array (osmpbf:datasize blob-header) :element-type '(unsigned-byte 8))))
                (read-sequence blob-buf fs)
                (let ((blob (make-instance 'osmpbf:blob)))
                  (pb:merge-from-array blob blob-buf 0 (length blob-buf))
                  (values blob-header blob))))))))

(defun get-blob-uncompressed-data (blob)
  (if (osmpbf:has-raw blob)
      (osmpbf:raw blob)
      (coerce (zlib:uncompress (osmpbf:zlib-data blob) :uncompressed-size (osmpbf:raw-size blob)) '(simple-array (unsigned-byte 8) (*)))))

(defun map-blobs (fun fs)
  (do ((file-size (file-length fs))
       (file-pos (file-position fs) (file-position fs)))
      ((>= file-pos file-size) fs)
    (multiple-value-bind (blob-header blob)
        (load-blob-and-header fs)
      (funcall fun blob-header (get-blob-uncompressed-data blob) file-pos))))

(defun init-osm-search (&optional (file-name #p"/home/yuri/work/globus/osm/out.osm.pbf"))
  (let ((sd (make-search-data :file-name file-name)))
    (with-open-file (f (search-data-file-name sd) :direction :input :element-type '(unsigned-byte 8))
      (map-blobs #'(lambda (header data header-pos)
                     (vector-push-extend header-pos (search-data-blob-offsets sd))
                     (when (string= (pb:string-value (osmpbf:type header)) "btree-index")
                       (let ((btree-idx (make-instance 'btreepbf:btree-index)))
                         (pb:merge-from-array btree-idx data 0 (length data))
                         (when (and (string= (pb:string-value (btreepbf:type btree-idx)) "node")
                                    (string= (pb:string-value (btreepbf:field btree-idx)) "id"))
                           (setf (search-data-node-id-btree-idx sd) btree-idx)))))
                 f))
    sd))

(defvar *blob-cache* (make-hash-table :test 'eq))

(defun get-btree-entry-in-blob (sd blob-num idx)
  (let ((btree (gethash blob-num *blob-cache*)))
    (unless btree
      (with-open-file (fs (search-data-file-name sd) :direction :input :element-type '(unsigned-byte 8))
        (let ((blob-offset (aref (search-data-blob-offsets sd) blob-num)))
          (multiple-value-bind (header blob)
              (load-blob-and-header fs blob-offset)
            (when (string= (pb:string-value (osmpbf:type header)) "btree")
              (let ((data (get-blob-uncompressed-data blob)))
                (setf btree (make-instance 'btreepbf:btree))
                (pb:merge-from-array btree data 0 (length data))
                (setf (gethash blob-num *blob-cache*) btree)))))))
    (aref (btreepbf:nodes btree) idx)))

(defun get-btree-entry (sd index)
  ;; when there will be multiple btree indexes - add here parameters to select proper btree (e.g. by struc type and field name)
  (loop for entry across (btreepbf:entries (search-data-node-id-btree-idx sd))
       do
       (when (and (>= index (btreepbf:min-node entry)) (<= index (btreepbf:max-node entry)))
         (return-from get-btree-entry (get-btree-entry-in-blob sd (btreepbf:blob-num entry) (- index (btreepbf:min-node entry)))))))

(defun search-btree (sd bnode id)
  (let ((keys-len (length (btreepbf:keys bnode))))
    (if (= (btreepbf:kind bnode) btreepbf:+bnode-kind-node+)
        (search-btree
         sd
         (get-btree-entry
          sd
          (aref (btreepbf:pointers bnode)
                (if (< id (aref (btreepbf:keys bnode) 0))
                    0
                    (or 
                     (block k-search
                       (dotimes (i (1- keys-len))
                         (when (and (<= (aref (btreepbf:keys bnode) i) id) (< id (aref (btreepbf:keys bnode) (1+ i))))
                           (return-from k-search (1+ i))))
                       nil)
                     keys-len))))
         id)
        (progn
          (loop for key across (btreepbf:keys bnode)
             for val across (btreepbf:values bnode)
               do (when (= key id)
                    (return-from search-btree val)))
          nil))))

(defun get-node-entry-in-blob (sd blob-num idx)
  (let ((pblock (gethash blob-num *blob-cache*)))
    (unless pblock
      (with-open-file (fs (search-data-file-name sd) :element-type '(unsigned-byte 8))
        (multiple-value-bind (header blob)
            (load-blob-and-header fs (aref (search-data-blob-offsets sd) blob-num))
          (when (string= (pb:string-value (osmpbf:type header)) "OSMData")
            (let ((data (get-blob-uncompressed-data blob)))
              (setf pblock (make-instance 'osmpbf:primitive-block))
              (pb:merge-from-array pblock data 0 (length data))
              (setf (gethash blob-num *blob-cache*) pblock))))))
    (let ((idx-to-return idx))
      (loop for pgroup across (osmpbf:primitivegroup pblock)
         do (let ((cur-nodes-len (length (osmpbf:nodes pgroup))))
              (if (< idx-to-return cur-nodes-len)
                  (return-from get-node-entry-in-blob (aref (osmpbf:nodes pgroup) idx-to-return))
                  (decf idx-to-return cur-nodes-len)))))))

(defun find-node-by-id (sd id)
  (let* ((root (get-btree-entry sd 0))
         (val (search-btree sd root id)))
    (when val
      (let ((blob-idx (make-instance 'btreepbf:blob-index)))
        (pb:merge-from-array blob-idx val 0 (length val))
        (get-node-entry-in-blob sd (btreepbf:blob-num blob-idx) (btreepbf:blob-elt blob-idx))))))

;; e.g. (find-node-by-id *sd* 337732605)
