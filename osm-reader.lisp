(defpackage :osm-reader
  (:use :cl))

(in-package :osm-reader)

(defun print-blob-descr (blob &optional (stream t))
  (format stream "blob")
  (if (osmpbf:has-raw blob)
      (format stream " raw data (size ~A)" (length (osmpbf:raw blob)))
      (when (osmpbf:has-zlib-data blob)
        (format stream " zlib data (size ~A, uncompressed ~A)" (length (osmpbf:zlib-data blob)) (osmpbf:raw-size blob))))
  (format stream "~%"))

(defvar *last-block-processed* nil)

(defun read-osm-file (&key (file-name #p"/home/yuri/work/globus/osm/UA.osm.pbf") (skip-to-block 0) count)
  (with-open-file (fs file-name :direction :input :element-type 'unsigned-byte)
    (do ((i 0 (1+ i))
         (file-size (file-length fs))
         (file-pos 0 (file-position fs))
         (proc-count 0))
        ((>= file-pos file-size) nil)
      (let ((blob-header-len-buf (make-array 4 :element-type '(unsigned-byte 8))))
        (read-sequence blob-header-len-buf fs)
        ;;(format t "================================= :~A [~,1F %]~%blob-header-len-buf ~A~%" file-pos (* (/ file-pos file-size) 100.0) blob-header-len-buf)
        (let ((blob-header-len 0))
          (loop for d across blob-header-len-buf
             do (setf blob-header-len (logior (ash blob-header-len 8) d)))
          ;;(format t "blob-header-len ~A~%" blob-header-len)
          (let ((blob-header-buf (make-array blob-header-len :element-type '(unsigned-byte 8))))
            (read-sequence blob-header-buf fs)
            (let ((blob-header (make-instance 'osmpbf:blob-header)))
              (pb:merge-from-array blob-header blob-header-buf 0 blob-header-len)
              ;;(format t "blob-header ~A ~A~%" i blob-header)
              (if (< i skip-to-block)
                  (file-position fs (+ (file-position fs) (osmpbf:datasize blob-header)))
                  (let ((blob-buf (make-array (osmpbf:datasize blob-header) :element-type '(unsigned-byte 8))))
                    (setf *last-block-processed* i)
                    (read-sequence blob-buf fs)
                    (let ((blob (make-instance 'osmpbf:blob)))
                      (pb:merge-from-array blob blob-buf 0 (length blob-buf))
                      ;;(print-blob-descr blob)
                      (handler-case
                          (let ((data (if (osmpbf:has-raw blob)
                                          (osmpbf:raw blob)
                                          (coerce (zlib:uncompress (osmpbf:zlib-data blob) :uncompressed-size (osmpbf:raw-size blob)) '(simple-array (unsigned-byte 8) (*))))))
                            (cond
                              ((string= (pb:string-value (osmpbf:type blob-header)) "OSMHeader")
                               (read-osm-header data))
                              ((string= (pb:string-value (osmpbf:type blob-header)) "OSMData")
                               (read-osm-data data)
                               (incf proc-count)
                               (when (and count (>= proc-count count))
                                 (return-from read-osm-file)))))
                        (condition (c) (format t "!!! data decode error ~A~%" c)))))))))))))

(defun read-osm-header (data)
  (let ((header (make-instance 'osmpbf:header-block)))
    (pb:merge-from-array header data 0 (length data))
    ;; (format t "header ======~%~A~%" header)
    ))

(defun read-string-table (st)
  (let ((st-converted (make-array (length st))))
    (loop for st-entry across st
         for i from 0
         do (setf (aref st-converted i)
                  (sb-ext:octets-to-string
                   (coerce st-entry
                           '(simple-array (unsigned-byte 8) (*))))))
    st-converted))

(defstruct node
  (id 0 :type (unsigned-byte 64))
  (lon 0 :type (unsigned-byte 64))
  (lat 0 :type (unsigned-byte 64))
  (tags nil :type list))

(defstruct way
  (id 0 :type (unsigned-byte 64))
  (refs (make-array 0 :element-type '(unsigned-byte 64)) :type (simple-array (unsigned-byte 64) (*)))
  (tags nil :type list))

(defun find-tag (tags key)
  (dolist (tag tags)
    (when (string= (car tag) key)
      (return-from find-tag tag)))
  nil)

(defun read-keys-values-arr (kv-arr st-arr nodes)
  "read key-value array and populate unpacked nodes with their attributes"
  (do ((node-idx 0)
       (kv-idx 0)
       (kv-size (length kv-arr)))
      ((>= kv-idx kv-size))
    (let ((key (aref kv-arr kv-idx)))
      (incf kv-idx)
      (if (zerop key)
          (incf node-idx)
          (let ((value (aref kv-arr kv-idx)))
            (incf kv-idx)
            (let ((node (aref nodes node-idx)))
              (setf (node-tags node)
                    (cons (cons (aref st-arr key)
                                (aref st-arr value))
                          (node-tags node)))))))))

(defun unpack-dense (dn st-arr)
  (let ((prev-node nil)
        (nodes (make-array (length (osmpbf:id dn)) :element-type 'node :initial-element (make-node))))
    (loop for id across (osmpbf:id dn)
         for lon across (osmpbf:lon dn)
         for lat across (osmpbf:lat dn)
         for i from 0
         do (let ((node (make-node)))
              (if prev-node
                  (progn
                    (setf (node-id node) (+ id (node-id prev-node))
                          (node-lat node) (+ lat (node-lat prev-node))
                          (node-lon node) (+ lon (node-lon prev-node))))
                  (setf (node-id node) id
                        (node-lat node) lat
                        (node-lon node) lon))
              (setf (aref nodes i) node
                    prev-node node)))
    (read-keys-values-arr (osmpbf:keys-vals dn) st-arr nodes)
    nodes))

(defun read-ways (ways st-arr)
  (let ((new-ways (make-array (length ways) :element-type 'way :initial-element (make-way))))
    (loop for way across ways
         for i from 0
         do (let ((new-way (make-way :id (osmpbf:id way)
                                 :refs (make-array (length (osmpbf:refs way)) :element-type '(unsigned-byte 64) :initial-element 0)))
                  (prev-ref 0))
              (loop for rr across (osmpbf:refs way)
                   for j from 0
                   do (let ((new-ref (+ prev-ref rr)))
                        (setf (aref (way-refs new-way) j) new-ref
                              prev-ref new-ref)))
              (loop for key across (osmpbf:keys way)
                   for val across (osmpbf:vals way)
                   do (setf (way-tags new-way)
                            (cons (cons (aref st-arr key) (aref st-arr val))
                                  (way-tags new-way))))
              (let ((name (cdr (find-tag (way-tags new-way) "name"))))
                (when (and (find-tag (way-tags new-way) "boundary")
                           (not (find-tag (way-tags new-way) "waterway"))
                           (> (length (way-tags new-way)) 2))
                  (when (= (aref (way-refs new-way) 0)
                           (aref (way-refs new-way) (1- (length (way-refs new-way)))))
                    (format t "===:="))
                  (let ((*print-pretty* nil))
                    (format t "~A ~A~%" name (way-tags new-way)))))
              (setf (aref new-ways i) new-way)))
    new-ways))

(defvar *last-dense* nil)
(defvar *last-ways* nil)
(defvar *last-string-table* nil)

(defun read-osm-data (data)
  (let ((pblock (make-instance 'osmpbf:primitive-block)))
    (pb:merge-from-array pblock data 0 (length data))
    ;; (format t "pblock ======~%")
    ;; (format t "granularity ~A~%" (osmpbf:granularity pblock))
    ;; (format t "lat-offset ~A~%" (osmpbf:lat-offset pblock))
    ;; (format t "lon-offset ~A~%" (osmpbf:lon-offset pblock))
    (let ((string-table (read-string-table (osmpbf:s (osmpbf:stringtable pblock)))))
      ;;(format t "string-table with ~A entries~%" (length string-table))
      (setf *last-string-table* string-table)
      (let ((pgroup-arr (osmpbf:primitivegroup pblock)))
        (loop for pgroup across pgroup-arr
           for pgidx from 0
           do
             (progn
               (when (osmpbf:has-dense pgroup)
                 (setf *last-dense* (osmpbf:dense pgroup)))
               (unless (zerop (length (osmpbf:ways pgroup)))
                 (setf *last-ways* (osmpbf:ways pgroup)))
               
               (read-ways (osmpbf:ways pgroup) string-table)
               
               ;; (format t " pgroup ~A ==~%  nodes ~A~%  dense ~A~%  ways ~A~%  relations ~A~%  changesets ~A~%"
               ;;         pgidx
               ;;         (length (osmpbf:nodes pgroup))
               ;;         (if (osmpbf:has-dense pgroup)
               ;;             (length (osmpbf:id (osmpbf:dense pgroup)))
               ;;             0)
               ;;         (length (osmpbf:ways pgroup))
               ;;         (length (osmpbf:relations pgroup))
               ;;         (length (osmpbf:changesets pgroup)))
               ))))))
