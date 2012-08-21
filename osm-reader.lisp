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

(defun read-osm-file (&optional (file-name #p"/home/yuri/work/globus/osm/UA.osm.pbf"))
  (with-open-file (fs file-name :direction :input :element-type 'unsigned-byte)
    (do ((i 0 (1+ i))
         (file-size (file-length fs))
         (file-pos 0 (file-position fs))
         (nodes nil))
        ((>= file-pos file-size) (values nodes))
      (let ((blob-header-len-buf (make-array 4 :element-type '(unsigned-byte 8))))
        (read-sequence blob-header-len-buf fs)
        (format t "================================= :~A [~,1F %]~%blob-header-len-buf ~A~%" file-pos (* (/ file-pos file-size) 100.0) blob-header-len-buf)
        (let ((blob-header-len 0))
          (loop for d across blob-header-len-buf
             do (setf blob-header-len (logior (ash blob-header-len 8) d)))
          (format t "blob-header-len ~A~%" blob-header-len)
          (let ((blob-header-buf (make-array blob-header-len :element-type '(unsigned-byte 8))))
            (read-sequence blob-header-buf fs)
            (let ((blob-header (make-instance 'osmpbf:blob-header)))
              (pb:merge-from-array blob-header blob-header-buf 0 blob-header-len)
              (format t "blob-header ~A~%" blob-header)
              (let ((blob-buf (make-array (osmpbf:datasize blob-header) :element-type '(unsigned-byte 8))))
                (read-sequence blob-buf fs)
                (let ((blob (make-instance 'osmpbf:blob)))
                  (pb:merge-from-array blob blob-buf 0 (length blob-buf))
                  (print-blob-descr blob)
                  (handler-case
                      (let ((data (if (osmpbf:has-raw blob)
                                      (osmpbf:raw blob)
                                      (coerce (zlib:uncompress (osmpbf:zlib-data blob) :uncompressed-size (osmpbf:raw-size blob)) '(simple-array (unsigned-byte 8) (*))))))
                        (cond
                          ((string= (pb:string-value (osmpbf:type blob-header)) "OSMHeader")
                           (read-osm-header data))
                          ((string= (pb:string-value (osmpbf:type blob-header)) "OSMData")
                           (setf nodes (read-osm-data data i nodes))
                           (when (>= i 0)
                             (return-from read-osm-file (values nodes)))
                           )))
                    (condition (c) (format t "!!! data decode error ~A~%" c))))))))))))

(defun read-osm-header (data)
  (let ((header (make-instance 'osmpbf:header-block)))
    (pb:merge-from-array header data 0 (length data))
    (format t "header ======~%~A~%" header)))

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
  (st-ref #() :type array)
  (tags nil :type list))

(defun print-node (node &optional (stream t))
  (format stream ":id ~A :lon ~A :lat ~A" (node-id node) (node-lon node) (node-lat node))
  (dolist (tag (node-tags node))
    (format stream "~%    ~A . ~A"
            (aref (node-st-ref node) (car tag))
            (aref (node-st-ref node) (cdr tag))))
  (format stream "~%"))

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
              (setf (node-st-ref node) st-arr
                    (node-tags node) (cons (cons key value) (node-tags node)))))))))

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

(defun read-osm-data (data idx &optional nodes)
  (let ((pblock (make-instance 'osmpbf:primitive-block)))
    (pb:merge-from-array pblock data 0 (length data))
    (format t "pblock ~A ======~%" idx)
    (format t "granularity ~A~%" (osmpbf:granularity pblock))
    (format t "lat-offset ~A~%" (osmpbf:lat-offset pblock))
    (format t "lon-offset ~A~%" (osmpbf:lon-offset pblock))
    (let ((string-table (read-string-table (osmpbf:s (osmpbf:stringtable pblock)))))
      (format t "string-table with ~A entries~%" (length string-table))
      (let ((pgroup-arr (osmpbf:primitivegroup pblock)))
        (loop for pgroup across pgroup-arr
           for pgidx from 0
           do
             (format t " pgroup ~A ==~%  nodes ~A~%  dense ~A~%  ways ~A~%  relations ~A~%  changesets ~A~%"
                     pgidx
                     (length (osmpbf:nodes pgroup))
                     (if (osmpbf:has-dense pgroup)
                         (progn
                           (setf nodes
                                 (cons
                                  (unpack-dense (osmpbf:dense pgroup) string-table)
                                  nodes))
                           (length (osmpbf:id (osmpbf:dense pgroup))))
                         0)
                     (length (osmpbf:ways pgroup))
                     (length (osmpbf:relations pgroup))
                     (length (osmpbf:changesets pgroup))))
        nodes))))
