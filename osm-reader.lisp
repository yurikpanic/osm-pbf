(defpackage :osm-reader
  (:use :cl
        :b-tree
        :osm-writer
        :in-mem-str))

(in-package :osm-reader)

(defun print-blob-descr (blob &optional (stream t))
  (format stream "blob")
  (if (osmpbf:has-raw blob)
      (format stream " raw data (size ~A)" (length (osmpbf:raw blob)))
      (when (osmpbf:has-zlib-data blob)
        (format stream " zlib data (size ~A, uncompressed ~A)" (length (osmpbf:zlib-data blob)) (osmpbf:raw-size blob))))
  (format stream "~%"))

(defvar *last-block-processed* nil)

(defun read-osm-file (&key (file-name #p"/home/yuri/work/globus/osm/ukraine.osm.pbf") (skip-to-block 0) count process-only)
  (with-open-file (fs file-name :direction :input :element-type 'unsigned-byte)
    (do ((i 0 (1+ i))
         (file-size (file-length fs))
         (file-pos 0 (file-position fs))
         (proc-count 0))
        ((>= file-pos file-size) nil)
      (let ((blob-header-len-buf (make-array 4 :element-type '(unsigned-byte 8))))
        (read-sequence blob-header-len-buf fs)
        (format t "================================= ~A :~A [~,1F %]~%" i file-pos (* (/ file-pos file-size) 100.0))
        ;;(format t "blob-header-len-buf ~A~%" blob-header-len-buf)
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
                               (read-osm-data data process-only)
                               (incf proc-count)
                               (when (and count (>= proc-count count))
                                 (return-from read-osm-file)))))
                        (condition (c) (progn
                                         (format t "!!! data decode error ~A ~A~%" c (osmpbf:raw-size blob))
                                         (with-open-file (bb (format nil "/home/yuri/work/globus/osm/zerror-~D" i) :element-type '(unsigned-byte 8) :direction :output :if-exists :supersede)
                                           (write-sequence (osmpbf:zlib-data blob) bb)))))))))))))))

(defvar *orig-header* nil)

(defun read-osm-header (data)
  (let ((header (make-instance 'osmpbf:header-block)))
    (pb:merge-from-array header data 0 (length data))
    ;;(format t "header ======~%~A~%" header)
    (setf *orig-header* header)
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

(defvar *ways-to-dump* (make-hash-table :test 'eq))
(defvar *nodes-to-dump* (make-hash-table :test 'eq))
(defvar *nodes-dump-reverse-order* nil)
(defvar *ways-dump-reverse-order* nil)
(defvar *relations-dump-reverse-order* nil)

(defvar *nodes-btree* (make-empty-btree 20))
(defvar *ways-btree* (make-empty-btree 20))
(defvar *relations-btree* (make-empty-btree 20))

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
              (when (gethash (node-id node) *nodes-to-dump*)
                (binsert *nodes-btree* (node-id node) node))
              (setf (aref nodes i) node
                    prev-node node)))
    (read-keys-values-arr (osmpbf:keys-vals dn) st-arr nodes)
    nodes))

(defun read-keyval-parr (keys vals st)
  "read parallel array of keys and values and form a list of cons cells containing corresponding strings from string table"
  (let ((res nil))
    (loop for key across keys
         for val across vals
         do (setf res
                  (cons (cons (aref st key) (aref st val))
                        res)))
    res))

(defun read-ways (ways st-arr)
  (let ((new-ways (make-array (length ways) :element-type 'way :initial-element (make-way))))
    (loop for way across ways
         for i from 0
         do (let ((new-way (make-way :id (osmpbf:id way)
                                     :tags (read-keyval-parr (osmpbf:keys way) (osmpbf:vals way) st-arr)
                                     :refs (make-array (length (osmpbf:refs way)) :element-type '(unsigned-byte 64) :initial-element 0)))
                  (prev-ref 0))
              (loop for rr across (osmpbf:refs way)
                   for j from 0
                   do (let ((new-ref (+ prev-ref rr)))
                        (when (gethash (way-id new-way) *ways-to-dump*)
                          (unless (gethash new-ref *nodes-to-dump*)
                            (setf *nodes-dump-reverse-order* (cons new-ref *nodes-dump-reverse-order*)))
                          (setf (gethash new-ref *nodes-to-dump*) t))
                        (setf (aref (way-refs new-way) j) new-ref
                              prev-ref new-ref)))
              (when (gethash (way-id new-way) *ways-to-dump*)
                (binsert *ways-btree* (way-id new-way) new-way))
              (setf (aref new-ways i) new-way)))
    new-ways))

(defun read-relations (rels st-arr)
  (let ((new-rels (make-array (length rels) :element-type 'relation :initial-element (make-relation))))
    (loop for rel across rels
         for i from 0
         do (let ((new-rel (make-relation :id (osmpbf:id rel) :tags (read-keyval-parr (osmpbf:keys rel) (osmpbf:vals rel) st-arr))))
              (unless (zerop (length (osmpbf:roles-sid rel)))
                (setf (relation-members new-rel) (make-array (length (osmpbf:roles-sid rel)) :element-type 'rel-member :initial-element (make-rel-member)))
                (let ((prev-mem-id 0))
                  (loop for role-sid across (osmpbf:roles-sid rel)
                     for memid across (osmpbf:memids rel)
                     for type across (osmpbf:types rel)
                     for j from 0
                     do (let ((new-rel-member (make-rel-member :id (+ prev-mem-id memid)
                                                               :type type
                                                               :role (aref st-arr role-sid))))
                          (setf prev-mem-id (rel-member-id new-rel-member)
                                (aref (relation-members new-rel) j) new-rel-member)))))
              (setf (aref new-rels i) new-rel)
              (when (equal (cdr (find-tag (relation-tags new-rel) "boundary")) "administrative")
                (loop for member across (relation-members new-rel)
                   do (progn
                        (when (= (rel-member-type member) 1)
                          (unless (gethash (rel-member-id member) *ways-to-dump*)
                            (setf *ways-dump-reverse-order* (cons (rel-member-id member) *ways-dump-reverse-order*)))
                          (setf (gethash (rel-member-id member) *ways-to-dump*) t))
                        (when (= (rel-member-type member) 0)
                          (unless (gethash (rel-member-id member) *nodes-to-dump*)
                            ;; add node to list only if it is not yet in hash - some nodes belong to many relations
                            (setf *nodes-dump-reverse-order* (cons (rel-member-id member) *nodes-dump-reverse-order*)))
                          (setf (gethash (rel-member-id member) *nodes-to-dump*) t))))
                (binsert *relations-btree* (relation-id new-rel) new-rel)
                (setf *relations-dump-reverse-order* (cons (relation-id new-rel) *relations-dump-reverse-order*)))))
    new-rels))

(defvar *last-dense* nil)
(defvar *last-ways* nil)
(defvar *last-relations* nil)
(defvar *last-string-table* nil)

(defun read-osm-data (data process-only)
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
               (unless (zerop (length (osmpbf:relations pgroup)))
                 (setf *last-relations* (osmpbf:relations pgroup)))

               (when (or (not process-only) (eq process-only :nodes))
                 (when (osmpbf:has-dense pgroup)
                   (unpack-dense (osmpbf:dense pgroup) string-table)))

               (when (or (not process-only) (eq process-only :ways))
                 (read-ways (osmpbf:ways pgroup) string-table))

               (when (or (not process-only) (eq process-only :relations))
                 (read-relations (osmpbf:relations pgroup) string-table))
               
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

(defvar *nodes-to-save* nil
  "A list of node IDs which are already serialized to PrimitiveBlock, but not yet saved to Blob.
Used to adjust the offset of particular node data in blob.")
(defvar *ways-to-save* nil)
(defvar *relations-to-save* nil)

(defmacro update-pb-offsets (type index)
  (let* ((in-mem (gensym))
         (type (string-upcase (if (typep type 'symbol) (symbol-name type) type)))
         (save-list (intern (format nil "*~AS-TO-SAVE*" type))))
    `(let ((,in-mem (bsearch ,(intern (format nil "*~AS-BTREE*" type)) (osmpbf:id self))))
       (when ,in-mem
         (setf (,(intern (format nil "~A-OFFS-IN-BLOB" type)) ,in-mem) ,index
               (,(intern (format nil "~A-PB-SIZE" type)) ,in-mem) (pb:octet-size self)
               ,save-list (cons (osmpbf:id self) ,save-list))))))

;;; compute the offset of particular node in buffer (offset from begining of PrimitiveBlock)
(defmethod pb:serialize :before ((self osmpbf:node) buffer index limit)
  (update-pb-offsets :node index))

(defmethod pb:serialize :before ((self osmpbf:way) buffer index limit)
  (update-pb-offsets :way index))

(defmethod pb:serialize :before ((self osmpbf:relation) buffer index limit)
  (update-pb-offsets :relation index))

;;; Adjust the offset of each node serialized to PrimitiveBlock to be the offset from beginning of Blob
;;; Code here is somewhat error prone, it will break if we'll have extra data except raw in Blob (like raw_size or zlib_data).
;;; But this is sufficient for current case, without modifying the guts of protobuf library.
(defmethod pb:serialize :before ((self osmpbf:blob) buffer index limit)
  (let ((offs-delta (- (pb:octet-size self) (length (osmpbf:raw self)))))
    (macrolet ((update-items-list (list btree offs-field)
                 (let ((id (gensym))
                       (item (gensym)))
                   `(dolist (,id ,list)
                      (let ((,item (bsearch ,btree ,id)))
                        (when ,item
                          (incf (,offs-field ,item) offs-delta)))))))
      (update-items-list *nodes-to-save* *nodes-btree* node-offs-in-blob)
      (update-items-list *ways-to-save* *ways-btree* way-offs-in-blob)
      (update-items-list *relations-to-save* *relations-btree* relation-offs-in-blob)))
  (setf *nodes-to-save* nil
        *ways-to-save* nil
        *relations-to-save* nil))

(defun update-bbox-by-node (bbox node)
  (macrolet ((set-if (op node-field bbox-field)
               `(when (,op (,node-field node) (,bbox-field bbox))
                  (setf (,bbox-field bbox) (,node-field node)))))
    (set-if < node-lon bbox-min-lon)
    (set-if < node-lat bbox-min-lat)
    (set-if > node-lon bbox-max-lon)
    (set-if > node-lat bbox-max-lat)
    bbox))

(defun calc-way-bbox (way)
  (let ((bbox (make-bbox)))
    (loop for node-id across (way-refs way)
         do (let ((node (bsearch *nodes-btree* node-id)))
              (when node (update-bbox-by-node bbox node))))
    bbox))

(defparameter *way-bboxes-btree* (make-empty-btree 20))

(defun make-way-bboxes ()
  (loop for way-id in *ways-dump-reverse-order*
       do (let ((way (bsearch *ways-btree* way-id)))
            (when way
              (binsert *way-bboxes-btree* (way-id way)
                       (calc-way-bbox way)))))
  (btree-size *way-bboxes-btree*))

(defparameter *relation-bboxes-btree* (make-empty-btree 20))

(defun make-relation-bboxes ()
  (loop for rel-id in *relations-dump-reverse-order*
       do (let ((rel (bsearch *relations-btree* rel-id)))
            (when rel
              (let ((bbox nil))
                (loop for mem across (relation-members rel)
                     do (when (eq (rel-member-type mem) 1)
                          (let ((way-bbox (or (bsearch *way-bboxes-btree* (rel-member-id mem))
                                              (let ((way (bsearch *ways-btree* (rel-member-id mem))))
                                                (when way (calc-way-bbox way))))))
                            (when way-bbox
                              (if bbox
                                  (extend-bbox bbox way-bbox)
                                  (setf bbox (copy-bbox way-bbox)))))))
                (when bbox (binsert *relation-bboxes-btree* rel-id bbox))))))
  (btree-size *relation-bboxes-btree*))

(defun clear-collected-data ()
  (clrhash *nodes-to-dump*)
  (clrhash *ways-to-dump*)
  (setf *nodes-dump-reverse-order* nil
        *ways-dump-reverse-order* nil
        *relations-dump-reverse-order* nil
        *nodes-btree* (make-empty-btree 20)
        *ways-btree* (make-empty-btree 20)
        *relations-btree* (make-empty-btree 20)
        *way-bboxes-btree* (make-empty-btree 20)
        *relation-bboxes-btree* (make-empty-btree 20))
  (values))

(defun save-collected-data ()
  (let ((w (begin-write :bbox (osmpbf:bbox *orig-header*))))
    (macrolet ((loop-through-items (rev-list btree)
                  (let ((id (gensym))
                        (item (gensym)))
                    `(dolist (,id (reverse ,rev-list))
                       (let ((,item (bsearch ,btree ,id)))
                         (when ,item
                           (write-item w ,item)))))))
      (loop-through-items *nodes-dump-reverse-order* *nodes-btree*)
      (loop-through-items *ways-dump-reverse-order* *ways-btree*)
      (loop-through-items *relations-dump-reverse-order* *relations-btree*))
    (flush-write w)
    (write-btree w *nodes-btree* #'make-index-arr "node" "id")
    (write-btree w *ways-btree* #'make-index-arr "way" "id")
    (write-btree w *relations-btree* #'make-index-arr "relation" "id")
    (write-btree w *way-bboxes-btree* #'make-bbox-leaf "way-bbox" "id")
    (write-btree w *relation-bboxes-btree* #'make-bbox-leaf "relation-bbox" "id")
    (end-write w)))