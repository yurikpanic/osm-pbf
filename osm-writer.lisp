(defpackage :osm-writer
  (:use :cl
        :b-tree
        :in-mem-str)
  (:export :begin-write
           :flush-write
           :end-write
           :write-blob
           :write-node
           :write-btree))

(in-package :osm-writer)

(defconstant +def-items-per-page+ 8000)

(defstruct write-descr
  (file-name #p"/home/yuri/work/globus/osm/out.osm.pbf" :type pathname)
  (stream *standard-output* :type stream)
  (blob-num 0 :type integer)
  (node-idx 0 :type integer)
  (st-hash (make-hash-table :test 'equal) :type hash-table)
  (st-hash-by-pos (make-hash-table :test 'eq) :type hash-table)
  (st-count 0 :type integer)
  (pgroup (make-instance 'osmpbf:primitive-group) :type osmpbf:primitive-group))

(defun blob-elts-count (wd)
  ;; TODO: add other data types here when they be available
  (+ (write-descr-node-idx wd)))

(defun write-blob (wd data &key (type "OSMData"))
  (let ((blob-header (make-instance 'osmpbf:blob-header))
        (blob (make-instance 'osmpbf:blob)))
    (setf (osmpbf:raw blob) data)
    (setf (osmpbf:type blob-header) (pb:string-field type)
          (osmpbf:datasize blob-header) (pb:octet-size blob))
    (let* ((size (pb:octet-size blob-header))
           (buf (make-array size :element-type '(unsigned-byte 8))))
      (pb:serialize blob-header buf 0 size)
      (loop for i from 3 downto 0
           do (write-byte (logand #xFF (ash size (* -8 i))) (write-descr-stream wd)))
      (write-sequence buf (write-descr-stream wd))
      (let* ((size (pb:octet-size blob))
             (buf (make-array size :element-type '(unsigned-byte 8))))
        (pb:serialize blob buf 0 size)
        (write-sequence buf (write-descr-stream wd)))
      (incf (write-descr-blob-num wd))
      (setf (write-descr-st-hash wd) (make-hash-table :test 'equal)
            (write-descr-st-hash-by-pos wd) (make-hash-table :test 'eq)
            (write-descr-node-idx wd) 0
            (write-descr-st-count wd) 0
            (write-descr-pgroup wd) (make-instance 'osmpbf:primitive-group))
      wd)))

(defun begin-write (&key file-name bbox)
  (let ((wd (make-write-descr)))
    (when file-name (setf (write-descr-file-name wd) file-name))
    (setf (write-descr-stream wd)
          (open (write-descr-file-name wd) :direction :output :element-type 'unsigned-byte :if-exists :supersede))
    (let ((header (make-instance 'osmpbf:header-block))
          (bb (if (typep bbox 'osmpbf:header-b-box)
                  bbox
                  (make-instance 'osmpbf:header-b-box))))
      (unless (typep bbox 'osmpbf:header-b-box)
        (setf (osmpbf:left bb) (first bbox)
              (osmpbf:right bb) (second bbox)
              (osmpbf:top bb) (third bbox)
              (osmpbf:bottom bb) (fourth bbox)))
      (setf (osmpbf:bbox header) bb)
      (vector-push-extend (pb:string-field "OsmSchema-V0.6") (osmpbf:required-features header))
      (setf (osmpbf:writingprogram header) (pb:string-field "cl-osm-pbf"))
      (let* ((size (pb:octet-size header))
             (buf (make-array size :element-type '(unsigned-byte 8))))
        (pb:serialize header buf 0 size)
        (write-blob wd buf :type "OSMHeader")
        wd))))

(defun make-string-table (wd)
  (let ((st (make-instance 'osmpbf:string-table)))
    (vector-push-extend (coerce #() '(simple-array (unsigned-byte 8) (*))) (osmpbf:s st))
    (loop for i from 1 to (write-descr-st-count wd)
         do (vector-push-extend (sb-ext:string-to-octets
                                 (gethash i (write-descr-st-hash-by-pos wd)))
                                (osmpbf:s st)))
    st))

(defun flush-write (wd)
  (when (or (> (write-descr-st-count wd) 0) (> (write-descr-node-idx wd) 0))
    (let ((pblock (make-instance 'osmpbf:primitive-block))
          (st (make-string-table wd)))
      (setf (osmpbf:stringtable pblock) st)
      (vector-push-extend (write-descr-pgroup wd) (osmpbf:primitivegroup pblock))
      (let* ((size (pb:octet-size pblock))
             (buf (make-array size :element-type '(unsigned-byte 8))))
        (pb:serialize pblock buf 0 size)
        (let ((ppb (make-instance 'osmpbf:primitive-block)))
          (pb:merge-from-array ppb buf 0 size))
        (write-blob wd buf)))))

(defun end-write (wd)
  (unless (zerop (blob-elts-count wd))
    (flush-write wd))
  (close (write-descr-stream wd)))

(defun st-entry (wd string)
  (let ((hashed-id (gethash string (write-descr-st-hash wd))))
    (or hashed-id
        (let ((new-id (incf (write-descr-st-count wd))))
          (setf (gethash string (write-descr-st-hash wd)) new-id
                (gethash new-id (write-descr-st-hash-by-pos wd)) string)
          new-id))))

(defun update-string-table (wd tags)
  (let ((res nil))
    (dolist (tag tags)
      (let ((key-id (st-entry wd (car tag)))
            (val-id (st-entry wd (cdr tag))))
        (setf res (cons (cons key-id val-id) res))))
    (nreverse res)))

(defun make-pbnode (node)
  (let ((pbnode (make-instance 'osmpbf:node)))
    (setf (osmpbf:id pbnode) (node-id node)
          (osmpbf:lat pbnode) (node-lat node)
          (osmpbf:lon pbnode) (node-lon node))
    (dolist (tag-kv (node-tags-st node))
      (vector-push-extend (car tag-kv) (osmpbf:keys pbnode))
      (vector-push-extend (cdr tag-kv) (osmpbf:vals pbnode)))
    pbnode))

(defun write-node (wd node)
  (setf (node-tags-st node) (update-string-table wd (node-tags node)))
  (let ((pbnode (make-pbnode node)))
    (setf (node-blob-num node) (write-descr-blob-num wd)
          (node-blob-elt node) (write-descr-node-idx wd))
    (incf (write-descr-node-idx wd))
    (vector-push-extend pbnode
                        (osmpbf:nodes (write-descr-pgroup wd))))
  (when (>= (blob-elts-count wd) +def-items-per-page+)
    (flush-write wd))
  wd)

;; (defun bnode-to-pbf (pbtree node serialize-values-fn)
;;   (let ((pbnode (make-instance 'btreepbf:bnode)))
;;     (when (eq (bnode-kind node) :node)
;;       (setf (btreepbf:kind pbnode) btreepbf:+bnode-kind-node+))
;;     (loop for key across (bnode-keys node)
;;          for i from 0 below (bnode-size node)
;;          do (vector-push-extend key (btreepbf:keys pbnode)))
;;     (let ((nodes-arr-idx (vector-push-extend pbnode (btreepbf:nodes pbtree))))
;;       (when (eq (bnode-kind node) :node)
;;         (loop for pnt across (bnode-pointers node)
;;              for i from 0 to (bnode-size node)
;;              do (vector-push-extend (bnode-to-pbf pbtree (car pnt) serialize-values-fn) (btreepbf:pointers pbnode))))
;;       (when (eq (bnode-kind node) :leaf)
;;         (loop for i from 1 to (bnode-size node)
;;            do (let ((val (car (aref (bnode-pointers node) i))))
;;                 (vector-push-extend (funcall serialize-values-fn val) (btreepbf:values pbnode)))))
;;       nodes-arr-idx)))

;; (defun btree-to-pbf (tree serialize-values-fn &optional type-str)
;;   (let ((pbtree (make-instance 'btreepbf:btree)))
;;     (setf (btreepbf:max-children pbtree) (btree-max-children tree))
;;     (when type-str (setf (btreepbf:type pbtree) (pb:string-field type-str)))
;;     (bnode-to-pbf pbtree (car (btree-root tree)) serialize-values-fn)
;;     pbtree))

(defun bnode-subtree-prepare-pbf (node serialize-values-fn node-num)
  (let ((pbnode (make-instance 'btreepbf:bnode)))
    (setf (bnode-data node) (list node-num pbnode))
    (loop for key across (bnode-keys node)
       for i from 0 below (bnode-size node)
       do (vector-push-extend key (btreepbf:keys pbnode)))
    (when (eq (bnode-kind node) :node)
      (setf (btreepbf:kind pbnode) btreepbf:+bnode-kind-node+)
      (loop for pnt across (bnode-pointers node)
           for i from 0 to (bnode-size node)
           do (when pnt
                (incf node-num)
                (vector-push-extend node-num (btreepbf:pointers pbnode))
                (setf node-num (bnode-subtree-prepare-pbf (car pnt) serialize-values-fn node-num)))))
    (when (eq (bnode-kind node) :leaf)
      (loop for i from 1 to (bnode-size node)
           do (let ((val (car (aref (bnode-pointers node) i))))
                (vector-push-extend (funcall serialize-values-fn val) (btreepbf:values pbnode)))))
    node-num))

(defun btree-prepare-pbf (tree serialize-values-fn)
  "For every bnode in btree construct btreepbf:bnode structure and store it in data field of bnode"
  (bnode-subtree-prepare-pbf (car (btree-root tree)) serialize-values-fn 0)
  (values))

(defconstant +btree-chunk-size+ 131072)

(defun save-btree-chunk (wd btree-index pbtree &optional final-chunk)
  (let* ((size (pb:octet-size pbtree))
         (buf (make-array size :element-type '(unsigned-byte 8)))
         (part-entry (make-instance 'btreepbf:btree-part-entry)))
    (format t "saving btree part [~A] ~A~%" size (aref (btreepbf:entries btree-index) (1- (length (btreepbf:entries btree-index)))))
    (pb:serialize pbtree buf 0 size)
    (write-blob wd buf :type "btree")
    (setf (fill-pointer (btreepbf:nodes pbtree)) 0)
    (unless final-chunk
      (setf (btreepbf:blob-num part-entry) (write-descr-blob-num wd))
      (vector-push-extend part-entry (btreepbf:entries btree-index)))))

(defun walk-save-bnodes (wd btree-index pbtree node)
  (vector-push-extend (second (bnode-data node)) (btreepbf:nodes pbtree))
  (let ((last-part (aref (btreepbf:entries btree-index) (1- (length (btreepbf:entries btree-index))))))
    (unless (btreepbf:has-min-node last-part)
      (setf (btreepbf:min-node last-part) (first (bnode-data node))))
    (setf (btreepbf:max-node last-part) (first (bnode-data node)))
    (when (> (pb:octet-size pbtree) +btree-chunk-size+)
      (save-btree-chunk wd btree-index pbtree)))
  (when (eq (bnode-kind node) :node)
      (loop for pnt across (bnode-pointers node)
           for i from 0 to (bnode-size node)
           do (walk-save-bnodes wd btree-index pbtree (car pnt)))))

(defun save-btree-index (wd btree-index)
  (let* ((size (pb:octet-size btree-index))
         (buf (make-array size :element-type '(unsigned-byte 8))))
    (pb:serialize btree-index buf 0 size)
    (write-blob wd buf :type "btree-index")))

(defun write-btree (wd tree serialize-values-fn &optional type-str field-str)
  (let ((btree-index (make-instance 'btreepbf:btree-index))
        (pbtree (make-instance 'btreepbf:btree)))
    (when type-str
      (setf (btreepbf:type btree-index) (pb:string-field type-str))
      (when field-str
        (setf (btreepbf:field btree-index) (pb:string-field field-str))))
    (setf (btreepbf:max-children btree-index) (btree-max-children tree))
    (flush-write wd)
    (btree-prepare-pbf tree serialize-values-fn)
    (let ((part-entry (make-instance 'btreepbf:btree-part-entry)))
      (setf (btreepbf:blob-num part-entry) (write-descr-blob-num wd))
      (vector-push-extend part-entry (btreepbf:entries btree-index)))
    (walk-save-bnodes wd btree-index pbtree (car (btree-root tree)))
    (save-btree-chunk wd btree-index pbtree t)
    (btree-clear-nodes-data tree)
    (format t "btree-index ~A~%" btree-index)
    (save-btree-index wd btree-index)
    wd))
