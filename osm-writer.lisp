(defpackage :osm-writer
  (:use :cl
        :b-tree
        :in-mem-str)
  (:export :begin-write
           :flush-write
           :end-write
           :write-blob
           :write-node
           :write-way
           :write-item
           :write-btree))

(in-package :osm-writer)

(defconstant +def-items-per-page+ 8000)

(defstruct write-descr
  (file-name #p"/home/yuri/work/globus/osm/out.osm.pbf" :type pathname)
  (temp-file #p"/home/yuri/work/globus/osm/out.tmp" :type pathname)
  (stream *standard-output* :type stream)
  (blob-num 0 :type integer)
  (node-idx 0 :type integer)
  (way-idx 0 :type integer)
  (st-hash (make-hash-table :test 'equal) :type hash-table)
  (st-hash-by-pos (make-hash-table :test 'eq) :type hash-table)
  (st-count 0 :type integer)
  (pgroup (make-instance 'osmpbf:primitive-group) :type osmpbf:primitive-group))

(defun blob-elts-count (wd)
  ;; TODO: add other data types here when they be available
  (+ (write-descr-node-idx wd) (write-descr-way-idx wd)))

(defmacro write-uint32 (val stream)
  (let ((i (gensym)))
    `(loop for ,i from 3 downto 0
        do (write-byte (logand #xFF (ash ,val (* -8 ,i))) ,stream))))

(defun copy-stream (source dest &optional (buf-size (* 1024 1024)))
  (let ((copy-buf (make-array buf-size :element-type '(unsigned-byte 8))))
    (do ((bytes-read #1=(read-sequence copy-buf source) #1#))
        ((zerop bytes-read) (values))
      (write-sequence copy-buf dest :end bytes-read))))

(defun write-blob-raw (wd raw-data-list type)
  (let ((blob-header (make-instance 'osmpbf:blob-header))
        (data-len (loop for raw-data in raw-data-list
                       summing
                       (if (typep raw-data 'stream)
                           (file-length raw-data)
                           (length raw-data)))))
    (setf (osmpbf:type blob-header) (pb:string-field type)
          (osmpbf:datasize blob-header) data-len)
    (let* ((size (pb:octet-size blob-header))
           (buf (make-array size :element-type '(unsigned-byte 8))))
      (pb:serialize blob-header buf 0 size)
      (write-uint32 size (write-descr-stream wd))
      (write-sequence buf (write-descr-stream wd))
      (loop for raw-data in raw-data-list
           do
           (if (typep raw-data 'stream)
               (copy-stream raw-data (write-descr-stream wd))
               (write-sequence raw-data (write-descr-stream wd))))
      (incf (write-descr-blob-num wd))
      wd)))

(defun write-blob (wd data &key (type "OSMData"))
  (let ((blob (make-instance 'osmpbf:blob)))
    (setf (osmpbf:raw blob) data)
    (let* ((size (pb:octet-size blob))
           (buf (make-array size :element-type '(unsigned-byte 8))))
      (pb:serialize blob buf 0 size)
      (write-blob-raw wd (list buf) type)
      (setf (write-descr-st-hash wd) (make-hash-table :test 'equal)
            (write-descr-st-hash-by-pos wd) (make-hash-table :test 'eq)
            (write-descr-node-idx wd) 0
            (write-descr-way-idx wd) 0
            (write-descr-st-count wd) 0
            (write-descr-pgroup wd) (make-instance 'osmpbf:primitive-group))
      wd)))

(defun begin-write (&key file-name bbox temp-file)
  (let ((wd (make-write-descr)))
    (when file-name (setf (write-descr-file-name wd) file-name))
    (when temp-file (setf (write-descr-temp-file wd) temp-file))
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
  (when (or (> (write-descr-st-count wd) 0) (> (blob-elts-count wd) 0))
    (let ((pblock (make-instance 'osmpbf:primitive-block))
          (st (make-string-table wd)))
      (setf (osmpbf:stringtable pblock) st)
      (vector-push-extend (write-descr-pgroup wd) (osmpbf:primitivegroup pblock))
      (let* ((size (pb:octet-size pblock))
             (buf (make-array size :element-type '(unsigned-byte 8))))
        (pb:serialize pblock buf 0 size)
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

(defun copy-tags-to-pb-obj (kv-list pbobj)
  (dolist (tag-kv kv-list)
    (vector-push-extend (car tag-kv) (osmpbf:keys pbobj))
    (vector-push-extend (cdr tag-kv) (osmpbf:vals pbobj))))

(defun make-pbnode (node)
  (let ((pbnode (make-instance 'osmpbf:node)))
    (setf (osmpbf:id pbnode) (node-id node)
          (osmpbf:lat pbnode) (node-lat node)
          (osmpbf:lon pbnode) (node-lon node))
    (copy-tags-to-pb-obj (node-tags-st node) pbnode)
    pbnode))

(defun write-node (wd node)
  (setf (node-tags-st node) (update-string-table wd (node-tags node)))
  (let ((pbnode (make-pbnode node)))
    (setf (node-blob-num node) (write-descr-blob-num wd))
    (incf (write-descr-node-idx wd))
    (vector-push-extend pbnode
                        (osmpbf:nodes (write-descr-pgroup wd))))
  (when (>= (blob-elts-count wd) +def-items-per-page+)
    (flush-write wd))
  wd)

(defun make-pbway (way)
  (let ((pbway (make-instance 'osmpbf:way)))
    (setf (osmpbf:id pbway) (way-id way))
    (let ((prev-ref nil))
      (loop for ref across (way-refs way)
         do (vector-push-extend (if prev-ref (- ref prev-ref) ref) (osmpbf:refs pbway))
         do (setf prev-ref ref)))
    (copy-tags-to-pb-obj (way-tags-st way) pbway)
    pbway))

(defun write-way (wd way)
  (setf (way-tags-st way) (update-string-table wd (way-tags way)))
  (let ((pbway (make-pbway way)))
    (setf (way-blob-num way) (write-descr-blob-num wd))
    (incf (write-descr-way-idx wd))
    (vector-push-extend pbway
                        (osmpbf:ways (write-descr-pgroup wd))))
  (when (>= (blob-elts-count wd) +def-items-per-page+)
    (flush-write wd))
  wd)

(defgeneric write-item (wd item)
  (:method (wd (item way))
    (write-way wd item))
  (:method (wd (item node))
    (write-node wd item)))

(defun write-btree (wd tree serialize-values-fn &optional type-str field-str)
  (flush-write wd)
  (let ((root-pos nil)
        (root-size nil))
    (with-open-file (fs (write-descr-temp-file wd) :direction :output :element-type '(unsigned-byte 8) :if-exists :supersede)
      (multiple-value-bind (pos size)
          (write-btree-tmp fs tree serialize-values-fn)
        (setf root-pos pos root-size size)))
    (when (and root-size root-pos)
      (let ((pbtree (make-instance 'btreepbf:btree)))
        (when type-str
          (setf (btreepbf:type pbtree) (pb:string-field type-str))
          (when field-str
            (setf (btreepbf:field pbtree) (pb:string-field field-str))))
        (setf (btreepbf:root-offs pbtree) root-pos
              (btreepbf:root-size pbtree) root-size)
        (let* ((size (pb:octet-size pbtree))
               (buf (make-array size :element-type '(unsigned-byte 8)))
               (len-buf (make-array 4 :element-type '(unsigned-byte 8))))
          (pb:serialize pbtree buf 0 size)
          (loop for i from 3 downto 0
               for j from 0 to 3
             do (setf (aref len-buf j) (logand #xFF (ash size (* -8 i)))))
          (with-open-file (fs (write-descr-temp-file wd) :direction :input :element-type '(unsigned-byte 8))
            (write-blob-raw wd (list len-buf buf fs) "btree")))))))

(defun walk-save-bnodes-tmp (fs node serialize-values-fn)
  (flet ((save-node-to-stream (fs pbnode)
           (declare (type btreepbf:bnode pbnode)
                    (type stream fs))
           (let* ((size (pb:octet-size pbnode))
                  (buf (make-array size :element-type '(unsigned-byte 8)))
                  (pos (file-position fs)))
             (pb:serialize pbnode buf 0 size)
             (write-sequence buf fs)
             (values pos size))))
    (let ((pbnode (make-instance 'btreepbf:bnode)))
      (loop for key across (bnode-keys node)
         for i from 0 below (bnode-size node)
         do (vector-push-extend key (btreepbf:keys pbnode)))
      (if (eq (bnode-kind node) :leaf)
          (progn
            (loop for i from 1 to (bnode-size node)
               do (let ((val (car (aref (bnode-pointers node) i))))
                    (vector-push-extend (funcall serialize-values-fn val) (btreepbf:values pbnode))))
            (save-node-to-stream fs pbnode))
          (when (eq (bnode-kind node) :node)
            (setf (btreepbf:kind pbnode) btreepbf:+bnode-kind-node+)
            (loop for pnt across (bnode-pointers node)
               for i from 0 to (bnode-size node)
               do (when pnt
                    (multiple-value-bind (child-pos child-size)
                        (walk-save-bnodes-tmp fs (car pnt) serialize-values-fn)
                      (vector-push-extend child-pos (btreepbf:pointers pbnode))
                      (vector-push-extend child-size (btreepbf:child-sizes pbnode)))))
            (save-node-to-stream fs pbnode))))))

(defun write-btree-tmp (fs tree serialize-values-fn)
  (walk-save-bnodes-tmp fs (car (btree-root tree)) serialize-values-fn))
