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
  (btree-file #p"/home/yuri/work/globus/osm/out.btree" :type pathname)
  (temp-file #p"/home/yuri/work/globus/osm/out.tmp" :type pathname)
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

(defun begin-write (&key file-name bbox btree-file temp-file)
  (let ((wd (make-write-descr)))
    (when file-name (setf (write-descr-file-name wd) file-name))
    (when btree-file (setf (write-descr-temp-file wd) btree-file))
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
  (when (or (> (write-descr-st-count wd) 0) (> (write-descr-node-idx wd) 0))
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
    (setf (node-blob-num node) (write-descr-blob-num wd))
    (incf (write-descr-node-idx wd))
    (vector-push-extend pbnode
                        (osmpbf:nodes (write-descr-pgroup wd))))
  (when (>= (blob-elts-count wd) +def-items-per-page+)
    (flush-write wd))
  wd)

(defun write-btree (wd tree serialize-values-fn &optional type-str field-str)
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
               (buf (make-array size :element-type '(unsigned-byte 8))))
          (pb:serialize pbtree buf 0 size)
          (with-open-file (ofs (write-descr-btree-file wd) :direction :output :element-type '(unsigned-byte 8 :if-exists :append))
            (loop for i from 3 downto 0
               do (write-byte (logand #xFF (ash size (* -8 i))) ofs))
            (write-sequence buf ofs)
            (with-open-file (fs (write-descr-temp-file wd) :direction :input :element-type '(unsigned-byte 8))
              (let ((btree-size (file-length fs))
                    (copy-buf (make-array (* 1024 1024) :element-type '(unsigned-byte 8))))
                (loop for i from 3 downto 0
                   do (write-byte (logand #xFF (ash btree-size (* -8 i))) ofs))
                (do ((bytes-read #1=(read-sequence copy-buf fs) #1#))
                    ((zerop bytes-read) (values))
                  (write-sequence copy-buf ofs :end bytes-read))))))))))

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
