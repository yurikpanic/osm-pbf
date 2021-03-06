(defpackage :osm-index-search
  (:use :cl)
  (:export :init-osm-search))

(in-package :osm-index-search)

(defstruct search-data
  (file-name #p"/home/yuri/work/globus/osm/out.osm.pbf" :type pathname)
  (blob-offsets (make-array 0 :element-type '(unsigned-byte 64) :adjustable t :fill-pointer 0) :type vector)
  (btrees (make-hash-table :test 'eq) :type hash-table)
  (deserialize-array nil :type (or null vector))
  (btree-offsets (make-hash-table :test 'eq) :type hash-table))

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
                           (macrolet ((detect-index (type field)
                                        (let ((tf (intern (format nil "~A-~A" (string-upcase type) (string-upcase field)) :keyword)))
                                          `(when (and (string= (pb:string-value (btreepbf:type pbtree)) ,type)
                                                      (string= (pb:string-value (btreepbf:field pbtree)) ,field))
                                             (setf (gethash ,tf (search-data-btrees sd)) pbtree
                                                   (gethash ,tf (search-data-btree-offsets sd)) (file-position fs))))))
                             (detect-index "way" "id")
                             (detect-index "node" "id")
                             (detect-index "relation" "id")
                             (detect-index "way-bbox" "id")
                             (detect-index "relation-bbox" "id"))))))
                 fs))
    sd))

(defun get-btree-node (sd offs size btree-id)
  (with-open-file (fs (search-data-file-name sd) :direction :input :element-type '(unsigned-byte 8))
    (file-position fs (+ offs (gethash btree-id (search-data-btree-offsets sd))))
    (let ((arr (get-deserialize-array sd size)))
      (read-sequence arr fs :end size)
      (let ((bnode (make-instance 'btreepbf:bnode)))
        (pb:merge-from-array bnode arr 0 size)
        bnode))))

(defun search-btree (sd bnode id btree-id)
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
                           (aref (btreepbf:child-sizes bnode) descend-idx)
                           btree-id)
           id
           btree-id))
        (progn
          (loop for key across (btreepbf:keys bnode)
             for val across (btreepbf:values bnode)
               do (when (= key id)
                    (return-from search-btree val)))
          nil))))

(defun load-direct (sd blob-num offs size)
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
          (let ((node-buf (make-array size :element-type '(unsigned-byte 8))))
            (read-sequence node-buf fs)
            node-buf))))))

(defun load-stringtable-entries (sd blob-num entries)
  (when (typep entries '(unsigned-byte 64))
    (setf entries (list entries)))
  (let ((stringtable (load-stringtable sd blob-num)))
    (when stringtable
      (if (typep entries 'list)
          (loop for entry in entries
             collect (sb-ext:octets-to-string (aref stringtable entry)))
          (loop for entry across entries
             collect (sb-ext:octets-to-string (aref stringtable entry)))))))

(defun load-stringtable (sd blob-num)
  (with-open-file (fs (search-data-file-name sd) :element-type '(unsigned-byte 8))
    (file-position fs (aref (search-data-blob-offsets sd) blob-num))
    (let ((blob-header-len-buf (make-array 4 :element-type '(unsigned-byte 8))))
      (read-sequence blob-header-len-buf fs)
      (let ((blob-header-len 0))
        (loop for d across blob-header-len-buf
           do (setf blob-header-len (logior (ash blob-header-len 8) d)))
        (let ((blob-header-buf (make-array blob-header-len :element-type '(unsigned-byte 8))))
          (read-sequence blob-header-buf fs)
          (let ((blob-header (make-instance 'osmpbf:blob-header)))
            (pb:merge-from-array blob-header blob-header-buf 0 blob-header-len)
            (when (string= (pb:string-value (osmpbf:type blob-header)) "OSMData")
              (let ((blob-buf (make-array (osmpbf:datasize blob-header) :element-type '(unsigned-byte 8))))
                (read-sequence blob-buf fs)
                (let ((blob (make-instance 'osmpbf:blob)))
                  (pb:merge-from-array blob blob-buf 0 (length blob-buf))
                  (let ((data (if (osmpbf:has-raw blob)
                                  (osmpbf:raw blob)
                                  (coerce (zlib:uncompress (osmpbf:zlib-data blob) :uncompressed-size (osmpbf:raw-size blob)) '(simple-array (unsigned-byte 8) (*))))))
                    (let ((pblock (make-instance 'osmpbf:primitive-block)))
                      (pb:merge-from-array pblock data 0 (length data))
                      (osmpbf:s (osmpbf:stringtable pblock)))))))))))))

(defun find-by-id-raw (sd id btree-id)
  (let* ((btree (gethash btree-id (search-data-btrees sd)))
         (root (get-btree-node sd (btreepbf:root-offs btree) (btreepbf:root-size btree) btree-id)))
    (search-btree sd root id btree-id)))

(defmacro deserialize-value-to-type (type val)
  (let ((res (gensym)))
  `(when ,val
     (let ((,res (make-instance ,type)))
       (pb:merge-from-array ,res ,val 0 (length ,val))
       ,res))))

(defun find-by-id-blob-idx (sd id btree-id)
  (let ((val (find-by-id-raw sd id btree-id)))
    (deserialize-value-to-type 'btreepbf:blob-index val)))

(defun find-by-id-bbox (sd id btree-id)
  (let ((val (find-by-id-raw sd id btree-id)))
    (deserialize-value-to-type 'btreepbf:bbox val)))

(defmacro item-deserializer (sd blob-idx kind)
  (let ((kind (string-upcase (if (typep kind 'symbol) (symbol-name kind) kind)))
        (item (gensym)))
    `(let ((,item (make-instance ',(intern kind :osmpbf))))
       (pb:merge-from-array ,item (load-direct ,sd (btreepbf:blob-num ,blob-idx) (btreepbf:blob-offs ,blob-idx) (btreepbf:size ,blob-idx)) 0 (btreepbf:size ,blob-idx))
       ,item)))

(defun find-node-by-id (sd id)
  (let ((blob-idx (find-by-id-blob-idx sd id :node-id))
        (node (make-instance 'osmpbf:node)))
    (when blob-idx
      (pb:merge-from-array
       node
       (load-direct sd (btreepbf:blob-num blob-idx) (btreepbf:blob-offs blob-idx) (btreepbf:size blob-idx))
       0 (btreepbf:size blob-idx))
      node)))

(defun find-way-by-id (sd id)
  (let ((blob-idx (find-by-id-blob-idx sd id :way-id))
        (way (make-instance 'osmpbf:way)))
    (when blob-idx
      (pb:merge-from-array
       way
       (load-direct sd (btreepbf:blob-num blob-idx) (btreepbf:blob-offs blob-idx) (btreepbf:size blob-idx))
       0 (btreepbf:size blob-idx))
      way)))

(defun find-relation-by-id (sd id)
  (let ((blob-idx (find-by-id-blob-idx sd id :relation-id))
        (rel (make-instance 'osmpbf:relation)))
    (when blob-idx
      (pb:merge-from-array
       rel
       (load-direct sd (btreepbf:blob-num blob-idx) (btreepbf:blob-offs blob-idx) (btreepbf:size blob-idx))
       0 (btreepbf:size blob-idx))
      (values rel blob-idx))))

(defun traverse-btree (sd bnode btree-id fun)
  (if (= (btreepbf:kind bnode) btreepbf:+bnode-kind-node+)
      (loop for pnt across (btreepbf:pointers bnode)
           for cs across (btreepbf:child-sizes bnode)
           do (traverse-btree sd (get-btree-node sd pnt cs btree-id) btree-id fun))
      (loop for key across (btreepbf:keys bnode)
         for val across (btreepbf:values bnode)
         do (when val
              (let ((blob-idx (make-instance 'btreepbf:blob-index)))
                (pb:merge-from-array blob-idx val 0 (length val))
                (funcall fun blob-idx))))))

(defun for-every (sd btree-id fun)
  (let* ((btree (gethash btree-id (search-data-btrees sd)))
         (root (get-btree-node sd (btreepbf:root-offs btree) (btreepbf:root-size btree) btree-id)))
    (traverse-btree sd root btree-id fun)))

;; iterate over relations
;; (for-every *sd* :relation-id 
;;            #'(lambda (x)
;;                (format nil "~A~%" (item-deserializer *sd* x :relation))))

(defun load-nodes-for-way (sd way)
  (let ((prev-id 0))
    (loop for ref across (osmpbf:refs way)
         collect (find-node-by-id sd (setf prev-id (+ prev-id ref))))))

(defun load-ways-for-relation (sd relation)
  (let ((prev-id 0))
    (loop for mem across (osmpbf:memids relation)
       for type across (osmpbf:types relation)
       when (eq type osmpbf:+relation-member-type-way+)
       collect (find-way-by-id sd (+ mem prev-id))
       do (setf prev-id (+ mem prev-id)))))
  
(defun check-right-ray-cross (lon lat lon1 lat1 lon2 lat2)
  (let ((lat-sign1 (signum (- lat lat1)))
        (lat-sign2 (signum (- lat lat2))))
    (when (and (zerop lat-sign1) (zerop lat-sign2))
      (return-from check-right-ray-cross (values t t t)))
    (when (or (and (= lat-sign1 lat-sign2))
              (and (< lon1 lon) (< lon2 lon)))
      (return-from check-right-ray-cross nil))
    (let ((x (+ lon1 (/ (* (- lon2 lon1) (- lat lat1)) (- lat2 lat1)))))
      (if (or (< lon1 x lon2) (< lon2 x lon1)) t
          (cond
            ((= lon1 x) (values t t nil))
            ((= x lon2) (values t nil t))
            (t nil))))))

(defun bbox-right-of-coord (bbox lon lat)
  (check-right-ray-cross lon lat
                         (+ (btreepbf:min-lon bbox) (btreepbf:max-lon bbox))
                         (btreepbf:min-lat bbox)
                         (+ (btreepbf:min-lon bbox) (btreepbf:max-lon bbox))
                         (+ (btreepbf:min-lat bbox) (btreepbf:max-lat bbox))))

(defun count-right-ray-cross (sd relation lon lat)
  (let ((cnt 0)
        (point-crossed (make-hash-table)))
    (loop for way in (load-ways-for-relation sd relation)
       do (if way
              (let ((waybb (find-by-id-bbox sd (osmpbf:id way) :way-bbox-id)))
                (unless (and waybb (not (bbox-right-of-coord waybb lon lat)))
                  (let ((prev-node nil))
                    (dolist (node (load-nodes-for-way sd way))
                      (when prev-node
                        (multiple-value-bind
                              (cross in-p1 in-p2)
                            (check-right-ray-cross lon lat
                                                   (osmpbf:lon prev-node) (osmpbf:lat prev-node)
                                                   (osmpbf:lon node) (osmpbf:lat node))
                          (when cross
                            (when (or
                                   (and (not in-p1) (not in-p2))
                                   (and in-p1 (not (gethash (osmpbf:id prev-node) point-crossed)))
                                   (and in-p2 (not (gethash (osmpbf:id node) point-crossed))))
                              (incf cnt)
                              (when in-p1 (setf (gethash (osmpbf:id prev-node) point-crossed) t))
                              (when in-p2 (setf (gethash (osmpbf:id node) point-crossed) t))))))
                      (setf prev-node node)))))
              ;; some ways are missing for this relation - dont check it
              (return-from count-right-ray-cross 0)))
    cnt))

(defparameter *lat* 478426580)
(defparameter *lon* 350764960)

(defun coord-in-bbox-p (lon lat bbox)
  (and (>= lon (btreepbf:min-lon bbox)) (>= lat (btreepbf:min-lon bbox))
       (<= lon (+ (btreepbf:min-lon bbox) (btreepbf:max-lon bbox)))
       (<= lat (+ (btreepbf:min-lat bbox) (btreepbf:max-lat bbox)))))

(defun count-test ()
  (for-every *sd* :relation-id
             #'(lambda (x)
                 (let* ((rel (item-deserializer *sd* x :relation))
                        (bbox (find-by-id-bbox *sd* (osmpbf:id rel) :relation-bbox-id)))
                   (when (and bbox (coord-in-bbox-p *lon* *lat* bbox))
                     (let ((cnt (count-right-ray-cross *sd* rel *lon* *lat*)))
                       (unless (zerop cnt)
                         (format t "~A ~A~%" (osmpbf:id rel) cnt))))))))


